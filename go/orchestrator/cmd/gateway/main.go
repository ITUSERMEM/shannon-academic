package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/cmd/gateway/internal/handlers"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/cmd/gateway/internal/middleware"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/cmd/gateway/internal/openai"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/cmd/gateway/internal/proxy"
	authpkg 	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/budget"
	cfg "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/config"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/daemon"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/db"
	agentpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/agent"
	orchpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/skills"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	circuitbreaker "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/circuitbreaker"
	degradationpkg "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/degradation"
	redisv8 "github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	academicworkflow "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/academic"
	academicactivities "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities/academic"
)

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	// Discover configuration defaults from features.yaml (best effort)
	var gatewaySkipAuthDefault *bool
	if featuresCfg, err := cfg.Load(); err != nil {
		logger.Warn("Failed to load feature configuration", zap.Error(err))
	} else if featuresCfg != nil && featuresCfg.Gateway.SkipAuth != nil {
		gatewaySkipAuthDefault = featuresCfg.Gateway.SkipAuth
	}
	if envVal := os.Getenv("GATEWAY_SKIP_AUTH"); envVal != "" {
		logger.Warn("Environment variable overrides gateway authentication setting",
			zap.String("env", "GATEWAY_SKIP_AUTH"),
			zap.String("config_key", "gateway.skip_auth"),
			zap.String("value", envVal))
	} else if gatewaySkipAuthDefault != nil {
		if *gatewaySkipAuthDefault {
			_ = os.Setenv("GATEWAY_SKIP_AUTH", "1")
		} else {
			_ = os.Setenv("GATEWAY_SKIP_AUTH", "0")
		}
	}

	// Initialize database
	dbConfig := &db.Config{
		Host:     getEnvOrDefault("POSTGRES_HOST", "postgres"),
		Port:     getEnvOrDefaultInt("POSTGRES_PORT", 5432),
		User:     getEnvOrDefault("POSTGRES_USER", "shannon"),
		Password: getEnvOrDefault("POSTGRES_PASSWORD", "shannon"),
		Database: getEnvOrDefault("POSTGRES_DB", "shannon"),
		SSLMode:  getEnvOrDefault("POSTGRES_SSLMODE", "disable"),
	}

	dbClient, err := db.NewClient(dbConfig, logger)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer dbClient.Close()

	// Create sqlx.DB wrapper for auth service
	pgDB := sqlx.NewDb(dbClient.GetDB(), "postgres")

	// Initialize budget manager for token/cost budgeting
	budgetManager := budget.NewBudgetManager(dbClient.GetDB(), logger)
	_ = budgetManager // used by /api/v1/budget/check

	// Initialize Redis client for rate limiting and idempotency
	redisURL := getEnvOrDefault("REDIS_URL", "redis://redis:6379")
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		logger.Fatal("Failed to parse Redis URL", zap.Error(err))
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	// Test Redis connection
	ctx := context.Background()
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
	}

	// Initialize circuit breakers for Redis and database
	redisCBCfg := circuitbreaker.GetRedisConfig().ToConfig()
	redisCBCfg.OnStateChange = func(name string, from, to circuitbreaker.State) {
		logger.Info("Redis circuit breaker state changed",
			zap.String("from", from.String()),
			zap.String("to", to.String()),
		)
	}
	redisCB := circuitbreaker.NewCircuitBreaker("redis", redisCBCfg, logger)
	circuitbreaker.GlobalMetricsCollector.RegisterCircuitBreaker("redis", "gateway", redisCB)

	dbCBCfg := circuitbreaker.GetDatabaseConfig().ToConfig()
	dbCBCfg.OnStateChange = func(name string, from, to circuitbreaker.State) {
		logger.Info("Database circuit breaker state changed",
			zap.String("from", from.String()),
			zap.String("to", to.String()),
		)
	}
	dbCB := circuitbreaker.NewCircuitBreaker("postgresql", dbCBCfg, logger)
	circuitbreaker.GlobalMetricsCollector.RegisterCircuitBreaker("postgresql", "gateway", dbCB)

	// Create circuit breaker status handler for Python HTTP bridge
	cbStatusHandler := handlers.NewCircuitBreakerStatusHandler(logger)
	cbStatusHandler.Register("redis", redisCB)
	cbStatusHandler.Register("postgresql", dbCB)

	// Initialize degradation manager
	degradationMgr := degradationpkg.NewManager(
		circuitBreakerOpenChecker(func() bool { return redisCB.State() == circuitbreaker.StateOpen }),
		circuitBreakerOpenChecker(func() bool { return dbCB.State() == circuitbreaker.StateOpen }),
		logger,
	)
	if err := degradationMgr.Start(ctx); err != nil {
		logger.Warn("Failed to start degradation manager", zap.Error(err))
	}
	degradationLevelHandler := handlers.NewDegradationLevelHandler(degradationMgr, logger)

	// Initialize streaming manager with Redis so gateway can subscribe to
	// workflow events for streaming card replies.
	// Streaming manager uses go-redis/v8, gateway uses redis/v9 — create a v8 client.
	streamingRedis := redisv8.NewClient(&redisv8.Options{
		Addr:     redisOpts.Addr,
		Password: redisOpts.Password,
		DB:       redisOpts.DB,
	})
	defer streamingRedis.Close()
	streaming.InitializeRedis(streamingRedis, logger)

	// Initialize auth service (direct access to internal package)
	jwtSecret := getEnvOrDefault("JWT_SECRET", "your-secret-key")
	authService := authpkg.NewService(pgDB, logger, jwtSecret)

	// Connect to orchestrator gRPC
	orchAddr := getEnvOrDefault("ORCHESTRATOR_GRPC", "orchestrator:50052")
	conn, err := grpc.Dial(orchAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(50*1024*1024)), // 50MB
	)
	if err != nil {
		logger.Fatal("Failed to connect to orchestrator", zap.Error(err))
	}
	defer conn.Close()

	orchClient := orchpb.NewOrchestratorServiceClient(conn)

	// Connect to Rust agent-core gRPC
	rustAgentAddr := getEnvOrDefault("RUST_AGENT_GRPC", "localhost:50051")
	rustAgentConn, err := grpc.Dial(rustAgentAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(50*1024*1024)),
	)
	if err != nil {
		logger.Fatal("Failed to connect to Rust agent-core", zap.Error(err))
	}
	defer rustAgentConn.Close()
	rustAgentClient := agentpb.NewAgentServiceClient(rustAgentConn)

	// Verify Rust agent connection with health check
	if healthResp, err := rustAgentClient.HealthCheck(ctx, &agentpb.HealthCheckRequest{}); err != nil {
		logger.Warn("Rust agent-core health check failed (service may be starting)", zap.Error(err))
	} else {
		logger.Info("Rust agent-core connected",
			zap.Bool("healthy", healthResp.GetHealthy()),
			zap.String("addr", rustAgentAddr),
		)
	}

	// Initialize skill registry
	skillRegistry := skills.NewRegistry()
	for _, dir := range skills.ResolveSkillDirs() {
		if err := skillRegistry.LoadDirectory(dir); err != nil {
			logger.Warn("Failed to load skills from directory", zap.String("path", dir), zap.Error(err))
		}
	}
	if err := skillRegistry.Finalize(); err != nil {
		logger.Fatal("Failed to finalize skill registry", zap.Error(err))
	}
	logger.Info("Skill registry initialized",
		zap.Int("total_skills", skillRegistry.Count()),
		zap.Strings("categories", skillRegistry.Categories()),
	)

	// Initialize academic gRPC client for Temporal activities
	academicAddr := getEnvOrDefault("ACADEMIC_GRPC", "localhost:50053")
	if err := academicactivities.InitAcademicClient(academicAddr); err != nil {
		logger.Fatal("Failed to connect to academic service", zap.Error(err))
	}
	logger.Info("Academic gRPC client initialized", zap.String("addr", academicAddr))

	// Start Temporal worker for academic pipeline workflows
	startTemporalWorker(logger)

	// Create session manager for persisting HITL messages to session history
	sessionMgr, err := session.NewManager(redisOpts.Addr, logger)
	if err != nil {
		logger.Warn("Failed to create session manager for HITL persistence", zap.Error(err))
	}

	// Create handlers
	taskHandler := handlers.NewTaskHandler(orchClient, pgDB, redisClient, skillRegistry, logger, sessionMgr)
	sessionHandler := handlers.NewSessionHandler(pgDB, redisClient, logger)
	approvalHandler := handlers.NewApprovalHandler(orchClient, logger)
	reviewHandler := handlers.NewReviewHandler(orchClient, redisClient, logger)
	scheduleHandler := handlers.NewScheduleHandler(orchClient, pgDB, logger)
	healthHandler := handlers.NewHealthHandler(orchClient, logger)
	openapiHandler := handlers.NewOpenAPIHandler()
	agentsHandler := handlers.NewAgentsHandler(orchClient, logger)
	skillHandler := handlers.NewSkillHandler(skillRegistry, logger)
	workspaceHandler := handlers.NewWorkspaceHandler(pgDB, logger)
	memoryHandler := handlers.NewMemoryHandler(logger)
	llmModelsPath := cfg.ResolveConfigFile("MODELS_CONFIG_PATH", []string{
		"/app/config/models.yaml",
		"config/models.yaml",
		"../../config/models.yaml",
	}, "config/models.yaml")
	llmModelsHandler := handlers.NewLLMModelsHandler(llmModelsPath, logger)

	adminURL := getEnvOrDefault("ADMIN_SERVER", "http://orchestrator:8081")

	// OAuth verifier (enterprise-only)
	// Web OAuth credentials
	googleClientID := getEnvOrDefault("GOOGLE_OAUTH_CLIENT_ID", "")
	googleClientSecret := getEnvOrDefault("GOOGLE_OAUTH_CLIENT_SECRET", "")
	// Desktop OAuth credentials (for Tauri/native apps)
	googleDesktopClientID := getEnvOrDefault("GOOGLE_DESKTOP_CLIENT_ID", "")
	googleDesktopClientSecret := getEnvOrDefault("GOOGLE_DESKTOP_CLIENT_SECRET", "")
	oauthVerifier := authpkg.NewGoogleOAuthVerifier(googleClientID, googleClientSecret, googleDesktopClientID, googleDesktopClientSecret)

	// Enterprise: Load rate limit config from YAML (or use defaults)
	rateLimitConfigPath := cfg.ResolveConfigFile("RATE_LIMIT_CONFIG_PATH", cfg.RateLimitConfigPaths, "config/rate_limits.yaml")
	rateLimitConfig, err := middleware.LoadRateLimitConfig(rateLimitConfigPath)
	if err != nil {
		logger.Warn("Failed to load rate limit config, using defaults", zap.String("path", rateLimitConfigPath), zap.Error(err))
		rateLimitConfig = middleware.DefaultRateLimitConfig()
	} else {
		logger.Info("Loaded rate limit config", zap.String("path", rateLimitConfigPath))
	}

	// Create auth handler (needs rateLimitConfig for config-based RPM/RPH in /auth/me)
	authHandler := handlers.NewAuthHandler(authService, oauthVerifier, pgDB, logger, rateLimitConfig)

	// Create middlewares
	authMiddleware := middleware.NewAuthMiddleware(authService, logger).Middleware
	rateLimiter := middleware.NewRateLimiterWithConfig(redisClient, logger, rateLimitConfig).Middleware
	idempotencyMiddleware := middleware.NewIdempotencyMiddleware(redisClient, logger).Middleware
	tracingMiddleware := middleware.NewTracingMiddleware(logger).Middleware
	validationMiddleware := middleware.NewValidationMiddleware(logger).Middleware

	// OpenAI-compatible API handler
	openaiHandler, err := openai.NewHandler(orchClient, pgDB, redisClient, logger, adminURL)
	if err != nil {
		logger.Warn("Failed to create OpenAI handler, endpoint disabled", zap.Error(err))
	}

	// Tool execution handler
	toolsHandler := handlers.NewToolsHandler(pgDB, logger)

	// Setup HTTP mux
	mux := http.NewServeMux()

	// Health check (no auth required)
	mux.HandleFunc("GET /health", healthHandler.Health)
	mux.HandleFunc("GET /readiness", healthHandler.Readiness)

	// Prometheus metrics endpoint (no auth required)
	mux.Handle("GET /metrics", promhttp.Handler())

	// OpenAPI spec (no auth required)
	mux.HandleFunc("GET /openapi.json", openapiHandler.ServeSpec)

	// Auth endpoints (no auth required for registration, auth required for me/refresh)
	// Auth endpoints
	mux.HandleFunc("POST /api/v1/auth/register", authHandler.Register)
	mux.HandleFunc("POST /api/v1/auth/refresh", authHandler.Refresh)
	mux.Handle("GET /api/v1/auth/me",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(authHandler.Me),
			),
		),
	)
	mux.Handle("POST /api/v1/auth/refresh-key",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(authHandler.RefreshKey),
			),
		),
	)

	// API Key management endpoints (require auth)
	mux.Handle("GET /api/v1/auth/api-keys",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(authHandler.ListAPIKeys),
			),
		),
	)
	mux.Handle("POST /api/v1/auth/api-keys",
		tracingMiddleware(
			authMiddleware(
				rateLimiter(
					http.HandlerFunc(authHandler.CreateKey),
				),
			),
		),
	)
	mux.Handle("DELETE /api/v1/auth/api-keys/{id}",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(authHandler.RevokeKey),
			),
		),
	)

	// Task endpoints (require auth)
	mux.Handle("POST /api/v1/tasks",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.SubmitTask),
						),
					),
				),
			),
		),
	)

	// Unified submit that returns the SSE stream URL (no long-lived stream here)
	mux.Handle("POST /api/v1/tasks/stream",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.SubmitTaskAndGetStreamURL),
						),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tasks",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(taskHandler.ListTasks),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tasks/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(taskHandler.GetTaskStatus),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tasks/{id}/stream",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(taskHandler.StreamTask),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tasks/{id}/events",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(taskHandler.GetTaskEvents),
					),
				),
			),
		),
	)

	mux.Handle("POST /api/v1/tasks/{id}/cancel",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.CancelTask),
						),
					),
				),
			),
		),
	)

	// Pause workflow
	mux.Handle("POST /api/v1/tasks/{id}/pause",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.PauseTask),
						),
					),
				),
			),
		),
	)

	// Resume workflow
	mux.Handle("POST /api/v1/tasks/{id}/resume",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.ResumeTask),
						),
					),
				),
			),
		),
	)

	// Swarm HITL: send human message to Lead
	mux.Handle("POST /api/v1/swarm/{workflowID}/message",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(taskHandler.SendSwarmMessage),
					),
				),
			),
		),
	)

	// Get control state
	mux.Handle("GET /api/v1/tasks/{id}/control-state",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(taskHandler.GetControlState),
					),
				),
			),
		),
	)

	// Approval endpoints (require auth)
	mux.Handle("POST /api/v1/approvals/decision",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(approvalHandler.SubmitDecision),
						),
					),
				),
			),
		),
	)

	// HITL Research Review
	mux.Handle("GET /api/v1/tasks/{workflowID}/review",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(reviewHandler.HandleGetReview),
				),
			),
		),
	)
	mux.Handle("POST /api/v1/tasks/{workflowID}/review",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(reviewHandler.HandleReview),
					),
				),
			),
		),
	)

	// Session endpoints (require auth)
	mux.Handle("GET /api/v1/sessions",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(sessionHandler.ListSessions),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/sessions/{sessionId}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(sessionHandler.GetSession),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/sessions/{sessionId}/history",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(sessionHandler.GetSessionHistory),
				),
			),
		),
	)

	// Session events (chat history-like, excludes LLM_PARTIAL)
	mux.Handle("GET /api/v1/sessions/{sessionId}/events",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(sessionHandler.GetSessionEvents),
				),
			),
		),
	)

	// Update session title
	mux.Handle("PATCH /api/v1/sessions/{sessionId}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(sessionHandler.UpdateSessionTitle),
					),
				),
			),
		),
	)

	// Soft delete session (idempotent)
	mux.Handle("DELETE /api/v1/sessions/{sessionId}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(sessionHandler.DeleteSession),
					),
				),
			),
		),
	)

	// Session workspace file operations (require auth)
	// List files in session workspace
	mux.Handle("GET /api/v1/sessions/{sessionId}/files",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(workspaceHandler.ListFiles),
					),
				),
			),
		),
	)

	// Download file from session workspace (path is the file path within workspace)
	mux.Handle("GET /api/v1/sessions/{sessionId}/files/{path...}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(workspaceHandler.DownloadFile),
					),
				),
			),
		),
	)

	// User memory file operations (require auth, user_id from token)
	// List files in authenticated user's memory directory
	mux.Handle("GET /api/v1/memory/files",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(memoryHandler.ListMemoryFiles),
					),
				),
			),
		),
	)

	// Download file from authenticated user's memory directory
	mux.Handle("GET /api/v1/memory/files/{path...}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(memoryHandler.DownloadMemoryFile),
					),
				),
			),
		),
	)

	// Schedule endpoints (require auth)
	mux.Handle("POST /api/v1/schedules",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(scheduleHandler.CreateSchedule),
						),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/schedules",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(scheduleHandler.ListSchedules),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/schedules/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(scheduleHandler.GetSchedule),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/schedules/{id}/runs",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(scheduleHandler.GetScheduleRuns),
				),
			),
		),
	)

	mux.Handle("PUT /api/v1/schedules/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(scheduleHandler.UpdateSchedule),
						),
					),
				),
			),
		),
	)

	mux.Handle("POST /api/v1/schedules/{id}/pause",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(scheduleHandler.PauseSchedule),
						),
					),
				),
			),
		),
	)

	mux.Handle("POST /api/v1/schedules/{id}/resume",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(scheduleHandler.ResumeSchedule),
						),
					),
				),
			),
		),
	)

	mux.Handle("DELETE /api/v1/schedules/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(scheduleHandler.DeleteSchedule),
					),
				),
			),
		),
	)

	// Agents endpoints (single-purpose deterministic agents)
	mux.Handle("GET /api/v1/agents",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(agentsHandler.ListAgents),
			),
		),
	)

	mux.Handle("GET /api/v1/agents/{id}",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(agentsHandler.GetAgent),
			),
		),
	)

	mux.Handle("POST /api/v1/agents/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(agentsHandler.ExecuteAgent),
						),
					),
				),
			),
		),
	)

	logger.Info("Registered agents API endpoints",
		zap.String("endpoints", "/api/v1/agents, /api/v1/agents/{id}"),
	)

	// Skills endpoints (require auth)
	mux.Handle("GET /api/v1/skills",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(skillHandler.ListSkills),
			),
		),
	)

	mux.Handle("GET /api/v1/skills/{name}",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(skillHandler.GetSkill),
			),
		),
	)

	mux.Handle("GET /api/v1/skills/{name}/versions",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(skillHandler.GetSkillVersions),
			),
		),
	)

	logger.Info("Registered skills API endpoints",
		zap.String("endpoints", "/api/v1/skills, /api/v1/skills/{name}, /api/v1/skills/{name}/versions"),
	)

	// Tool execution endpoints (require auth)
	// NOTE: GET /api/v1/tools must be registered BEFORE GET /api/v1/tools/{name}
	// to avoid routing conflicts with Go 1.22 ServeMux.
	mux.Handle("GET /api/v1/tools",
		tracingMiddleware(
			authMiddleware(
				rateLimiter(
					http.HandlerFunc(toolsHandler.ListTools),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tools/{name}",
		tracingMiddleware(
			authMiddleware(
				rateLimiter(
					http.HandlerFunc(toolsHandler.GetTool),
				),
			),
		),
	)

	mux.Handle("POST /api/v1/tools/{name}/execute",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(toolsHandler.ExecuteTool),
					),
				),
			),
		),
	)

	// Rust agent ExecuteTask endpoint: POST /api/v1/tools/execute
	mux.Handle("POST /api/v1/tools/execute",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							var req struct {
								ToolName string                 `json:"tool_name"`
								Input    map[string]interface{} `json:"input"`
							}
							if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
								http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
								return
							}
							if req.ToolName == "" {
								http.Error(w, `{"error":"tool_name is required"}`, http.StatusBadRequest)
								return
							}

							inputStruct, err := structpb.NewStruct(req.Input)
							if err != nil {
								http.Error(w, `{"error":"invalid input"}`, http.StatusBadRequest)
								return
							}

							grpcResp, err := rustAgentClient.ExecuteTask(r.Context(), &agentpb.ExecuteTaskRequest{
								Query:   req.ToolName,
								Context: inputStruct,
							})
							if err != nil {
								logger.Error("Rust agent ExecuteTask failed", zap.Error(err))
								http.Error(w, `{"error":"task execution failed"}`, http.StatusInternalServerError)
								return
							}

							resp := map[string]interface{}{
								"task_id": grpcResp.GetTaskId(),
								"result":  grpcResp.GetResult(),
								"status":  grpcResp.GetStatus().String(),
							}
							w.Header().Set("Content-Type", "application/json")
							json.NewEncoder(w).Encode(resp)
						}),
					),
				),
			),
		),
	)

	logger.Info("Registered tool execution API endpoints",
		zap.String("endpoints", "/api/v1/tools, /api/v1/tools/{name}, /api/v1/tools/{name}/execute, /api/v1/tools/execute"),
	)

	// OpenAI-compatible API endpoints (/v1/*)
	if openaiHandler != nil {
		// POST /v1/chat/completions - main chat completion endpoint
		mux.Handle("POST /v1/chat/completions",
			tracingMiddleware(
				authMiddleware(
					validationMiddleware(
						rateLimiter(
							http.HandlerFunc(openaiHandler.ChatCompletions),
						),
					),
				),
			),
		)

		// POST /v1/completions - passthrough proxy to LLM service (for CLI tool execution)
		mux.Handle("POST /v1/completions",
			tracingMiddleware(
				authMiddleware(
					validationMiddleware(
						rateLimiter(
							idempotencyMiddleware(
								http.HandlerFunc(openaiHandler.Completions),
							),
						),
					),
				),
			),
		)

		// GET /v1/models - list available models
		mux.Handle("GET /v1/models",
			tracingMiddleware(
				authMiddleware(
					http.HandlerFunc(openaiHandler.ListModels),
				),
			),
		)

		// GET /v1/models/{model} - get specific model info
		mux.Handle("GET /v1/models/{model}",
			tracingMiddleware(
				authMiddleware(
					http.HandlerFunc(openaiHandler.GetModel),
				),
			),
		)

		// GET /v1/llm-models - list available LLM provider models (from models.yaml)
		mux.Handle("GET /v1/llm-models",
			tracingMiddleware(
				authMiddleware(
					http.HandlerFunc(llmModelsHandler.ListLLMModels),
				),
			),
		)

		logger.Info("OpenAI-compatible API enabled",
			zap.String("endpoints", "/v1/chat/completions, /v1/completions, /v1/models, /v1/llm-models"),
		)
	}

	// SSE/WebSocket reverse proxy to admin server
	streamProxy, err := proxy.NewStreamingProxy(adminURL, logger)
	if err != nil {
		logger.Fatal("Failed to create streaming proxy", zap.Error(err))
	}

	// Proxy SSE and WebSocket endpoints
	mux.Handle("/api/v1/stream/sse",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					streamProxy,
				),
			),
		),
	)

	mux.Handle("/api/v1/stream/ws",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					streamProxy,
				),
			),
		),
	)

	// Daemon command channel
	daemonHub := daemon.NewHub(redisClient, logger)
	// Apply claim TTL from features config if loaded
	if featuresCfg, err := cfg.Load(); err == nil && featuresCfg.Daemon.HeartbeatTimeoutSeconds > 0 {
		daemonHub.Claims().SetClaimTTL(time.Duration(featuresCfg.Daemon.HeartbeatTimeoutSeconds) * time.Second)
	}
	// Subscribe to dispatch requests from orchestrator (Redis stream bridge)
	go daemonHub.SubscribeDispatches(ctx)
	approvalManager := daemon.NewApprovalManager(redisClient)
	// Token for internal admin server calls (/daemon/signal, /events).
	// Falls back to APPROVALS_AUTH_TOKEN to match orchestrator's daemon signal endpoint.
	eventsAuthToken := getEnvOrDefault("EVENTS_AUTH_TOKEN", getEnvOrDefault("APPROVALS_AUTH_TOKEN", ""))
	daemonHandler := handlers.NewDaemonHandler(daemonHub, approvalManager, adminURL, eventsAuthToken, logger)

	// WebSocket endpoint for shan CLI daemons (no method prefix for WS upgrade)
	mux.Handle("/v1/ws/messages",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(daemonHandler.HandleWebSocket),
			),
		),
	)

	// Daemon status API
	mux.Handle("GET /api/v1/daemon/status",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(daemonHandler.HandleStatus),
			),
		),
	)

	// Blob proxy: GET /api/v1/blob/{key} -> admin /blob/{key}
	// Used to fetch large base64 screenshots stored in Redis
	mux.Handle("GET /api/v1/blob/",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					streamProxy,
				),
			),
		),
	)

	// Timeline proxy: GET /api/v1/tasks/{id}/timeline -> admin /timeline
	mux.Handle("GET /api/v1/tasks/{id}/timeline",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					id := r.PathValue("id")
					if id == "" {
						http.Error(w, "{\"error\":\"Task ID required\"}", http.StatusBadRequest)
						return
					}
					// Rebuild target URL
					target := strings.TrimRight(adminURL, "/") + "/timeline?workflow_id=" + id
					if raw := r.URL.RawQuery; raw != "" {
						target += "&" + raw
					}
					req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, target, nil)
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						logger.Error("timeline proxy error", zap.Error(err))
						http.Error(w, "{\"error\":\"upstream unavailable\"}", http.StatusBadGateway)
						return
					}
					defer resp.Body.Close()
					for k, v := range resp.Header {
						for _, vv := range v {
							w.Header().Add(k, vv)
						}
					}
					w.WriteHeader(resp.StatusCode)
					_, _ = io.Copy(w, resp.Body)
				}),
			),
		),
	)

	// Budget check endpoint (requires auth)
	mux.Handle("POST /api/v1/budget/check",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var req struct {
						UserID        string  `json:"user_id"`
						Model         string  `json:"model"`
						EstimatedCost float64 `json:"estimated_cost"`
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
						return
					}
					if req.UserID == "" {
						http.Error(w, `{"error":"user_id is required"}`, http.StatusBadRequest)
						return
					}
					perToken := pricing.DefaultPerToken()
					if p, ok := pricing.PricePerTokenForModel(req.Model); ok {
						perToken = p
					}
					estimatedTokens := 0
					if perToken > 0 {
						estimatedTokens = int(req.EstimatedCost / perToken)
					}
					if estimatedTokens < 1 && req.EstimatedCost > 0 {
						estimatedTokens = 1
					}

					result, err := budgetManager.CheckBudget(r.Context(), req.UserID, "", "", estimatedTokens)
					if err != nil {
						logger.Error("budget check failed", zap.Error(err))
						http.Error(w, `{"error":"budget check failed"}`, http.StatusInternalServerError)
						return
					}

					remainingBudget := float64(result.RemainingSessionBudget) * perToken

					resp := map[string]interface{}{
						"allowed":          result.CanProceed,
						"remaining_budget": remainingBudget,
					}
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(resp)
				}),
			),
		),
	)

	logger.Info("Registered budget check endpoint",
		zap.String("endpoint", "POST /api/v1/budget/check"),
	)

	// Circuit breaker status endpoint (Python bridge, no auth)
	mux.Handle("GET /api/v1/circuitbreaker/status",
		tracingMiddleware(
			http.HandlerFunc(cbStatusHandler.ServeHTTP),
		),
	)

	// Degradation level endpoint (Python bridge, no auth)
	mux.Handle("GET /api/v1/degradation/level",
		tracingMiddleware(
			http.HandlerFunc(degradationLevelHandler.ServeHTTP),
		),
	)

	// Schedules list endpoint (already exists via scheduleHandler.ListSchedules at line ~590)
	// Python bridge: standalone /api/v1/shannon/schedules returns schedule info
	mux.Handle("GET /api/v1/shannon/schedules",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(scheduleHandler.ListSchedules),
				),
			),
		),
	)

	// Academic workflow endpoints
	mux.Handle("POST /api/v1/workflow/start",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(handleStartAcademicWorkflow),
				),
			),
		),
	)

	logger.Info("Registered academic workflow endpoint",
		zap.String("endpoint", "POST /api/v1/workflow/start"),
	)

	logger.Info("Registered Shannon system endpoints",
		zap.String("endpoints", "GET /api/v1/circuitbreaker/status, GET /api/v1/degradation/level, GET /api/v1/shannon/schedules"),
	)

	// Swagger UI (no auth required)
	mux.HandleFunc("GET /docs", handlers.DocsHandler)
	mux.HandleFunc("GET /api/openapi.yaml", handlers.SpecHandler)

	logger.Info("Registered docs endpoints",
		zap.String("endpoints", "GET /docs, GET /api/openapi.yaml"),
	)

	// CORS middleware for all routes (development friendly)
	corsHandler := corsMiddleware(mux)

	// Create HTTP server
	port := getEnvOrDefaultInt("PORT", 8080)
	server := &http.Server{
		Addr:         ":" + strconv.Itoa(port),
		Handler:      corsHandler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,                 // No write timeout for SSE/streaming support
		IdleTimeout:  300 * time.Second, // 5 minutes idle for long-lived SSE connections
	}

	// Start server in goroutine
	go func() {
		logger.Info("Gateway starting", zap.Int("port", port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Failed to start gateway", zap.Error(err))
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Gateway shutting down...")

	// Stop background workers (prevents goroutine leaks)
	authService.Close()
	if openaiHandler != nil {
		openaiHandler.Close()
	}

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("Gateway forced to shutdown", zap.Error(err))
	}

	logger.Info("Gateway stopped")
}

// startTemporalWorker creates and starts a Temporal worker for academic workflows.
func startTemporalWorker(logger *zap.Logger) {
	c, err := client.Dial(client.Options{})
	if err != nil {
		logger.Fatal("Unable to create Temporal client", zap.Error(err))
	}

	w := worker.New(c, "academic-task-queue", worker.Options{})
	w.RegisterWorkflow(academicworkflow.AcademicWorkflow)
	w.RegisterActivity(academicactivities.ExecutePhaseActivity)
	w.RegisterActivity(academicactivities.EvaluateGateActivity)
	w.RegisterActivity(academicactivities.RecordEventActivity)
	w.RegisterActivity(academicactivities.GetBudgetActivity)

	go func() {
		if err := w.Run(worker.InterruptCh()); err != nil {
			logger.Fatal("Unable to start Temporal worker", zap.Error(err))
		}
	}()
	logger.Info("Temporal worker started", zap.String("task_queue", "academic-task-queue"))
}

// handleStartAcademicWorkflow starts an AcademicWorkflow via Temporal client.
func handleStartAcademicWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProjectID  string `json:"project_id"`
		Title      string `json:"title"`
		StartPhase int32  `json:"start_phase"`
		EndPhase   int32  `json:"end_phase"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.ProjectID == "" {
		http.Error(w, `{"error":"project_id is required"}`, http.StatusBadRequest)
		return
	}

	c, err := client.Dial(client.Options{})
	if err != nil {
		http.Error(w, `{"error":"failed to connect to Temporal"}`, http.StatusInternalServerError)
		return
	}
	defer c.Close()

	opts := client.StartWorkflowOptions{
		ID:        "academic-" + req.ProjectID,
		TaskQueue: "academic-task-queue",
	}
	we, err := c.ExecuteWorkflow(context.Background(), opts, academicworkflow.AcademicWorkflow, academicworkflow.AcademicWorkflowParams{
		ProjectID:  req.ProjectID,
		Title:      req.Title,
		StartPhase: req.StartPhase,
		EndPhase:   req.EndPhase,
	})
	if err != nil {
		log.Printf("error: failed to start academic workflow: %v", err)
		http.Error(w, `{"error":"failed to start workflow"}`, http.StatusInternalServerError)
		return
	}

	resp := map[string]string{
		"workflow_id": we.GetID(),
		"run_id":      we.GetRunID(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// circuitBreakerOpenChecker adapts a bool func to the interface expected by degradation manager
type circuitBreakerOpenChecker func() bool

func (f circuitBreakerOpenChecker) IsCircuitBreakerOpen() bool { return f() }

// corsMiddleware adds CORS headers for development
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isStreaming := strings.HasPrefix(r.URL.Path, "/api/v1/stream/")

		allowedHeaders := "Content-Type, Authorization, X-API-Key, X-User-Id, Idempotency-Key, traceparent, tracestate, Cache-Control, Last-Event-ID, If-Match"

		// Expose custom headers for frontend access (rate limits, tracing, session)
		exposedHeaders := "X-RateLimit-Limit-Minute, X-RateLimit-Remaining-Minute, X-RateLimit-Limit-Hour, X-RateLimit-Remaining-Hour, X-RateLimit-Reset, X-Trace-Id, X-Span-Id, X-Session-ID, X-Shannon-Session-ID"

		// Get allowed origins from environment (comma-separated) or default to wildcard
		allowedOrigins := getEnvOrDefault("CORS_ALLOWED_ORIGINS", "*")

		if !isStreaming {
			// Allow CORS - use specific origins in production, wildcard for dev
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigins)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
			w.Header().Set("Access-Control-Expose-Headers", exposedHeaders)
			w.Header().Set("Access-Control-Max-Age", "3600")
		} else {
			// Streaming endpoints also need CORS headers for GET requests
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigins)
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
			w.Header().Set("Access-Control-Expose-Headers", exposedHeaders)
			w.Header().Set("Access-Control-Max-Age", "3600")
		}

		if r.Method == http.MethodOptions {
			// Handle preflight - headers already set above
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Helper functions
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
