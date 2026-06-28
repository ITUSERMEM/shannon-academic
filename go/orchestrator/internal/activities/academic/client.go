package academic

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	academicpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/academic"
)

var academicClient academicpb.AcademicServiceClient

func InitAcademicClient(target string) error {
	conn, err := grpc.Dial(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	academicClient = academicpb.NewAcademicServiceClient(conn)
	// Give the gRPC connection time to be established
	_ = time.Sleep
	return nil
}

func ExecutePhaseActivity(ctx context.Context, req *academicpb.ExecutePhaseRequest) (*academicpb.ExecutePhaseResponse, error) {
	return academicClient.ExecutePhase(ctx, req)
}

func EvaluateGateActivity(ctx context.Context, req *academicpb.EvaluateGateRequest) (*academicpb.EvaluateGateResponse, error) {
	return academicClient.EvaluateGate(ctx, req)
}

func RecordEventActivity(ctx context.Context, req *academicpb.RecordEventRequest) (*academicpb.RecordEventResponse, error) {
	return academicClient.RecordEvent(ctx, req)
}

func GetBudgetActivity(ctx context.Context, req *academicpb.GetBudgetRequest) (*academicpb.GetBudgetResponse, error) {
	return academicClient.GetBudget(ctx, req)
}
