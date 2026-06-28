package academic

import (
	"strconv"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	academicpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/academic"
	academicactivities "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities/academic"
)

type AcademicWorkflowParams struct {
	ProjectID  string
	Title      string
	StartPhase int32
	EndPhase   int32
}

func AcademicWorkflow(ctx workflow.Context, params AcademicWorkflowParams) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("AcademicWorkflow started", "project_id", params.ProjectID)

	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 35 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	for phase := params.StartPhase; phase <= params.EndPhase; phase++ {
		logger.Info("Executing phase", "phase", phase)

		_ = workflow.ExecuteActivity(actCtx, academicactivities.RecordEventActivity, &academicpb.RecordEventRequest{
			ProjectId: params.ProjectID,
			EventType: "phase_start",
			Payload:   `{"phase":` + strconv.Itoa(int(phase)) + `}`,
		}).Get(ctx, nil)

		var phaseResult *academicpb.ExecutePhaseResponse
		err := workflow.ExecuteActivity(actCtx, academicactivities.ExecutePhaseActivity, &academicpb.ExecutePhaseRequest{
			ProjectId: params.ProjectID,
			Phase:     phase,
			Title:     params.Title,
		}).Get(ctx, &phaseResult)
		if err != nil {
			logger.Error("Phase execution failed", "phase", phase, "error", err)
			return err
		}

		_ = workflow.ExecuteActivity(actCtx, academicactivities.RecordEventActivity, &academicpb.RecordEventRequest{
			ProjectId: params.ProjectID,
			EventType: "phase_complete",
			Payload:   phaseResult.ResultSummary,
		}).Get(ctx, nil)

		if phase < params.EndPhase {
			var gateResult *academicpb.EvaluateGateResponse
			err := workflow.ExecuteActivity(actCtx, academicactivities.EvaluateGateActivity, &academicpb.EvaluateGateRequest{
				ProjectId:    params.ProjectID,
				CurrentPhase: phase,
				PhaseResult:  phaseResult.ResultSummary,
			}).Get(ctx, &gateResult)
			if err != nil {
				return err
			}
			if !gateResult.Passed {
				logger.Warn("Gate not passed", "phase", phase, "reason", gateResult.Reason)
				return temporal.NewApplicationError(gateResult.Reason, "gate_failed")
			}
		}
	}

	logger.Info("AcademicWorkflow completed", "project_id", params.ProjectID)
	return nil
}
