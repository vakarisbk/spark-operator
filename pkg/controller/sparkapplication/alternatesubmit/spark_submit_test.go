package alternatesubmit

import (
	"github.com/google/uuid"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestRunAltSparkSubmit(t *testing.T) {
	submissionID := uuid.New().String()
	fakeClient := fake.NewSimpleClientset()
	requestSucceeded, errRunAltSparkSubmit := RunAltSparkSubmit(TestApp, submissionID, fakeClient)
	if errRunAltSparkSubmit != nil || !requestSucceeded {
		t.Fatalf("failed to run spark-submit: %v", errRunAltSparkSubmit)
	}
}
