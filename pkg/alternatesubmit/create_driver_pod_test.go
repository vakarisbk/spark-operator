package alternatesubmit

import (
	"github.com/kubeflow/spark-operator/pkg/common"
	"testing"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreateSparkAppDriverPod(t *testing.T) {
	type testcase struct {
		app                  *v1beta2.SparkApplication
		driverConfigMapName  string
		serviceLabels        map[string]string
		submissionID         string
		createdApplicationId string
	}
	fakeClient := fake.NewSimpleClientset()
	testFn := func(test testcase, t *testing.T) {
		errCreateSparkAppConfigMap := createSparkAppConfigMap(test.app, test.submissionID, test.createdApplicationId, fakeClient, test.driverConfigMapName)
		if errCreateSparkAppConfigMap != nil {
			t.Errorf("failed to create configmap: %v", errCreateSparkAppConfigMap)
		}

		errCreateDriverPod := createDriverPod(test.app, test.serviceLabels, test.driverConfigMapName, fakeClient, test.app.Spec.Driver.VolumeMounts, test.app.Spec.Volumes)
		if errCreateDriverPod != nil {
			t.Errorf("failed to create Driver pod: %v", errCreateDriverPod)
		}

	}
	testcases := []testcase{
		{
			app:                  TestApp,
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{common.LabelSparkAppName: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
		},
		{
			app:                  TestApp,
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{common.LabelSparkAppName: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
		},
	}
	for index, test := range testcases {
		if index == 1 {
			test.app.Spec.Driver.SecurityContext = nil
			test.app.Spec.Driver.MemoryOverhead = nil
			test.app.Spec.ImagePullPolicy = nil
			test.app.Spec.Image = nil
			test.app.Spec.Driver.Image = stringPointer("gcr.io/spark-operator/spark:v3.1.1")
			test.app.Spec.MemoryOverheadFactor = stringPointer("400")
			test.app.Spec.Driver.Memory = nil
			test.app.Spec.Driver.CoreLimit = nil
			test.app.Spec.Driver.Cores = nil

		}
		testFn(test, t)
	}
}
