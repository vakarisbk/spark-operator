package alternatesubmit

import (
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestCreateDriverService(t *testing.T) {
	type testcase struct {
		app                  *v1beta2.SparkApplication
		driverConfigMapName  string
		serviceLabels        map[string]string
		submissionID         string
		createdApplicationId string
	}
	fakeClient := fake.NewSimpleClientset()
	serviceLabels := map[string]string{config.SparkAppNameLabel: "test-app"}
	testFn := func(test testcase, t *testing.T) {
		errCreateSparkAppConfigMap := createSparkAppConfigMap(test.app, test.submissionID, test.createdApplicationId, fakeClient, test.driverConfigMapName)
		if errCreateSparkAppConfigMap != nil {
			t.Errorf("failed to create configmap: %v", errCreateSparkAppConfigMap)
		}

		errCreateDriverPod := createDriverPod(test.app, test.serviceLabels, test.driverConfigMapName, fakeClient, test.app.Spec.Driver.VolumeMounts, test.app.Spec.Volumes)
		if errCreateDriverPod != nil {
			t.Errorf("failed to create Driver pod: %v", errCreateDriverPod)
		}
		err := createDriverService(test.app, serviceLabels, fakeClient, "abcdefg123231kkllkjjlkl")
		if err != nil {
			t.Errorf("failed to create driver service: %v", err)
		}
	}
	testcases := []testcase{
		{
			app:                  TestApp,
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{config.SparkAppNameLabel: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}
