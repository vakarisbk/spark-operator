package alternatesubmit

import (
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestCreateSparkAppConfigMapUtil(t *testing.T) {
	//CreateConfigMapUtil(configMapName string, app *v1beta2.SparkApplication, configMapData map[string]string, kubeClient kubernetes.Interface) error

	type testcase struct {
		app           *v1beta2.SparkApplication
		configMapName string
		configMapData map[string]string
	}
	fakeClient := fake.NewSimpleClientset()
	testFn := func(test testcase, t *testing.T) {
		err := createConfigMapUtil(test.configMapName, test.app, test.configMapData, fakeClient)
		if err != nil {
			t.Errorf("Unit test for createConfigMapUtil() function failed with error : %v", err)
		}

	}
	testcases := []testcase{
		{
			app:           TestApp,
			configMapName: "test-app-driver-config-map",
			configMapData: map[string]string{"app-name": "test-app"},
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}

func TestRandomHex(t *testing.T) {
	_, error := randomHex(10)
	if error != nil {
		t.Fatalf(`Unit test for randomHex() function failed with error: %v`, error)
	}
}

func TestGetServiceName(t *testing.T) {

	type testcase struct {
		app *v1beta2.SparkApplication
	}

	testFn := func(test testcase, t *testing.T) {
		serviceName := getServiceName(test.app)
		if serviceName == "" {
			t.Fatalf(`Unit test for getServiceName() function failed`)
		}

	}
	testcases := []testcase{
		{
			app: getServiceNameFunctionTestData1,
		},
		{
			app: getServiceNameFunctionTestData2,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}

var getServiceNameFunctionTestData1 = &v1beta2.SparkApplication{
	Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.ScalaApplicationType,
		Driver: v1beta2.DriverSpec{
			PodName: StringPointer("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"),
		},
	},
}

var getServiceNameFunctionTestData2 = &v1beta2.SparkApplication{
	Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.ScalaApplicationType,
		Driver: v1beta2.DriverSpec{
			PodName: StringPointer("abcdefghijk"),
		},
	},
}

func TestGetDriverPodName(t *testing.T) {
	//fmt.Sprintf("getDriverPodName call return  " + getDriverPodName(TestApp))
	//if getDriverPodName(TestApp) != "" {
	//	t.Fatalf(`Unit test for getDriverPodName() failed`)
	//}
	podName := getDriverPodName(TestApp)
	if podName == "" {
		t.Fatalf(`Unit test for getDriverPodName() failed`)
	}
}

var TestDataGetDriverPodNamevariant1 = &v1beta2.SparkApplication{
	Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.ScalaApplicationType,
		SparkConf: map[string]string{
			"spark.kubernetes.driver.pod.name": "test-pod",
		},
	},
}

func TestAddLocalDirConfOptions(t *testing.T) {

	testFn := func(test testcase, t *testing.T) {

		_, err := addLocalDirConfOptions(test.app)
		if err != nil {
			t.Fatalf(`Unit test for addLocalDirConfOptions() failed`)
		}

	}
	testcases := TestCasesList
	for index, test := range testcases {
		indexedProcessing(index, test)
		testFn(test, t)
	}

}

type testcase struct {
	app *v1beta2.SparkApplication
}

var TestCasesList = []testcase{
	{
		app: TestApp,
	},
	{
		app: TestApp,
	},
	{
		app: TestApp,
	},
}

func indexedProcessing(index int, test testcase) {
	if index == 1 {
		test.app.Spec.Volumes = nil
		test.app.Spec.Driver.VolumeMounts = nil
	}
}
