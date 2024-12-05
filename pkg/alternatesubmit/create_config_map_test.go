package alternatesubmit

import (
	"context"
	"testing"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var memoryQuantity resource.Quantity
var cpuQuantity resource.Quantity
var TestApp = &v1beta2.SparkApplication{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test-app",
		Namespace: "default",
		Labels:    map[string]string{"sparkdeps": "v3"},
	},
	Spec: v1beta2.SparkApplicationSpec{
		Arguments:       []string{"pwd"},
		Type:            v1beta2.SparkApplicationTypeScala,
		Mode:            v1beta2.DeployModeCluster,
		Image:           stringPointer("gcr.io/spark-operator/spark:v3.1.1"),
		ImagePullPolicy: stringPointer("IfNotPresent"),
		ImagePullSecrets: []string{
			"dummy-data",
		},
		MainClass:           stringPointer("org.apache.spark.examples.SparkPi"),
		MainApplicationFile: stringPointer("local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar"),
		SparkVersion:        "",
		NodeSelector: map[string]string{
			"spark.authenticate": "true"},
		RestartPolicy: v1beta2.RestartPolicy{
			Type: "Never",
		},
		SparkConf: map[string]string{
			"spark.authenticate":                            "true",
			"spark.io.encryption.enabled":                   "true",
			"spark.network.crypto.enabled":                  "true",
			"spark.kubernetes.local.dirs.tmpfs":             "true",
			"spark.kubernetes.driver.SPARK_LOCAL_DIRS":      "/tmp/spark-local-dir-2,/tmp/spark-local-dir-1,/tmp/spark-local-dir-100",
			"spark.kubernetes.driverEnv.SPARK_LOCAL_DIRS":   "/tmp/spark-local-dir-2,/tmp/spark-local-dir-1,/tmp/spark-local-dir-100",
			"spark.kubernetes.kerberos.tokenSecret.itemKey": "test",
		},
		Driver: v1beta2.DriverSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{

				Cores: int32Pointer(1),

				ConfigMaps: []v1beta2.NamePath{
					{Name: "test-configmap",
						Path: "/etc/ccp/lldc/rsyslog"},
				},
				CoreLimit: stringPointer("1200m"),
				Env: []corev1.EnvVar{{
					Name:  "Dummy-env",
					Value: "dummy-env-val",
				},
				},
				EnvVars: map[string]string{
					"Name":  "Dummy-env",
					"Value": "dummy-env-val",
				},

				MemoryOverhead: stringPointer("10m"),
				Memory:         stringPointer("512m"),
				Labels:         labelsForSpark(),
				ServiceAccount: stringPointer("spark"),
				Annotations: map[string]string{
					"opencensus.k8s-integration.sfdc.com/inject":             "enabled",
					"opencensus.k8s-integration.sfdc.com/prometheus-targets": `[{"path": "/metrics","port": "8090","container_name": "spark-kubernetes-driver"}]`,
				},
				PodSecurityContext: &corev1.PodSecurityContext{
					RunAsUser:    int64Pointer(185),
					RunAsNonRoot: booleanPointer(true),
				},
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:    int64Pointer(185),
					RunAsNonRoot: booleanPointer(true),
				},
				Secrets: []v1beta2.SecretInfo{
					{
						Name: "f8d3bdf3-a20b-448c-b2ae-bf14ba4bffc6",
						Path: "/etc/ccp-secrets",
						Type: "Generic",
					},
				},

				Sidecars: []corev1.Container{
					{
						Command: []string{
							"/usr/sbin/rsyslogd",
							"-n",
							"-f",
							"/etc/ccp/lldc/rsyslog/rsyslog.conf",
						},
						Env: []corev1.EnvVar{
							{
								Name:  "LLDC_BUS",
								Value: "einstein.test.streaming__aws.perf2-uswest2.einstein.ajnalocal1__strm.lldcbus",
							},
						},
						Image: "331455399823.dkr.ecr.us-east-2.amazonaws.com/sfci/monitoring/sfdc_rsyslog_gcp:latest",
						Name:  "ccp-lldc",
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								Memory: memoryQuantity,
								Cpu:    cpuQuantity,
							},
							Requests: corev1.ResourceList{
								Memory: memoryQuantity,
								Cpu:    cpuQuantity,
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								MountPath: "/etc/ccp/lldc/applogs",
								Name:      "ccp-lldc-applogs",
							},
							{
								MountPath: "/etc/ccp/lldc/statedir",
								Name:      "ccp-lldc-statedir",
							},
						},
					},
				},

				VolumeMounts: []corev1.VolumeMount{

					{
						MountPath: "/etc/ccp/lldc/applogs",
						Name:      "ccp-lldc-applogs",
					},
					{
						MountPath: "/etc/ccp/lldc/statedir",
						Name:      "ccp-lldc-statedir",
					},
					{
						MountPath: "/tmp/spark-local-dir-100",
						Name:      "spark-local-dir-100",
					},
					{
						MountPath: "/tmp/spark-local-dir-1",
						Name:      "spark-local-dir-1",
					},
					{
						MountPath: "/tmp/spark-local-dir-2",
						Name:      "spark-local-dir-2",
					},
				},
			},
			CoreRequest: stringPointer("1000m"),
		},
		Executor: v1beta2.ExecutorSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{
				Annotations: map[string]string{
					"opencensus.k8s-integration.sfdc.com/inject":             "enabled",
					"opencensus.k8s-integration.sfdc.com/prometheus-targets": `[{"path": "/metrics","port": "8090","container_name": "spark-kubernetes-driver"}]`,
				},
				Cores: int32Pointer(1),

				Labels: labelsForSpark(),
				Memory: stringPointer("512m"),
				PodSecurityContext: &corev1.PodSecurityContext{
					RunAsUser:    int64Pointer(185),
					RunAsNonRoot: booleanPointer(true),
				},
			},
		},
		Monitoring: &v1beta2.MonitoringSpec{
			ExposeDriverMetrics:   true,
			ExposeExecutorMetrics: true,
		},
		Volumes: []corev1.Volume{

			{
				Name: "ccp-lldc-applogs",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium: "Memory",
					},
				},
			},

			{
				Name: "ccp-lldc-statedir",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium: "Memory",
					},
				},
			},
			{
				Name: "spark-local-dir-100",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium: "Memory",
					},
				},
			},
		},
	},
}

func TestCreateSparkAppConfigMap(t *testing.T) {

	type testcase struct {
		app                  *v1beta2.SparkApplication
		submissionID         string
		createdApplicationId string
		driverConfigMapName  string
	}

	fakeClient := fake.NewSimpleClientset()
	testFn := func(test testcase, t *testing.T) {
		err := createSparkAppConfigMap(test.app, test.submissionID, test.createdApplicationId, fakeClient, test.driverConfigMapName)
		if err != nil {
			t.Errorf("failed to create configmap: %v", err)
		}

		_, errGetConfigMap := fakeClient.CoreV1().ConfigMaps(test.app.Namespace).Get(context.TODO(), "test-app-driver-configmap", metav1.GetOptions{})
		if errGetConfigMap != nil {
			t.Errorf("failed to get ConfigMap %s: %v", "test-app-driver-configmap", err)
		}

	}
	testcases := []testcase{
		{
			app:                  TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverConfigMapName:  "test-app-driver-configmap",
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}

func TestBuildAltSubmissionCommandArgs(t *testing.T) {

	type testcase struct {
		app                  *v1beta2.SparkApplication
		driverPodName        string
		submissionID         string
		createdApplicationId string
	}
	testFn := func(test testcase, t *testing.T) {
		_, err := buildAltSubmissionCommandArgs(test.app, test.driverPodName, test.submissionID, test.createdApplicationId)
		if err != nil {
			t.Errorf("failed to build Spark Application Submission Arguments: %v", err)
		}
	}
	testcases := []testcase{
		{
			app:                  TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}
func stringPointer(a string) *string {
	return &a
}

func int32Pointer(a int32) *int32 {
	return &a
}

func int64Pointer(a int64) *int64 {
	return &a
}

func booleanPointer(a bool) *bool {
	return &a
}
func labelsForSpark() map[string]string {
	return map[string]string{"version": "3.0.1"}
}
