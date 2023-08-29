package alternatesubmit

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/apis/policy"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// CreateConfigMapUtil Helper func to create Spark Application configmap
func createConfigMapUtil(configMapName string, app *v1beta2.SparkApplication, configMapData map[string]string, kubeClient kubernetes.Interface) error {
	configMap := &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            configMapName,
			Namespace:       app.Namespace,
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Data: configMapData,
	}

	createConfigMapErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm, err := kubeClient.CoreV1().ConfigMaps(app.Namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			_, createErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
			return createErr
		}
		if err != nil {
			return err
		}

		cm.Data = configMapData
		_, updateErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
		return updateErr
	})
	return createConfigMapErr
}

// Helper func to create random string of given length to create unique service name for the Spark Application Driver Pod
func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	return hex.EncodeToString(bytes), nil
}

// getServiceName Helper function to get Spark Application Driver Pod's Service Name
func getServiceName(app *v1beta2.SparkApplication) string {
	driverPodServiceName := getDriverPodName(app) + ServiceNameExtension
	if !(len(driverPodServiceName) <= KubernetesDNSLabelNameMaxLength) {
		timeInString := strconv.Itoa(int(time.Now().Unix()))
		randomHexString, _ := randomHex(10)
		randomServiceId := randomHexString + timeInString
		driverPodServiceName = SparkWithDash + randomServiceId + SparkAppDriverServiceNameExtension
	}
	return driverPodServiceName
}

type SupplementalGroup []int64

func Int32Pointer(a int32) *int32 {
	return &a
}

func Int64Pointer(a int64) *int64 {
	return &a
}

func BoolPointer(a bool) *bool {
	return &a
}

func StringPointer(a string) *string {
	return &a
}

func SupplementalGroups(a int64) SupplementalGroup {
	s := SupplementalGroup{a}
	return s
}

func AddEscapeCharacter(configMapArg string) string {
	configMapArg = strings.ReplaceAll(configMapArg, ":", "\\:")
	configMapArg = strings.ReplaceAll(configMapArg, "=", "\\=")
	return configMapArg
}

func GetAppNamespace(app *v1beta2.SparkApplication) string {
	namespace := "default"
	if app.Namespace != "" {
		namespace = app.Namespace
	}
	return namespace
}

// Helper func to get driver pod name from Spark Application CRD instance
func getDriverPodName(app *v1beta2.SparkApplication) string {
	name := app.Spec.Driver.PodName
	if name != nil && len(*name) > 0 {
		return *name
	}
	sparkConf := app.Spec.SparkConf
	if sparkConf[config.SparkDriverPodNameKey] != "" {
		return sparkConf[config.SparkDriverPodNameKey]
	}
	return fmt.Sprintf("%s-driver", app.Name)
}

// Helper func to get Owner references to be added to Spark Application resources - pod, service, configmap
func getOwnerReference(app *v1beta2.SparkApplication) *metav1.OwnerReference {
	controller := true
	return &metav1.OwnerReference{
		APIVersion: v1beta2.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta2.SparkApplication{}).Name(),
		Name:       app.Name,
		UID:        app.UID,
		Controller: &controller,
	}
}

func getMasterURL() (string, error) {
	kubernetesServiceHost := os.Getenv(kubernetesServiceHostEnvVar)
	if kubernetesServiceHost == "" {
		kubernetesServiceHost = "localhost"
	}
	kubernetesServicePort := os.Getenv(kubernetesServicePortEnvVar)
	if kubernetesServicePort == "" {
		kubernetesServicePort = "443"
	}
	return fmt.Sprintf("k8s://https://%s:%s", kubernetesServiceHost, kubernetesServicePort), nil
}

const (
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesServicePortEnvVar = "KUBERNETES_SERVICE_PORT"
)

// addLocalDirConfOptions excludes local dir volumes, update SparkApplication and returns local dir config options
func addLocalDirConfOptions(app *v1beta2.SparkApplication) ([]string, error) {
	var localDirConfOptions []string

	sparkLocalVolumes := map[string]apiv1.Volume{}
	var mutateVolumes []apiv1.Volume

	// Filter local dir volumes
	for _, volume := range app.Spec.Volumes {
		if strings.HasPrefix(volume.Name, config.SparkLocalDirVolumePrefix) {
			sparkLocalVolumes[volume.Name] = volume
		} else {
			mutateVolumes = append(mutateVolumes, volume)
		}
	}
	app.Spec.Volumes = mutateVolumes

	// Filter local dir volumeMounts and set mutate volume mounts to driver and executor
	if app.Spec.Driver.VolumeMounts != nil {
		driverMutateVolumeMounts, driverLocalDirConfConfOptions := filterMutateMountVolumes(app.Spec.Driver.VolumeMounts, config.SparkDriverVolumesPrefix, sparkLocalVolumes)
		app.Spec.Driver.VolumeMounts = driverMutateVolumeMounts
		localDirConfOptions = append(localDirConfOptions, driverLocalDirConfConfOptions...)
	}

	if app.Spec.Executor.VolumeMounts != nil {
		executorMutateVolumeMounts, executorLocalDirConfConfOptions := filterMutateMountVolumes(app.Spec.Executor.VolumeMounts, config.SparkExecutorVolumesPrefix, sparkLocalVolumes)
		app.Spec.Executor.VolumeMounts = executorMutateVolumeMounts
		localDirConfOptions = append(localDirConfOptions, executorLocalDirConfConfOptions...)
	}

	return localDirConfOptions, nil
}

func filterMutateMountVolumes(volumeMounts []apiv1.VolumeMount, prefix string, sparkLocalVolumes map[string]apiv1.Volume) ([]apiv1.VolumeMount, []string) {
	var mutateMountVolumes []apiv1.VolumeMount
	var localDirConfOptions []string
	for _, volumeMount := range volumeMounts {
		if volume, ok := sparkLocalVolumes[volumeMount.Name]; ok {
			options := buildLocalVolumeOptions(prefix, volume, volumeMount)
			for _, option := range options {
				localDirConfOptions = append(localDirConfOptions, option)
			}
		} else {
			mutateMountVolumes = append(mutateMountVolumes, volumeMount)
		}
	}

	return mutateMountVolumes, localDirConfOptions
}

func buildLocalVolumeOptions(prefix string, volume apiv1.Volume, volumeMount apiv1.VolumeMount) []string {
	VolumeMountPathTemplate := prefix + "%s.%s.mount.path=%s"
	VolumeMountOptionTemplate := prefix + "%s.%s.options.%s=%s"

	var options []string
	switch {
	case volume.HostPath != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, string(policy.HostPath), volume.Name, volumeMount.MountPath))
		options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, string(policy.HostPath), volume.Name, "path", volume.HostPath.Path))
		if volume.HostPath.Type != nil {
			options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, string(policy.HostPath), volume.Name, "type", *volume.HostPath.Type))
		}
	case volume.EmptyDir != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, string(policy.EmptyDir), volume.Name, volumeMount.MountPath))
	case volume.PersistentVolumeClaim != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, string(policy.PersistentVolumeClaim), volume.Name, volumeMount.MountPath))
		options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, string(policy.PersistentVolumeClaim), volume.Name, "claimName", volume.PersistentVolumeClaim.ClaimName))
	}

	return options
}

func getResourceLabels(app *v1beta2.SparkApplication) map[string]string {
	labels := map[string]string{config.SparkAppNameLabel: app.Name}
	if app.Status.SubmissionID != "" {
		labels[config.SubmissionIDLabel] = app.Status.SubmissionID
	}
	return labels
}
