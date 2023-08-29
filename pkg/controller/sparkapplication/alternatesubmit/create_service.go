package alternatesubmit

import (
	"context"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"time"
)

// Helper func to create Service for the Driver Pod of the Spark Application
func createDriverService(app *v1beta2.SparkApplication, serviceSelectorLabels map[string]string, kubeClient kubernetes.Interface, createdApplicationId string) error {

	//Service Schema populating with specific values/data
	var serviceObjectMetaData metav1.ObjectMeta
	serviceName := getServiceName(app)
	//Driver Pod Service name
	serviceObjectMetaData.Name = serviceName
	// Spark Application Namespace
	serviceObjectMetaData.Namespace = GetAppNamespace(app)
	// Service Schema Owner References
	serviceObjectMetaData.OwnerReferences = []metav1.OwnerReference{*getOwnerReference(app)}
	//Service Schema label
	serviceLabels := map[string]string{config.SparkApplicationSelectorLabel: createdApplicationId}
	serviceObjectMetaData.Labels = serviceLabels
	//Service Schema Annotation
	if app.Spec.Driver.Annotations != nil {
		serviceObjectMetaData.Annotations = app.Spec.Driver.Annotations
	}
	//Service Schema Creation
	driverPodService := &apiv1.Service{
		ObjectMeta: serviceObjectMetaData,
		Spec: apiv1.ServiceSpec{
			ClusterIP: None,
			Ports: []apiv1.ServicePort{
				{
					Name:     DriverPortName,
					Port:     DefaultDriverPort,
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: DefaultDriverPort,
					},
				},
				{
					Name:     BlockManagerPortName,
					Port:     DefaultBlockManagerPort,
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: DefaultBlockManagerPort,
					},
				},
				{
					Name:     UiPortName,
					Port:     UiPort,
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: UiPort,
					},
				},
			},
			Selector:        serviceSelectorLabels,
			SessionAffinity: None,
			Type:            ClusterIP,
		},
	}
	//K8S API Server Call to create Service
	createServiceErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existingService, err := kubeClient.CoreV1().Services(app.Namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			_, createErr := kubeClient.CoreV1().Services(app.Namespace).Create(context.TODO(), driverPodService, metav1.CreateOptions{})
			if createErr == nil {
				return createAndCheckDriverService(kubeClient, app, driverPodService, 2, serviceName)
			}
		}
		if err != nil {
			return err
		}

		//Copying over the data to existing service
		existingService.ObjectMeta = driverPodService.ObjectMeta
		existingService.Spec = driverPodService.Spec
		_, updateErr := kubeClient.CoreV1().Services(app.Namespace).Update(context.TODO(), existingService, metav1.UpdateOptions{})
		return updateErr
	})
	return createServiceErr
}

func createAndCheckDriverService(kubeClient kubernetes.Interface, app *v1beta2.SparkApplication, driverPodService *apiv1.Service, attemptCount int, serviceName string) error {
	const sleepDuration = 2000 * time.Millisecond

	for iteration := 0; iteration < attemptCount; iteration++ {
		_, err := kubeClient.CoreV1().Services(app.Namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			time.Sleep(sleepDuration)
			glog.Info("Service does not exist, attempt #", iteration+2, " to create service for the app %s", app.Name)
			if _, err := kubeClient.CoreV1().Services(app.Namespace).Create(context.TODO(), driverPodService, metav1.CreateOptions{}); err != nil {
				return err
			}
		} else {
			glog.Info("Driver Service found in attempt", iteration+2, "for the app %s", app.Name)
			return nil
		}
	}

	return nil
}
