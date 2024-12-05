package alternatesubmit

import (
	"context"
	"fmt"
	"github.com/kubeflow/spark-operator/pkg/common"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// Helper func to create Driver Pod of the Spark Application
func createDriverPod(app *v1beta2.SparkApplication, serviceLabels map[string]string, driverConfigMapName string, kubeClient kubernetes.Interface, appSpecVolumeMounts []apiv1.VolumeMount, appSpecVolumes []apiv1.Volume) error {
	// Spark Application Driver Pod schema populating with specific values/data
	var podObjectMetadata metav1.ObjectMeta
	//Driver Pod Name
	podObjectMetadata.Name = getDriverPodName(app)
	//Driver pod Namespace
	podObjectMetadata.Namespace = GetAppNamespace(app)
	//Driver Pod labels
	podObjectMetadata.Labels = serviceLabels
	//Driver pod annotations
	if app.Spec.Driver.Annotations != nil {
		podObjectMetadata.Annotations = app.Spec.Driver.Annotations
	}
	//Driver Pod Owner Reference
	podObjectMetadata.OwnerReferences = []metav1.OwnerReference{*getOwnerReference(app)}

	//Driver pod spec instance
	var driverPodSpec apiv1.PodSpec

	//Driver pod DNS policy
	driverPodSpec.DNSPolicy = SparkDriverDNSPolicy

	//Driver pod enable service link
	driverPodSpec.EnableServiceLinks = BoolPointer(true)

	//Driver pod node selector
	if app.Spec.Driver.NodeSelector != nil {
		driverPodSpec.NodeSelector = app.Spec.Driver.NodeSelector
	}
	//Image pull Secrets
	if app.Spec.ImagePullSecrets != nil {
		imagePullSecrets := app.Spec.ImagePullSecrets
		var imagePullSecretsList []apiv1.LocalObjectReference
		for _, secretName := range imagePullSecrets {
			var imagePullSecret apiv1.LocalObjectReference
			imagePullSecret.Name = secretName
			imagePullSecretsList = append(imagePullSecretsList, imagePullSecret)
		}
		driverPodSpec.ImagePullSecrets = imagePullSecretsList
	}

	//RestartPolicy
	driverPodSpec.RestartPolicy = DriverPodRestartPolicyNever
	//Driver pod security context
	if app.Spec.Driver.SecurityContext != nil {
		if app.Spec.Driver.SecurityContext.RunAsUser != nil || app.Spec.Driver.SecurityContext.RunAsNonRoot != nil {
			var podSecurityContext apiv1.PodSecurityContext
			if app.Spec.Driver.SecurityContext.RunAsUser != nil {
				podSecurityContext.RunAsUser = app.Spec.Driver.SecurityContext.RunAsUser
				podSecurityContext.FSGroup = app.Spec.Driver.SecurityContext.RunAsUser
				podSecurityContext.SupplementalGroups = SupplementalGroups(*app.Spec.Driver.SecurityContext.RunAsUser)
			}
			if app.Spec.Driver.SecurityContext.RunAsNonRoot != nil {
				podSecurityContext.RunAsNonRoot = app.Spec.Driver.SecurityContext.RunAsNonRoot
			}

			driverPodSpec.SecurityContext = &podSecurityContext
		}
	} else {
		driverPodSpec.SecurityContext = &apiv1.PodSecurityContext{
			RunAsUser: Int64Pointer(DriverPodSecurityContextID),
			FSGroup:   Int64Pointer(DriverPodSecurityContextID),
			//Run as non-root
			RunAsNonRoot:       BoolPointer(true),
			SupplementalGroups: SupplementalGroups(DriverPodSecurityContextID),
		}
	}
	//Service Account
	if app.Spec.Driver.ServiceAccount != nil {
		driverPodSpec.ServiceAccountName = *app.Spec.Driver.ServiceAccount
	}
	//Termination grace period
	if app.Spec.Driver.TerminationGracePeriodSeconds != nil {
		driverPodSpec.TerminationGracePeriodSeconds = app.Spec.Driver.TerminationGracePeriodSeconds
	} else {
		driverPodSpec.TerminationGracePeriodSeconds = Int64Pointer(DefaultTerminationGracePeriodSeconds)
	}
	//Tolerations
	if app.Spec.Driver.Tolerations != nil {
		driverPodSpec.Tolerations = app.Spec.Driver.Tolerations
	} else {
		//Assigning default toleration
		var tolerations []apiv1.Toleration
		tolerations = []apiv1.Toleration{
			{
				Effect:            TolerationEffect,
				Key:               NodeNotReady,
				Operator:          Operator,
				TolerationSeconds: Int64Pointer(DefaultTolerationSeconds),
			},
			{
				Effect:            TolerationEffect,
				Key:               NodeNotReachable,
				Operator:          Operator,
				TolerationSeconds: Int64Pointer(DefaultTolerationSeconds),
			},
		}
		driverPodSpec.Tolerations = tolerations
	}

	//Pod Volumes setup
	var driverPodVolumes []apiv1.Volume

	// spark-conf-volume-driver addition
	sparkConfVolume := apiv1.Volume{
		Name: SparkConfVolumeDriver,
		VolumeSource: apiv1.VolumeSource{
			ConfigMap: &apiv1.ConfigMapVolumeSource{
				DefaultMode: Int32Pointer(420),
				Items: []apiv1.KeyToPath{
					{
						Key:  SparkEnvScriptFileName,
						Mode: Int32Pointer(420),
						Path: SparkEnvScriptFileName,
					},
					{
						Key:  SparkPropertiesFileName,
						Mode: Int32Pointer(420),
						Path: SparkPropertiesFileName,
					},
				},
				LocalObjectReference: apiv1.LocalObjectReference{Name: driverConfigMapName},
			},
		},
	}

	driverPodVolumes = append(driverPodVolumes, sparkConfVolume)

	driverPodContainerSpec, resolvedLocalDirs := CreateDriverPodContainerSpec(app)
	var containerSpecList []apiv1.Container
	localDirFeatureSetupError := handleLocalDirsFeatureStep(app, resolvedLocalDirs, &driverPodVolumes, &driverPodContainerSpec.VolumeMounts, &driverPodContainerSpec.Env, appSpecVolumeMounts, appSpecVolumes)
	if localDirFeatureSetupError != nil {
		return fmt.Errorf("failed to setup local directory for the driver pod %s in namespace %s: %v", getDriverPodName(app), app.Namespace, localDirFeatureSetupError)
	}

	//mountSecretFeatureStepError := handleMountSecretFeatureStep(app, &driverPodVolumes, &driverPodContainerSpec.VolumeMounts)
	//if mountSecretFeatureStepError != nil {
	//	return fmt.Errorf("failed to setup secrets for the driver pod %s in namespace %s: %v", getDriverPodName(app), app.Namespace, mountSecretFeatureStepError)
	//}
	volumeExtension := "-volume"
	if app.Spec.Driver.Secrets != nil {
		for _, secret := range app.Spec.Driver.Secrets {
			secretVolume := apiv1.Volume{
				Name: fmt.Sprintf("%s%s", secret.Name, volumeExtension),
				VolumeSource: apiv1.VolumeSource{
					Secret: &apiv1.SecretVolumeSource{
						SecretName: secret.Name,
					},
				},
			}
			driverPodVolumes = append(driverPodVolumes, secretVolume)

			//Volume mount for the local temp folder
			volumeMount := apiv1.VolumeMount{
				Name:      fmt.Sprintf("%s%s", secret.Name, volumeExtension),
				MountPath: secret.Path,
			}
			driverPodContainerSpec.VolumeMounts = append(driverPodContainerSpec.VolumeMounts, volumeMount)

		}
	}

	containerSpecList = append(containerSpecList, driverPodContainerSpec)

	containerSpecList = handleSideCars(app, containerSpecList, appSpecVolumes)

	if app.Spec.Driver.InitContainers != nil {
		driverPodSpec.InitContainers = app.Spec.Driver.InitContainers
	}

	driverPodSpec.Containers = containerSpecList

	// Setting up pod volumes
	driverPodSpec.Volumes = driverPodVolumes

	driverPod := &apiv1.Pod{
		ObjectMeta: podObjectMetadata,
		Spec:       driverPodSpec,
	}

	//Check existence of pod
	createPodErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		driverPodExisting, err := kubeClient.CoreV1().Pods(app.Namespace).Get(context.TODO(), podObjectMetadata.Name, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			_, createErr := kubeClient.CoreV1().Pods(app.Namespace).Create(context.TODO(), driverPod, metav1.CreateOptions{})
			return createErr
		}
		if err != nil {
			return err
		}
		driverPodExisting.ObjectMeta = podObjectMetadata
		driverPodExisting.Spec = driverPodSpec
		_, updateErr := kubeClient.CoreV1().Pods(app.Namespace).Update(context.TODO(), driverPodExisting, metav1.UpdateOptions{})
		return updateErr
	})

	if createPodErr != nil {
		return fmt.Errorf("failed to create/update driver pod %s in namespace %s: %v", getDriverPodName(app), app.Namespace, createPodErr)
	}

	return nil
}

func handleSideCars(app *v1beta2.SparkApplication, containerSpecList []apiv1.Container, appSpecVolumes []apiv1.Volume) []apiv1.Container {
	if app.Spec.Driver.Sidecars != nil {
		var sideCarVolumeMounts []apiv1.VolumeMount
		for _, configMap := range app.Spec.Driver.ConfigMaps {
			//Volume mount for the configmap
			volumeMount := apiv1.VolumeMount{
				Name:      configMap.Name + "-vol",
				MountPath: configMap.Path,
			}
			sideCarVolumeMounts = append(sideCarVolumeMounts, volumeMount)
		}

		for _, sideCarContainer := range app.Spec.Driver.Sidecars {
			//Handle volume mounts
			for _, volumeTobeMounted := range sideCarContainer.VolumeMounts {
				//Volume mount for the volumes
				if checkVolumeMountIsVolume(appSpecVolumes, volumeTobeMounted.Name) {
					volumeMount := apiv1.VolumeMount{
						Name:      volumeTobeMounted.Name,
						MountPath: volumeTobeMounted.MountPath,
						ReadOnly:  volumeTobeMounted.ReadOnly,
					}
					sideCarVolumeMounts = append(sideCarVolumeMounts, volumeMount)
				}
			}
			sideCarContainer.VolumeMounts = nil
			if len(sideCarVolumeMounts) > 0 {
				sideCarContainer.VolumeMounts = sideCarVolumeMounts
			}

			containerSpecList = append(containerSpecList, sideCarContainer)
		}
	}
	return containerSpecList
}

func checkVolumeMountIsVolume(appSpecVolumes []apiv1.Volume, volumeName string) bool {
	for _, appSpecVolume := range appSpecVolumes {
		if appSpecVolume.Name == volumeName {
			return true
		}
	}
	return false
}

// CreateDriverPodContainerSpec Helper func to create Driver Pod Driver contianer spec creation
func CreateDriverPodContainerSpec(app *v1beta2.SparkApplication) (apiv1.Container, []string) {
	var driverPodContainerSpec apiv1.Container
	mainClass := ""
	mainApplicationFile := ""
	var args []string

	if app.Spec.Arguments != nil {
		args = app.Spec.Arguments
	}

	if app.Spec.MainClass != nil {
		mainClass = *app.Spec.MainClass
	}

	if app.Spec.MainApplicationFile != nil {
		mainApplicationFile = *app.Spec.MainApplicationFile
	}

	//Driver Container default arguments
	driverPodContainerSpec.Args = []string{
		SparkDriverArg,
		SparkDriverArgPropertiesFile,
		SparkDriverArgPropertyFilePath,
		SparkDriverArgClass,
		mainClass,
		mainApplicationFile,
	}
	if args != nil {
		for _, arg := range args {
			driverPodContainerSpec.Args = append(driverPodContainerSpec.Args, arg)
		}
	}

	var driverPodContainerEnvVars []apiv1.EnvVar

	//Spark User details
	var sparkUserDetails apiv1.EnvVar
	if os.Getenv(SparkUser) != "" {
		sparkUserDetails.Value = os.Getenv(SparkUser)
	} else {
		sparkUserDetails.Value = SparkUserId
	}
	sparkUserDetails.Name = SparkUser
	driverPodContainerEnvVars = append(driverPodContainerEnvVars, sparkUserDetails)

	//Spark Application ID
	var sparkAppDetails apiv1.EnvVar
	sparkAppDetails.Name = SparkApplicationID
	sparkAppDetails.Value = app.Status.SparkApplicationID
	driverPodContainerEnvVars = append(driverPodContainerEnvVars, sparkAppDetails)

	//Spark Driver Bind Address
	var driverPodContainerEnvVarBindAddress apiv1.EnvVar
	driverPodContainerEnvVarBindAddress.Name = SparkDriverBindAddress
	driverPodContainerEnvVarBindAddress.ValueFrom = &apiv1.EnvVarSource{
		FieldRef: &apiv1.ObjectFieldSelector{
			APIVersion: ApiVersionV1,
			FieldPath:  SparkDriverPodIP,
		},
	}
	driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvVarBindAddress)

	// Add all the spark.kubernetes.driverEnv. prefixed sparkConf key value pairs
	var resolvedLocalDirs []string
	resolvedLocalDirs, driverPodContainerEnvVars = processSparkConfEnv(app, driverPodContainerEnvVars)
	sparkConfKeyValuePairs := app.Spec.SparkConf

	// Adding Secrets as environment variables for the https://spark.apache.org/docs/latest/running-on-kubernetes.html#secret-management
	//if app.Spec.Driver.EnvSecretKeyRefs != nil {
	//	for _, value := range app.Spec.Driver.EnvSecretKeyRefs {
	//		var driverPodContainerEnvVar apiv1.EnvVar
	//		driverPodContainerEnvVar.Name = value.Name
	//		driverPodContainerEnvVar.Value = value.Key
	//		driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvVar)
	//	}
	//}

	// Addition of the spark.kubernetes.kerberos.tokenSecret.itemKey
	// Handling https://spark.apache.org/docs/3.0.0-preview2/security.html#long-running-applications
	//spark.kubernetes.kerberos.tokenSecret.name spark.kubernetes.kerberos.tokenSecret.itemKey
	sparkConfValue, valueExists := sparkConfKeyValuePairs[KerberosTokenSecretItemKey]
	if valueExists {
		var driverPodContainerEnvVar apiv1.EnvVar
		driverPodContainerEnvVar.Name = KerberosHadoopSecretFilePathKey
		driverPodContainerEnvVar.Value = KerberosHadoopSecretFilePath + sparkConfValue
		driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvVar)
	}

	//Spark Config directory
	var sparkConfigDir apiv1.EnvVar
	sparkConfigDir.Name = common.EnvSparkConfDir
	sparkConfigDir.Value = SparkConfVolumeDriverMountPath
	driverPodContainerEnvVars = append(driverPodContainerEnvVars, sparkConfigDir)
	//Assign the Driver Pod Container Environment variables to Container Spec
	driverPodContainerSpec.Env = driverPodContainerEnvVars

	//Assign Driver Pod container image

	if app.Spec.Driver.Image != nil {
		driverPodContainerSpec.Image = *app.Spec.Driver.Image
	} else if app.Spec.Image != nil {
		driverPodContainerSpec.Image = *app.Spec.Image
	}

	if app.Spec.ImagePullPolicy != nil {
		driverPodContainerSpec.ImagePullPolicy = apiv1.PullPolicy(*app.Spec.ImagePullPolicy)
	} else {
		//Default value
		driverPodContainerSpec.ImagePullPolicy = ImagePullPolicyIfNotPresent
	}

	//Driver Pod Container Name
	driverPodContainerSpec.Name = common.SparkDriverContainerName

	//Driver pod contianer ports
	driverPodContainerSpec.Ports = []apiv1.ContainerPort{
		{
			ContainerPort: DefaultDriverPort,
			Name:          DriverPortName,
			Protocol:      Protocol,
		},
		{
			ContainerPort: DefaultBlockManagerPort,
			Name:          BlockManagerPortName,
			Protocol:      Protocol,
		},
		{
			ContainerPort: UiPort,
			Name:          UiPortName,
			Protocol:      Protocol,
		},
	}

	//Driver pod container cpu and memory requests and limits populating
	driverPodContainerSpec.Resources = handleResources(app)

	//Security Context
	driverPodContainerSpec.SecurityContext = &apiv1.SecurityContext{
		Capabilities: &apiv1.Capabilities{
			Drop: []apiv1.Capability{All},
		},
		Privileged: BoolPointer(false),
	}
	//Driver pod termination path
	driverPodContainerSpec.TerminationMessagePath = DriverPodTerminationLogPath
	//Driver pod termination message policy
	driverPodContainerSpec.TerminationMessagePolicy = DriverPodTerminationMessagePolicy
	//Driver pod container volume mounts
	var volumeMounts []apiv1.VolumeMount

	//Volume mount for the configmap
	volumeMount := apiv1.VolumeMount{
		Name:      SparkConfVolumeDriver,
		MountPath: SparkConfVolumeDriverMountPath,
	}
	volumeMounts = append(volumeMounts, volumeMount)

	volumeMounts = handleKerberosCreds(app, volumeMounts)

	driverPodContainerSpec.VolumeMounts = volumeMounts

	return driverPodContainerSpec, resolvedLocalDirs
}

func checkMountingKubernetesCredentials(sparkConfKeyValuePairs map[string]string) bool {
	if sparkConfKeyValuePairs != nil {
		_, OAuthTokenConfFileExists := sparkConfKeyValuePairs[OAuthTokenConfFile]
		_, ClientKeyFileExists := sparkConfKeyValuePairs[ClientKeyFile]
		_, ClientCertFileExists := sparkConfKeyValuePairs[ClientCertFile]
		_, CaCertFileExists := sparkConfKeyValuePairs[CaCertFile]
		if OAuthTokenConfFileExists || ClientKeyFileExists || ClientCertFileExists || CaCertFileExists {
			return true
		}
	}
	return false
}

// Refers to codebase - https://github.com/apache/spark/blob/master/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/BasicDriverFeatureStep.scala#L79-L82
func incorporateMemoryOvehead(memoryNumber int, app *v1beta2.SparkApplication, memoryUnit string) string {
	//Memory Overhead or Memory OverheadFactor incorporating
	var memory string
	if app.Spec.Driver.MemoryOverhead != nil {
		memoryOverhead := *app.Spec.Driver.MemoryOverhead
		//both memory and memoryOverhead are in same Memory Unit(MiB or GiB)
		// Amount of memory to use for the driver process, i.e. where SparkContext is initialized, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).
		//https://spark.apache.org/docs/latest/configuration.html
		memoryOverheadInMiB := processMemoryUnit(memoryOverhead)

		memoryWithOverhead := memoryNumber + memoryOverheadInMiB
		memory = fmt.Sprintf("%d%s", memoryWithOverhead, memoryUnit)

	} else if app.Spec.MemoryOverheadFactor != nil {
		MemoryOveheadMinInMiB := 384.0
		memoryOverheadFactor, _ := strconv.ParseFloat(*app.Spec.MemoryOverheadFactor, 64)
		memoryWithOverhead := memoryOverheadFactor * float64(memoryNumber)
		memoryOverheadFactorInclusion := math.Max(MemoryOveheadMinInMiB, memoryWithOverhead)
		memoryWithOverheadFactor := float64(memoryNumber) + memoryOverheadFactorInclusion
		memory = fmt.Sprintf("%F%s", memoryWithOverheadFactor, memoryUnit)
	} else {
		memory = fmt.Sprintf("%d%s", memoryNumber, memoryUnit)
	}
	return memory
}

func processMemoryUnit(memoryData string) int {

	memoryData = strings.TrimSpace(memoryData)
	memoryData = strings.ToUpper(memoryData)
	units := map[string]float64{
		"K": 0.0009765625,
		"M": 1,
		"G": 1024,
		"T": 1024 * 1024,
	}
	for unit, multiplier := range units {
		if strings.Contains(memoryData, unit) {
			memoryNumber := memoryData[:strings.Index(memoryData, unit)]
			fmt.Println(memoryNumber)
			memoryConverted, _ := strconv.Atoi(memoryNumber)
			return int(float64(memoryConverted) * multiplier)
		}
	}
	//handling default case
	memoryInMiB, _ := strconv.Atoi(memoryData)
	return memoryInMiB
}

func processSparkConfEnv(app *v1beta2.SparkApplication, driverPodContainerEnvVars []apiv1.EnvVar) ([]string, []apiv1.EnvVar) {
	var resolvedLocalDirs []string
	sparkConfKeyValuePairs := app.Spec.SparkConf
	if sparkConfKeyValuePairs != nil {
		var driverPodContainerEnvConfigVars apiv1.EnvVar
		for sparkConfKey, sparkConfValue := range sparkConfKeyValuePairs {
			if strings.Contains(sparkConfKey, SparkDriverEnvPrefix) {
				lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
				driverPodContainerEnvConfigVarKey := sparkConfKey[lastDotIndex+1:]
				if driverPodContainerEnvConfigVarKey == "SPARK_LOCAL_DIRS" {
					resolvedLocalDirs = strings.Split(sparkConfValue, ",")
				}
				driverPodContainerEnvConfigVars.Name = driverPodContainerEnvConfigVarKey
				driverPodContainerEnvConfigVars.Value = sparkConfValue
				driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvConfigVars)
			}
		}
	}
	// Add envVars of Driver portion to Driver Pod Spec
	sparkDriverConfKeyValuePairs := app.Spec.Driver.EnvVars
	if sparkDriverConfKeyValuePairs != nil {
		var driverPodContainerEnvVar apiv1.EnvVar
		for sparkDriverEnvKey, sparkDriverEnvValue := range sparkDriverConfKeyValuePairs {
			driverPodContainerEnvVar.Name = sparkDriverEnvKey
			driverPodContainerEnvVar.Value = sparkDriverEnvValue
			driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvVar)
		}
	}
	return resolvedLocalDirs, driverPodContainerEnvVars
}

func handleKerberosCreds(app *v1beta2.SparkApplication, volumeMounts []apiv1.VolumeMount) []apiv1.VolumeMount {
	sparkConfKeyValuePairs := app.Spec.SparkConf
	if sparkConfKeyValuePairs != nil {
		_, kerberosPatheExists := sparkConfKeyValuePairs[KerberosPath]
		_, kerberosConfigMapNameExists := sparkConfKeyValuePairs[KerberosConfigMapName]
		if kerberosPatheExists || kerberosConfigMapNameExists {
			kerberosConfigMapVolumeMount := apiv1.VolumeMount{
				Name:      KerberosFileVolume,
				MountPath: KerberosFileDirectoryPath + ForwardSlash + KerberosFileName,
				SubPath:   KerberosFileName,
			}
			volumeMounts = append(volumeMounts, kerberosConfigMapVolumeMount)
		}
	}

	if checkMountingKubernetesCredentials(app.Spec.SparkConf) {
		kubernetesCredentialsVolumeMount := apiv1.VolumeMount{
			Name:      KubernetesCredentials,
			MountPath: KubernetesCredentialsVolumeMountPath,
		}
		volumeMounts = append(volumeMounts, kubernetesCredentialsVolumeMount)
	}
	return volumeMounts
}

func handleResources(app *v1beta2.SparkApplication) apiv1.ResourceRequirements {
	var driverPodResourceRequirement apiv1.ResourceRequirements
	var memoryQuantity resource.Quantity
	var cpuQuantity resource.Quantity
	var memoryInBytes string
	memoryValExists := false
	cpuValExists := false
	//Memory Request
	if app.Spec.Driver.Memory != nil {
		memoryData := *app.Spec.Driver.Memory
		// Identify memory unit and convert everything in MiB for uniformity
		memoryInMiB := processMemoryUnit(memoryData)
		memoryInBytes = incorporateMemoryOvehead(memoryInMiB, app, "Mi")
		memoryQuantity = resource.MustParse(memoryInBytes)
		memoryValExists = true
	} else { //setting default value
		memoryQuantity = resource.MustParse("1")
	}

	if app.Spec.Driver.CoreLimit != nil {
		cpuQuantity := resource.MustParse(*app.Spec.Driver.CoreLimit)
		driverPodResourceRequirement.Limits = apiv1.ResourceList{
			Cpu:    cpuQuantity,
			Memory: memoryQuantity,
		}
	} else { //set memory Limit
		driverPodResourceRequirement.Limits = apiv1.ResourceList{
			Memory: memoryQuantity,
		}
	}
	//Cores OR Cores Request - https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/issues/581
	if app.Spec.Driver.CoreRequest != nil {
		cpuQuantity = resource.MustParse(*app.Spec.Driver.CoreRequest)
		cpuValExists = true
	} else if app.Spec.Driver.Cores != nil {
		cpuQuantity = resource.MustParse(fmt.Sprint(*app.Spec.Driver.Cores))
		cpuValExists = true
	} else {
		//Setting default value as cores or coreLimit is not passed
		cpuValExists = true
		cpuQuantity = resource.MustParse("1")
	}

	if cpuValExists && memoryValExists {
		driverPodResourceRequirement.Requests = apiv1.ResourceList{
			Memory: memoryQuantity,
			Cpu:    cpuQuantity,
		}

	} else if memoryValExists {
		driverPodResourceRequirement.Requests = apiv1.ResourceList{
			Memory: memoryQuantity,
		}
	} else if cpuValExists {
		driverPodResourceRequirement.Requests = apiv1.ResourceList{
			Cpu: cpuQuantity,
		}
	}

	return driverPodResourceRequirement
}
