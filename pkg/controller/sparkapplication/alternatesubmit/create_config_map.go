package alternatesubmit

import (
	"fmt"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/magiconair/properties"
	"k8s.io/client-go/kubernetes"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Function to create Spark Application Configmap
// Spark Application ConfigMap is pre-requisite for Driver Pod Creation; this configmap is mounted on driver pod
// Spark Application ConfigMap acts as configuration repository for the Driver, executor pods
func createSparkAppConfigMap(app *v1beta2.SparkApplication, submissionID string, createdApplicationId string, kubeClient kubernetes.Interface, driverConfigMapName string) error {
	var errorSubmissionCommandArgs error

	//ConfigMap is created with Key, Value Pairs
	driverConfigMapData := make(map[string]string)
	//Followed the convention of Scala Implementation for this attribute, constant value is assigned
	driverConfigMapData[SparkEnvScriptFileName] = SparkEnvScriptFileCommand
	//Spark Application namespace
	driverConfigMapData[config.SparkAppNamespaceKey] = app.GetNamespace()

	// Utility function buildAltSubmissionCommandArgs to add other key, value configuration pairs
	driverConfigMapData[SparkPropertiesFileName], errorSubmissionCommandArgs = buildAltSubmissionCommandArgs(app, getDriverPodName(app), submissionID, createdApplicationId)
	if errorSubmissionCommandArgs != nil {
		return fmt.Errorf("failed to create submission command args for the driver configmap %s in namespace %s: %v", driverConfigMapName, app.Namespace, errorSubmissionCommandArgs)
	}
	//Create Spark Application ConfigMap
	createErr := createConfigMapUtil(driverConfigMapName, app, driverConfigMapData, kubeClient)
	if createErr != nil {
		return fmt.Errorf("failed to create/update driver configmap %s in namespace %s: %v", driverConfigMapName, app.Namespace, createErr)
	}
	return nil
}

// Helper func to create key/value pairs required for the Spark Application Configmap
// Majority of the code borrowed from Scala implementation
func buildAltSubmissionCommandArgs(app *v1beta2.SparkApplication, driverPodName string, submissionID string, createdApplicationId string) (string, error) {
	var args string
	masterURL, err := getMasterURL()
	if err != nil {
		return args, err
	}
	masterURL = AddEscapeCharacter(masterURL)
	//Construct Service Name
	serviceName := driverPodName + HyphenSeparator + ServiceShortForm +
		DotSeparator + app.Namespace + DotSeparator + ServiceShortForm

	args = args + fmt.Sprintf("%s=%s", SparkDriverHost, serviceName) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkAppId, createdApplicationId) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkMaster, masterURL) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkSubmitDeploymentMode, string(app.Spec.Mode)) + NewLineString

	args = args + fmt.Sprintf("%s=%s", config.SparkAppNamespaceKey, app.Namespace) + NewLineString

	args = args + fmt.Sprintf("%s=%s", config.SparkAppNameKey, app.Name) + NewLineString

	args = args + fmt.Sprintf("%s=%s", config.SparkDriverPodNameKey, driverPodName) + NewLineString

	if app.Spec.Deps.Jars != nil && len(app.Spec.Deps.Jars) > 0 {
		modifiedJarList := make([]string, len(app.Spec.Deps.Jars))
		for _, sparkjar := range app.Spec.Deps.Jars {
			sparkjar = AddEscapeCharacter(sparkjar)
			modifiedJarList = append(modifiedJarList, sparkjar)
		}
		args = args + SparkJars + EqualsSign + strings.Join(modifiedJarList, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.Files != nil && len(app.Spec.Deps.Files) > 0 {
		args = args + SparkFiles + EqualsSign + strings.Join(app.Spec.Deps.Files, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.PyFiles != nil && len(app.Spec.Deps.PyFiles) > 0 {
		args = args + SparkPyFiles + EqualsSign + strings.Join(app.Spec.Deps.PyFiles, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.Packages != nil && len(app.Spec.Deps.Packages) > 0 {
		args = args + SparkPackages + EqualsSign + strings.Join(app.Spec.Deps.Packages, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.ExcludePackages != nil && len(app.Spec.Deps.ExcludePackages) > 0 {
		args = args + SparkExcludePackages + EqualsSign + strings.Join(app.Spec.Deps.ExcludePackages, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.Repositories != nil && len(app.Spec.Deps.Repositories) > 0 {
		args = args + SparkRepositories + EqualsSign + strings.Join(app.Spec.Deps.Repositories, CommaSeparator) + NewLineString
	}

	if app.Spec.Image != nil {
		sparkContainerImage := AddEscapeCharacter(*app.Spec.Image)
		args = args + fmt.Sprintf("%s=%s", config.SparkContainerImageKey, sparkContainerImage) + NewLineString
	}
	if app.Spec.ImagePullPolicy != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkContainerImagePullPolicyKey, *app.Spec.ImagePullPolicy) + NewLineString
	}
	if len(app.Spec.ImagePullSecrets) > 0 {
		secretNames := strings.Join(app.Spec.ImagePullSecrets, CommaSeparator)
		args = args + fmt.Sprintf("%s=%s", config.SparkImagePullSecretKey, secretNames) + NewLineString
	}
	if app.Spec.PythonVersion != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkPythonVersion, *app.Spec.PythonVersion) + NewLineString
	}
	if app.Spec.MemoryOverheadFactor != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkMemoryOverheadFactor, *app.Spec.MemoryOverheadFactor) + NewLineString
	} else {
		args = args + fmt.Sprintf("%s=%s", config.SparkMemoryOverheadFactor, "0.1") + NewLineString
	}

	// Operator triggered spark-submit should never wait for App completion
	args = args + fmt.Sprintf("%s=false", config.SparkWaitAppCompletion) + NewLineString

	// Add Spark configuration properties.
	for key, value := range app.Spec.SparkConf {
		// Configuration property for the driver pod name has already been set.
		if key != config.SparkDriverPodNameKey {
			//Adding escape character for the spark.executor.extraClassPath and spark.driver.extraClassPath
			if key == SparkDriverExtraClassPath || key == SparkExecutorExtraClassPath {
				value = AddEscapeCharacter(value)
			}
			args = args + fmt.Sprintf("%s=%s", key, value) + NewLineString
		}
	}

	// Add Hadoop configuration properties.
	for key, value := range app.Spec.HadoopConf {
		args = args + fmt.Sprintf("spark.hadoop.%s=%s", key, value) + NewLineString
	}
	if app.Spec.HadoopConf != nil || app.Spec.HadoopConfigMap != nil {
		// Adding Environment variable
		args = args + fmt.Sprintf("spark.hadoop.%s=%s", HadoopConfDir, HadoopConfDirPath) + NewLineString
	}

	// Add the driver and executor configuration options.
	// Note that when the controller submits the application, it expects that all dependencies are local
	// so init-container is not needed and therefore no init-container image needs to be specified.
	args = args + fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.SparkAppNameLabel, app.Name) + NewLineString
	//driverConfOptions = append(driverConfOptions,
	args = args + fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.LaunchedBySparkOperatorLabel, "true") + NewLineString

	args = args + fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.SubmissionIDLabel, submissionID) + NewLineString

	if app.Spec.Driver.Image != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkDriverContainerImageKey, *app.Spec.Driver.Image) + NewLineString
	}

	if app.Spec.Driver.Cores != nil {
		args = args + fmt.Sprintf("%s=%d", SparkDriverCores, *app.Spec.Driver.Cores) + NewLineString
	} else { // Driver default cores - https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L561
		args = args + fmt.Sprintf("%s=%s", SparkDriverCores, DriverDefaultCores) + NewLineString
	}
	if app.Spec.Driver.CoreRequest != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkDriverCoreRequestKey, *app.Spec.Driver.CoreRequest) + NewLineString
	}
	if app.Spec.Driver.CoreLimit != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkDriverCoreLimitKey, *app.Spec.Driver.CoreLimit) + NewLineString
	}
	var memory string

	if app.Spec.Driver.Memory != nil {
		memory = *app.Spec.Driver.Memory
		args = args + fmt.Sprintf("spark.driver.memory=%s", *app.Spec.Driver.Memory) + NewLineString
	} else { //Driver default memory - https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L537
		args = args + fmt.Sprintf("spark.driver.memory=%s", DriverDefaultMemory) + NewLineString
		memory = DriverDefaultMemory
	}

	memoryValue := memory[0 : len(memory)-1]
	memoryIntValue, _ := strconv.Atoi(memoryValue)
	if app.Spec.Driver.MemoryOverhead != nil {
		memoryOverheadFactor, _ := strconv.ParseFloat(*app.Spec.Driver.MemoryOverhead, 64)
		memoryOverheadCalculated := float64(memoryIntValue) * memoryOverheadFactor
		memoryOverheadCalculatedWithUnit := fmt.Sprintf("%v%s", int(memoryOverheadCalculated), memory[len(memory)-1:len(memory)])
		args = args + fmt.Sprintf("spark.driver.memoryOverhead=%v", memoryOverheadCalculatedWithUnit) + NewLineString
	} else {
		memoryOverheadFactor, _ := strconv.ParseFloat(DriverMemoryOverheadDefault, 64)
		memoryOverheadCalculated := float64(memoryIntValue) * memoryOverheadFactor
		memoryOverheadCalculatedWithUnit := fmt.Sprintf("%v%s", int(memoryOverheadCalculated), memory[len(memory)-1:len(memory)])
		args = args + fmt.Sprintf("spark.driver.memoryOverhead=%v", memoryOverheadCalculatedWithUnit) + NewLineString
	}

	if app.Spec.Driver.ServiceAccount != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkDriverServiceAccountName, *app.Spec.Driver.ServiceAccount) + NewLineString
	}

	if app.Spec.Driver.JavaOptions != nil {
		driverJavaOptionsList := AddEscapeCharacter(*app.Spec.Driver.JavaOptions)
		args = args + fmt.Sprintf("%s=%s", config.SparkDriverJavaOptions, driverJavaOptionsList) + NewLineString
	}

	if app.Spec.Driver.KubernetesMaster != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkDriverKubernetesMaster, *app.Spec.Driver.KubernetesMaster) + NewLineString
	}

	//Populate SparkApplication Labels to Driver
	driverLabels := make(map[string]string)
	for key, value := range app.Labels {
		driverLabels[key] = value
	}
	for key, value := range app.Spec.Driver.Labels {
		driverLabels[key] = value
	}

	for key, value := range driverLabels {
		args = args + fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, key, value) + NewLineString
	}
	for key, value := range app.Spec.Driver.Annotations {
		if key == OpencensusPrometheusTarget {
			value = strings.Replace(value, "\n", "", -1)
			value = AddEscapeCharacter(value)
		}
		args = args + fmt.Sprintf("%s%s=%s", config.SparkDriverAnnotationKeyPrefix, key, value) + NewLineString
	}

	for key, value := range app.Spec.Driver.EnvSecretKeyRefs {
		args = args + fmt.Sprintf("%s%s=%s:%s", config.SparkDriverSecretKeyRefKeyPrefix, key, value.Name, value.Key) + NewLineString
	}

	for key, value := range app.Spec.Driver.ServiceAnnotations {
		args = args + fmt.Sprintf("%s%s=%s", config.SparkDriverServiceAnnotationKeyPrefix, key, value) + NewLineString
	}

	//driverConfOptions = append(driverConfOptions, GetDriverSecretConfOptions(app)...)
	for _, s := range app.Spec.Driver.Secrets {
		args = args + fmt.Sprintf("%s%s=%s", config.SparkDriverSecretKeyPrefix, s.Name, s.Path) + NewLineString
		//secretConfOptions = append(secretConfOptions, conf)
		if s.Type == v1beta2.GCPServiceAccountSecret {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				config.SparkDriverEnvVarConfigKeyPrefix,
				config.GoogleApplicationCredentialsEnvVar,
				filepath.Join(s.Path, config.ServiceAccountJSONKeyFileName)) + NewLineString

		} else if s.Type == v1beta2.HadoopDelegationTokenSecret {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				config.SparkDriverEnvVarConfigKeyPrefix,
				config.HadoopTokenFileLocationEnvVar,
				filepath.Join(s.Path, config.HadoopDelegationTokenFileName)) + NewLineString

		}
	}

	for key, value := range app.Spec.Driver.EnvVars {
		args = args + fmt.Sprintf("%s%s=%s", config.SparkDriverEnvVarConfigKeyPrefix, key, value) + NewLineString

	}

	for key, value := range app.Spec.Driver.Env {
		args = args + fmt.Sprintf("%s%d=%s", config.SparkDriverEnvVarConfigKeyPrefix, key, value) + NewLineString
	}

	args = args + fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.SparkAppNameLabel, app.Name) + NewLineString

	args = args + fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.LaunchedBySparkOperatorLabel, "true") + NewLineString

	args = args + fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.SubmissionIDLabel, submissionID) + NewLineString

	if app.Spec.Executor.Instances != nil {
		args = args + fmt.Sprintf("spark.executor.instances=%d", *app.Spec.Executor.Instances) + NewLineString
	}

	if app.Spec.Executor.Image != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkExecutorContainerImageKey, *app.Spec.Executor.Image) + NewLineString
	}

	if app.Spec.Executor.Cores != nil {
		// Property "spark.executor.cores" does not allow float values.
		args = args + fmt.Sprintf("spark.executor.cores=%d", int32(*app.Spec.Executor.Cores)) + NewLineString
	}
	if app.Spec.Executor.CoreRequest != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkExecutorCoreRequestKey, *app.Spec.Executor.CoreRequest) + NewLineString
	}
	if app.Spec.Executor.CoreLimit != nil {

		args = args + fmt.Sprintf("%s=%s", config.SparkExecutorCoreLimitKey, *app.Spec.Executor.CoreLimit) + NewLineString
	}
	if app.Spec.Executor.Memory != nil {
		args = args + fmt.Sprintf("spark.executor.memory=%s", *app.Spec.Executor.Memory) + NewLineString
		memory = *app.Spec.Executor.Memory
	} else { //Setting default 1g
		//Executor default memory - https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L544
		args = args + fmt.Sprintf("spark.executor.memory=%s", ExecutorDefaultMemory) + NewLineString
		memory = ExecutorDefaultMemory
	}
	memoryValue = memory[0 : len(memory)-1]
	memoryIntValue, _ = strconv.Atoi(memoryValue)

	if app.Spec.Executor.MemoryOverhead != nil {
		memoryOverheadFactor, _ := strconv.ParseFloat(*app.Spec.Executor.MemoryOverhead, 64)
		memoryOverheadCalculated := float64(memoryIntValue) * memoryOverheadFactor
		memoryOverheadCalculatedWithUnit := fmt.Sprintf("%v%s", int(memoryOverheadCalculated), memory[len(memory)-1:len(memory)])
		args = args + fmt.Sprintf("spark.executor.memoryOverhead=%s", memoryOverheadCalculatedWithUnit) + NewLineString
	} else {
		//Logic of calculating default value executor memory overhead is
		// For JVM-based jobs this value will default to 0.10 and 0.40 for non-JVM jobs as per https://spark.apache.org/docs/latest/running-on-kubernetes.html
		var memoryOveheadFactor float64
		if app.Spec.Type == v1beta2.JavaApplicationType || app.Spec.Type == v1beta2.ScalaApplicationType {
			//args = args + fmt.Sprintf("spark.executor.memoryOverhead=%s", "0.10") + NewLineString
			memoryOveheadFactor = 0.10
		} else {

			memoryOveheadFactor = 0.40
		}
		memoryOverheadCalculated := float64(memoryIntValue) * memoryOveheadFactor
		memoryOverheadCalculatedWithUnit := fmt.Sprintf("%v%s", int(memoryOverheadCalculated), memory[len(memory)-1:len(memory)])
		args = args + fmt.Sprintf("spark.executor.memoryOverhead=%s", memoryOverheadCalculatedWithUnit) + NewLineString
	}

	if app.Spec.Executor.ServiceAccount != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkExecutorAccountName, *app.Spec.Executor.ServiceAccount) + NewLineString
	}

	if app.Spec.Executor.DeleteOnTermination != nil {

		args = args + fmt.Sprintf("%s=%t", config.SparkExecutorDeleteOnTermination, *app.Spec.Executor.DeleteOnTermination) + NewLineString
	}

	//Populate SparkApplication Labels to Executors
	executorLabels := make(map[string]string)
	for key, value := range app.Labels {
		executorLabels[key] = value
	}
	for key, value := range app.Spec.Executor.Labels {
		executorLabels[key] = value
	}
	for key, value := range executorLabels {
		args = args + fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, key, value) + NewLineString
	}
	for key, value := range app.Spec.Executor.Annotations {
		if key == OpencensusPrometheusTarget {
			value = strings.Replace(value, "\n", "", -1)
			value = AddEscapeCharacter(value)
		}

		args = args + fmt.Sprintf("%s%s=%s", config.SparkExecutorAnnotationKeyPrefix, key, value) + NewLineString
	}

	for key, value := range app.Spec.Executor.EnvSecretKeyRefs {
		args = args + fmt.Sprintf("%s%s=%s:%s", config.SparkExecutorSecretKeyRefKeyPrefix, key, value.Name, value.Key) + NewLineString
	}

	if app.Spec.Executor.JavaOptions != nil {
		args = args + fmt.Sprintf("%s=%s", config.SparkExecutorJavaOptions, *app.Spec.Executor.JavaOptions) + NewLineString
	}

	//executorConfOptions = append(executorConfOptions, GetExecutorSecretConfOptions(app)...)
	for _, s := range app.Spec.Executor.Secrets {
		args = args + fmt.Sprintf("%s%s=%s", config.SparkExecutorSecretKeyPrefix, s.Name, s.Path) + NewLineString

		if s.Type == v1beta2.GCPServiceAccountSecret {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				config.SparkExecutorEnvVarConfigKeyPrefix,
				config.GoogleApplicationCredentialsEnvVar,
				filepath.Join(s.Path, config.ServiceAccountJSONKeyFileName)) + NewLineString

		} else if s.Type == v1beta2.HadoopDelegationTokenSecret {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				config.SparkExecutorEnvVarConfigKeyPrefix,
				config.HadoopTokenFileLocationEnvVar,
				filepath.Join(s.Path, config.HadoopDelegationTokenFileName)) + NewLineString

		}
	}
	//executorConfOptions = append(executorConfOptions, GetExecutorEnvVarConfOptions(app)...)
	for key, value := range app.Spec.Executor.EnvVars {
		args = args + fmt.Sprintf("%s%s=%s", config.SparkExecutorEnvVarConfigKeyPrefix, key, value) + NewLineString
	}
	if app.Spec.DynamicAllocation != nil {

		args = args + fmt.Sprintf("%s=true", config.SparkDynamicAllocationEnabled) + NewLineString
		// Turn on shuffle tracking if dynamic allocation is enabled.
		args = args + fmt.Sprintf("%s=true", config.SparkDynamicAllocationShuffleTrackingEnabled) + NewLineString
		dynamicAllocation := app.Spec.DynamicAllocation
		if dynamicAllocation.InitialExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", config.SparkDynamicAllocationInitialExecutors, *dynamicAllocation.InitialExecutors) + NewLineString
		}
		if dynamicAllocation.MinExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", config.SparkDynamicAllocationMinExecutors, *dynamicAllocation.MinExecutors) + NewLineString
		}
		if dynamicAllocation.MaxExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", config.SparkDynamicAllocationMaxExecutors, *dynamicAllocation.MaxExecutors) + NewLineString
		}
		if dynamicAllocation.ShuffleTrackingTimeout != nil {
			args = args + fmt.Sprintf("%s=%d", config.SparkDynamicAllocationShuffleTrackingTimeout, *dynamicAllocation.ShuffleTrackingTimeout) + NewLineString
		}
	}

	for key, value := range app.Spec.NodeSelector {
		args = args + fmt.Sprintf("%s%s=%s", config.SparkNodeSelectorKeyPrefix, key, value) + NewLineString

	}

	args = args + fmt.Sprintf("%s=%s", SubmitInDriver, True) + NewLineString

	//
	args = args + fmt.Sprintf("%s=%v", SparkDriverBlockManagerPort, DefaultBlockManagerPort) + NewLineString
	//
	args = args + fmt.Sprintf("%s=%v", SparkDriverPort, DefaultDriverPort) + NewLineString
	//
	appSpecType := app.Spec.Type
	if appSpecType == SparkAppTypeScala || appSpecType == SparkAppTypeJavaCamelCase {
		appSpecType = SparkAppTypeJava
	} else if appSpecType == SparkAppTypePythonWithP {
		appSpecType = SparkAppTypePython
	} else if appSpecType == SparkAppTypeRWithR {
		appSpecType = SparkAppTypeR
	}
	args = args + fmt.Sprintf("%s=%s", SparkApplicationType, appSpecType) + NewLineString

	args = args + fmt.Sprintf("%s=%v", SparkApplicationSubmitTime, time.Now().UnixMilli()) + NewLineString

	sparkUIProxyBase := ForwardSlash + GetAppNamespace(app) + HyphenSeparator + app.Name + SparkUIProxyBaseRegex

	args = args + fmt.Sprintf("%s=%s", SparkUIProxyBase, sparkUIProxyBase) + NewLineString

	//spark.ui.proxyRedirectUri
	args = args + fmt.Sprintf("%s=%s", SparkUIProxyRedirectURI, ForwardSlash) + NewLineString

	//Load spark-default.conf properties, if exist /opt/spark/conf/
	sparkDefaultConfFilePath := SparkDefaultsConfigFilePath + DefaultSparkConfFileName
	propertyPairs, propertyFileReadError := properties.LoadFile(sparkDefaultConfFilePath, properties.UTF8)
	if propertyFileReadError == nil {
		fmt.Println(propertyPairs)
		keysList := propertyPairs.Keys()
		for _, key := range keysList {
			value, _ := propertyPairs.Get(key)
			fmt.Println(key, value)
			args = args + fmt.Sprintf("%s=%s", key, value) + NewLineString
		}
	}

	//Monitoring Section
	if app.Spec.Monitoring != nil {

		SparkMetricsNamespace := GetAppNamespace(app) + DotSeparator + app.Name
		args = args + fmt.Sprintf("%s=%s", SparkMetricsNamespaceKey, SparkMetricsNamespace) + NewLineString

		// Spark Metric Properties file
		if app.Spec.Monitoring.MetricsPropertiesFile != nil {
			args = args + fmt.Sprintf("%s=%s", SparkMetricConfKey, *app.Spec.Monitoring.MetricsPropertiesFile) + NewLineString
		}

	}

	// Volumes
	if app.Spec.Volumes != nil {
		options, err := addLocalDirConfOptions(app)
		if err != nil {
			return "Error occcurred while building configmap", err
		}
		for _, option := range options {
			args = args + option + NewLineString
		}
	}

	if app.Spec.MainApplicationFile != nil {
		// Add the main application file if it is present.
		sparkAppJar := AddEscapeCharacter(*app.Spec.MainApplicationFile)
		args = args + fmt.Sprintf("%s=%s", SparkJars, sparkAppJar) + NewLineString
	}

	// Add application arguments.
	for _, argument := range app.Spec.Arguments {
		args = args + argument + NewLineString
	}

	return args, nil
}
