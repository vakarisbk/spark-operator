package alternatesubmit

import (
	"bytes"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/google/uuid"
	apiv1 "k8s.io/api/core/v1"
	"strconv"
	"strings"
)

func handleLocalDirsFeatureStep(app *v1beta2.SparkApplication, resolvedLocalDirs []string, driverPodVolumes *[]apiv1.Volume, volumeMounts *[]apiv1.VolumeMount, envVariables *[]apiv1.EnvVar, appSpecVolumeMounts []apiv1.VolumeMount, appSpecVolumes []apiv1.Volume) error {
	//Value of this variable is set to SPARK_LOCAL_DIRS environment variable
	var localDirsList bytes.Buffer
	sparkConfKeyValuePairs := app.Spec.SparkConf
	localDirIndex := 1
	//unique mountPaths storing variable
	var mountedPaths []string
	//Flag indicating whether to create in-memory volume or not
	localDirTmpFSFlag, localDirTmpFSFlagExists := sparkConfKeyValuePairs["spark.kubernetes.local.dirs.tmpfs"]

	if len(appSpecVolumeMounts) > 0 {
		length := len(appSpecVolumeMounts)
		for index, driverContainerVolumeMount := range appSpecVolumeMounts {
			if strings.Contains(driverContainerVolumeMount.Name, LocalStoragePrefix) {
				//Mount the volume
				mountLocalDir(localDirTmpFSFlagExists, localDirTmpFSFlag, driverPodVolumes, volumeMounts, driverContainerVolumeMount)
				//concatenate to SPARK_LOCAL_DIRS list and increment the index
				localDirsList.WriteString(driverContainerVolumeMount.MountPath)
				if index < (length - 1) {
					localDirsList.WriteString(",")
				}
				localDirIndex++
				mountedPaths = append(mountedPaths, driverContainerVolumeMount.MountPath)
			}
		}

	}
	//Only if there are no local directory volume mounts
	if localDirIndex == 1 {
		if len(resolvedLocalDirs) > 0 {
			//Logic is to check if local directories specified in SPARK_LOCAL_DIRS or spark.local.dir of SparkConf section
			localDirsList, localDirIndex = handleLocalDirsMounting(resolvedLocalDirs, localDirIndex, localDirTmpFSFlagExists, localDirTmpFSFlag, driverPodVolumes, volumeMounts, localDirsList, mountedPaths)
		} else {
			if sparkConfKeyValuePairs != nil {
				sparkLocalDirectoryList, sparkLocalDirectoryListExists := sparkConfKeyValuePairs["spark.local.dir"]
				if sparkLocalDirectoryListExists {
					localDirs := strings.Split(sparkLocalDirectoryList, ",")
					if len(localDirs) > 0 {
						localDirsList, localDirIndex = handleLocalDirsMounting(localDirs, localDirIndex, localDirTmpFSFlagExists, localDirTmpFSFlag, driverPodVolumes, volumeMounts, localDirsList, mountedPaths)
					}
				}
			}

		}
	}
	//No local directory exists, creating default local directory
	if localDirIndex == 1 {
		//addDriverContainerEnvVariable(sparkLocalDirName string, envVariables *[]apiv1.EnvVar)
		sparkLocalDirName := SparkLocalDirPath + uuid.New().String()
		addDriverContainerEnvVariable(sparkLocalDirName, envVariables)
		// Set default temp directory, as there are no local directory specified
		sparkLocalDirVolume := apiv1.Volume{
			Name: SparkLocalDirectoryName + strconv.Itoa(1),
			VolumeSource: apiv1.VolumeSource{
				EmptyDir: &apiv1.EmptyDirVolumeSource{
					Medium: "Memory",
				},
			},
		}
		*driverPodVolumes = append(*driverPodVolumes, sparkLocalDirVolume)
		volumeMount := apiv1.VolumeMount{
			Name:      SparkLocalDirectoryName + strconv.Itoa(1),
			MountPath: sparkLocalDirName,
		}
		*volumeMounts = append(*volumeMounts, volumeMount)

	} else {
		//there is at least one local volume mount in App Spec
		addDriverContainerEnvVariable(localDirsList.String(), envVariables)
	}
	return nil
}

func mountLocalDir(localDirTmpFSFlagExists bool, localDirTmpFSFlag string, driverPodVolumes *[]apiv1.Volume, volumeMounts *[]apiv1.VolumeMount, driverContainerVolumeMount apiv1.VolumeMount) {
	//Check if "spark.kubernetes.local.dirs.tmpfs" is set
	if localDirTmpFSFlagExists && localDirTmpFSFlag == "true" {
		sparkLocalDirVolume := apiv1.Volume{
			Name: driverContainerVolumeMount.Name,
			VolumeSource: apiv1.VolumeSource{
				EmptyDir: &apiv1.EmptyDirVolumeSource{
					Medium: "Memory",
				},
			},
		}
		*driverPodVolumes = append(*driverPodVolumes, sparkLocalDirVolume)
	} else {
		sparkLocalDirVolume := apiv1.Volume{
			Name: driverContainerVolumeMount.Name,
			VolumeSource: apiv1.VolumeSource{
				EmptyDir: &apiv1.EmptyDirVolumeSource{},
			},
		}
		*driverPodVolumes = append(*driverPodVolumes, sparkLocalDirVolume)
	}
	//Volume mount for the local temp folder
	volumeMount := apiv1.VolumeMount{
		Name:      driverContainerVolumeMount.Name,
		MountPath: driverContainerVolumeMount.MountPath,
	}
	*volumeMounts = append(*volumeMounts, volumeMount)
}

func handleLocalDirsMounting(localDirs []string, localDirIndex int, localDirTmpFSFlagExists bool, localDirTmpFSFlag string, driverPodVolumes *[]apiv1.Volume, volumeMounts *[]apiv1.VolumeMount, localDirsList bytes.Buffer, mountedPaths []string) (bytes.Buffer, int) {
	for _, localDirectoryPath := range localDirs {
		if sliceContainsString(mountedPaths, localDirectoryPath) {
			continue
		}
		//Mount the local directory to Driver container
		var driverContainerVolumeMount apiv1.VolumeMount
		driverContainerVolumeMount.Name = SparkLocalDirectoryName + strconv.Itoa(localDirIndex)
		driverContainerVolumeMount.MountPath = localDirectoryPath
		mountLocalDir(localDirTmpFSFlagExists, localDirTmpFSFlag, driverPodVolumes, volumeMounts, driverContainerVolumeMount)
		//concatenate to SPARK_LOCAL_DIRS list and increment the index
		localDirsList.WriteString(localDirectoryPath)
		localDirsList.WriteString(",")
		localDirIndex++
	}
	return localDirsList, localDirIndex
}
func sliceContainsString(mountedPaths []string, localDirectoryPath string) bool {
	for _, mountedPath := range mountedPaths {
		if mountedPath == localDirectoryPath {
			return true
		}
	}
	return false
}
func addDriverContainerEnvVariable(sparkLocalDirName string, envVariables *[]apiv1.EnvVar) {
	var sparkLocalDir apiv1.EnvVar
	sparkLocalDir.Name = SparkLocalDir
	sparkLocalDir.Value = sparkLocalDirName
	*envVariables = append(*envVariables, sparkLocalDir)
}
