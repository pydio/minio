/*
 * MinIO Cloud Storage, (C) 2015-2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bufio"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
)

const (
	minioReleaseTagTimeLayout = "2006-01-02T15-04-05Z"
	minioOSARCH               = runtime.GOOS + "-" + runtime.GOARCH
	minioReleaseURL           = "https://dl.min.io/server/minio/release/" + minioOSARCH + SlashSeparator

	envMinisignPubKey = "MINIO_UPDATE_MINISIGN_PUBKEY"
	updateTimeout     = 10 * time.Second
)

// minioVersionToReleaseTime - parses a standard official release
// MinIO version string.
//
// An official binary's version string is the release time formatted
// with RFC3339 (in UTC) - e.g. `2017-09-29T19:16:56Z`
func minioVersionToReleaseTime(version string) (releaseTime time.Time, err error) {
	return time.Parse(time.RFC3339, version)
}

// IsDocker - returns if the environment minio is running in docker or
// not. The check is a simple file existence check.
//
// https://github.com/moby/moby/blob/master/daemon/initlayer/setup_unix.go#L25
//
//	"/.dockerenv":      "file",
func IsDocker() bool {
	if env.Get("MINIO_CI_CD", "") == "" {
		_, err := os.Stat("/.dockerenv")
		if osIsNotExist(err) {
			return false
		}

		// Log error, as we will not propagate it to caller
		logger.LogIf(GlobalContext, err)

		return err == nil
	}
	return false
}

// IsDCOS returns true if minio is running in DCOS.
func IsDCOS() bool {
	if env.Get("MINIO_CI_CD", "") == "" {
		// http://mesos.apache.org/documentation/latest/docker-containerizer/
		// Mesos docker containerizer sets this value
		return env.Get("MESOS_CONTAINER_NAME", "") != ""
	}
	return false
}

// IsKubernetesReplicaSet returns true if minio is running in kubernetes replica set.
func IsKubernetesReplicaSet() bool {
	return IsKubernetes() && (env.Get("KUBERNETES_REPLICA_SET", "") != "")
}

// IsKubernetes returns true if minio is running in kubernetes.
func IsKubernetes() bool {
	if env.Get("MINIO_CI_CD", "") == "" {
		// Kubernetes env used to validate if we are
		// indeed running inside a kubernetes pod
		// is KUBERNETES_SERVICE_HOST
		// https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/kubelet_pods.go#L541
		return env.Get("KUBERNETES_SERVICE_HOST", "") != ""
	}
	return false
}

// IsBOSH returns true if minio is deployed from a bosh package
func IsBOSH() bool {
	// "/var/vcap/bosh" exists in BOSH deployed instance.
	_, err := os.Stat("/var/vcap/bosh")
	if osIsNotExist(err) {
		return false
	}

	// Log error, as we will not propagate it to caller
	logger.LogIf(GlobalContext, err)

	return err == nil
}

// MinIO Helm chart uses DownwardAPIFile to write pod label info to /podinfo/labels
// More info: https://kubernetes.io/docs/tasks/inject-data-application/downward-api-volume-expose-pod-information/#store-pod-fields
// Check if this is Helm package installation and report helm chart version
func getHelmVersion(helmInfoFilePath string) string {
	// Read the file exists.
	helmInfoFile, err := os.Open(helmInfoFilePath)
	if err != nil {
		// Log errors and return "" as MinIO can be deployed
		// without Helm charts as well.
		if !osIsNotExist(err) {
			reqInfo := (&logger.ReqInfo{}).AppendTags("helmInfoFilePath", helmInfoFilePath)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)
		}
		return ""
	}

	scanner := bufio.NewScanner(helmInfoFile)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "chart=") {
			helmChartVersion := strings.TrimPrefix(scanner.Text(), "chart=")
			// remove quotes from the chart version
			return strings.Trim(helmChartVersion, `"`)
		}
	}

	return ""
}

// IsSourceBuild - returns if this binary is a non-official build from
// source code.
func IsSourceBuild() bool {
	_, err := minioVersionToReleaseTime(Version)
	return err != nil
}

// IsPCFTile returns if server is running in PCF
func IsPCFTile() bool {
	return env.Get("MINIO_PCF_TILE_VERSION", "") != ""
}

// DO NOT CHANGE USER AGENT STYLE.
// The style should be
//
//	MinIO (<OS>; <ARCH>[; <MODE>][; dcos][; kubernetes][; docker][; source]) MinIO/<VERSION> MinIO/<RELEASE-TAG> MinIO/<COMMIT-ID> [MinIO/universe-<PACKAGE-NAME>] [MinIO/helm-<HELM-VERSION>]
//
// Any change here should be discussed by opening an issue at
// https://github.com/minio/minio/issues.
func getUserAgent(mode string) string {

	userAgentParts := []string{}
	// Helper function to concisely append a pair of strings to a
	// the user-agent slice.
	uaAppend := func(p, q string) {
		userAgentParts = append(userAgentParts, p, q)
	}

	uaAppend("MinIO (", runtime.GOOS)
	uaAppend("; ", runtime.GOARCH)
	if mode != "" {
		uaAppend("; ", mode)
	}
	if IsDCOS() {
		uaAppend("; ", "dcos")
	}
	if IsKubernetes() {
		uaAppend("; ", "kubernetes")
	}
	if IsDocker() {
		uaAppend("; ", "docker")
	}
	if IsBOSH() {
		uaAppend("; ", "bosh")
	}
	if IsSourceBuild() {
		uaAppend("; ", "source")
	}

	uaAppend(") MinIO/", Version)
	uaAppend(" MinIO/", ReleaseTag)
	uaAppend(" MinIO/", CommitID)
	if IsDCOS() {
		universePkgVersion := env.Get("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION", "")
		// On DC/OS environment try to the get universe package version.
		if universePkgVersion != "" {
			uaAppend(" MinIO/universe-", universePkgVersion)
		}
	}

	if IsKubernetes() {
		// In Kubernetes environment, try to fetch the helm package version
		helmChartVersion := getHelmVersion("/podinfo/labels")
		if helmChartVersion != "" {
			uaAppend(" MinIO/helm-", helmChartVersion)
		}
		// In Kubernetes environment, try to fetch the Operator, VSPHERE plugin version
		opVersion := env.Get("MINIO_OPERATOR_VERSION", "")
		if opVersion != "" {
			uaAppend(" MinIO/operator-", opVersion)
		}
		vsphereVersion := env.Get("MINIO_VSPHERE_PLUGIN_VERSION", "")
		if vsphereVersion != "" {
			uaAppend(" MinIO/vsphere-plugin-", vsphereVersion)
		}
	}

	if IsPCFTile() {
		pcfTileVersion := env.Get("MINIO_PCF_TILE_VERSION", "")
		if pcfTileVersion != "" {
			uaAppend(" MinIO/pcf-tile-", pcfTileVersion)
		}
	}

	return strings.Join(userAgentParts, "")
}
