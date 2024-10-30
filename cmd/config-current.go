/*
 * MinIO Cloud Storage, (C) 2016-2019 MinIO, Inc.
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
	"context"
	"fmt"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/api"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/config/compress"
	"github.com/minio/minio/cmd/config/etcd"
	"github.com/minio/minio/cmd/config/heal"
	xldap "github.com/minio/minio/cmd/config/identity/ldap"
	"github.com/minio/minio/cmd/config/identity/openid"
	"github.com/minio/minio/cmd/config/notify"
	"github.com/minio/minio/cmd/config/policy/opa"
	"github.com/minio/minio/cmd/config/scanner"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
)

func initHelp() {
	var kvs = map[string]config.KVS{
		config.EtcdSubSys:           etcd.DefaultKVS,
		config.CacheSubSys:          cache.DefaultKVS,
		config.CompressionSubSys:    compress.DefaultKVS,
		config.IdentityLDAPSubSys:   xldap.DefaultKVS,
		config.IdentityOpenIDSubSys: openid.DefaultKVS,
		config.PolicyOPASubSys:      opa.DefaultKVS,
		config.RegionSubSys:         config.DefaultRegionKVS,
		config.APISubSys:            api.DefaultKVS,
		config.CredentialsSubSys:    config.DefaultCredentialKVS,
		config.KmsVaultSubSys:       crypto.DefaultVaultKVS,
		config.KmsKesSubSys:         crypto.DefaultKesKVS,
		config.LoggerWebhookSubSys:  logger.DefaultKVS,
		config.AuditWebhookSubSys:   logger.DefaultAuditKVS,
		config.HealSubSys:           heal.DefaultKVS,
		config.ScannerSubSys:        scanner.DefaultKVS,
	}
	for k, v := range notify.DefaultNotificationKVS {
		kvs[k] = v
	}
	config.RegisterDefaultKVS(kvs)

	// Captures help for each sub-system
	var helpSubSys = config.HelpKVS{
		config.HelpKV{
			Key:         config.RegionSubSys,
			Description: "label the location of the server",
		},
		config.HelpKV{
			Key:         config.CacheSubSys,
			Description: "add caching storage tier",
		},
		config.HelpKV{
			Key:         config.CompressionSubSys,
			Description: "enable server side compression of objects",
		},
		config.HelpKV{
			Key:         config.EtcdSubSys,
			Description: "federate multiple clusters for IAM and Bucket DNS",
		},
		config.HelpKV{
			Key:         config.IdentityOpenIDSubSys,
			Description: "enable OpenID SSO support",
		},
		config.HelpKV{
			Key:         config.IdentityLDAPSubSys,
			Description: "enable LDAP SSO support",
		},
		config.HelpKV{
			Key:         config.PolicyOPASubSys,
			Description: "[DEPRECATED] enable external OPA for policy enforcement",
		},
		config.HelpKV{
			Key:         config.KmsVaultSubSys,
			Description: "enable external HashiCorp Vault key management service",
		},
		config.HelpKV{
			Key:         config.KmsKesSubSys,
			Description: "enable external MinIO key encryption service",
		},
		config.HelpKV{
			Key:         config.APISubSys,
			Description: "manage global HTTP API call specific features, such as throttling, authentication types, etc.",
		},
		config.HelpKV{
			Key:         config.HealSubSys,
			Description: "manage object healing frequency and bitrot verification checks",
		},
		config.HelpKV{
			Key:         config.ScannerSubSys,
			Description: "manage namespace scanning for usage calculation, lifecycle, healing and more",
		},
		config.HelpKV{
			Key:             config.LoggerWebhookSubSys,
			Description:     "send server logs to webhook endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.AuditWebhookSubSys,
			Description:     "send audit logs to webhook endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyWebhookSubSys,
			Description:     "publish bucket notifications to webhook endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyAMQPSubSys,
			Description:     "publish bucket notifications to AMQP endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyKafkaSubSys,
			Description:     "publish bucket notifications to Kafka endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyMQTTSubSys,
			Description:     "publish bucket notifications to MQTT endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyNATSSubSys,
			Description:     "publish bucket notifications to NATS endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyNSQSubSys,
			Description:     "publish bucket notifications to NSQ endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyMySQLSubSys,
			Description:     "publish bucket notifications to MySQL databases",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyPostgresSubSys,
			Description:     "publish bucket notifications to Postgres databases",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyESSubSys,
			Description:     "publish bucket notifications to Elasticsearch endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyRedisSubSys,
			Description:     "publish bucket notifications to Redis datastores",
			MultipleTargets: true,
		},
	}

	var helpMap = map[string]config.HelpKVS{
		"":                          helpSubSys, // Help for all sub-systems.
		config.RegionSubSys:         config.RegionHelp,
		config.APISubSys:            api.Help,
		config.StorageClassSubSys:   storageclass.Help,
		config.EtcdSubSys:           etcd.Help,
		config.CacheSubSys:          cache.Help,
		config.CompressionSubSys:    compress.Help,
		config.HealSubSys:           heal.Help,
		config.ScannerSubSys:        scanner.Help,
		config.IdentityOpenIDSubSys: openid.Help,
		config.IdentityLDAPSubSys:   xldap.Help,
		config.PolicyOPASubSys:      opa.Help,
		config.KmsVaultSubSys:       crypto.HelpVault,
		config.KmsKesSubSys:         crypto.HelpKes,
		config.LoggerWebhookSubSys:  logger.Help,
		config.AuditWebhookSubSys:   logger.HelpAudit,
		config.NotifyAMQPSubSys:     notify.HelpAMQP,
		config.NotifyKafkaSubSys:    notify.HelpKafka,
		config.NotifyMQTTSubSys:     notify.HelpMQTT,
		config.NotifyNATSSubSys:     notify.HelpNATS,
		config.NotifyNSQSubSys:      notify.HelpNSQ,
		config.NotifyMySQLSubSys:    notify.HelpMySQL,
		config.NotifyPostgresSubSys: notify.HelpPostgres,
		config.NotifyRedisSubSys:    notify.HelpRedis,
		config.NotifyWebhookSubSys:  notify.HelpWebhook,
		config.NotifyESSubSys:       notify.HelpES,
	}

	config.RegisterHelpSubSys(helpMap)
}

func (gl *Globals) lookupConfigs(s config.Config, setDriveCounts []int) {
	ctx := GlobalContext

	var err error
	if !gl.ActiveCred.IsValid() {
		// Env doesn't seem to be set, we fallback to lookup creds from the config.
		gl.ActiveCred, err = config.LookupCreds(s[config.CredentialsSubSys][config.Default])
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("Invalid credentials configuration: %w", err))
		}
	}

	gl.ServerRegion, err = config.LookupRegion(s[config.RegionSubSys][config.Default])
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Invalid region configuration: %w", err))
	}

	apiConfig, err := api.LookupConfig(s[config.APISubSys][config.Default])
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Invalid api configuration: %w", err))
	}

	gl.APIConfig.init(gl, apiConfig, setDriveCounts)

	gl.ConfigTargetList, err = notify.GetNotificationTargets(GlobalContext, s, NewGatewayHTTPTransport(gl), false)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize notification target(s): %w", err))
	}

	gl.EnvTargetList, err = notify.GetNotificationTargets(GlobalContext, gl.newServerConfig(), NewGatewayHTTPTransport(gl), true)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize notification target(s): %w", err))
	}

	// Apply dynamic config values
	logger.LogIf(ctx, gl.applyDynamicConfig(ctx, gl.newObjectLayerFn(), s))
}

// applyDynamicConfig will apply dynamic config values.
// Dynamic systems should be in config.SubSystemsDynamic as well.
func (gl *Globals) applyDynamicConfig(ctx context.Context, objAPI ObjectLayer, s config.Config) error {
	if objAPI == nil {
		return nil
	}

	// Read all dynamic configs.
	// API
	apiConfig, err := api.LookupConfig(s[config.APISubSys][config.Default])
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Invalid api configuration: %w", err))
	}

	// Compression
	cmpCfg, err := compress.LookupConfig(s[config.CompressionSubSys][config.Default])
	if err != nil {
		return fmt.Errorf("Unable to setup Compression: %w", err)
	}

	// Validate if the object layer supports compression.
	if cmpCfg.Enabled && !objAPI.IsCompressionSupported() {
		return fmt.Errorf("Backend does not support compression")
	}

	// Scanner
	scannerCfg, err := scanner.LookupConfig(s[config.ScannerSubSys][config.Default])
	if err != nil {
		return fmt.Errorf("Unable to apply scanner config: %w", err)
	}

	// Apply configurations.
	// We should not fail after this.
	gl.APIConfig.init(gl, apiConfig, objAPI.SetDriveCounts())

	gl.CompressConfigMu.Lock()
	gl.CompressConfig = cmpCfg
	gl.CompressConfigMu.Unlock()

	// update dynamic scanner values.
	scannerCycle.Update(scannerCfg.Cycle)
	logger.LogIf(ctx, scannerSleeper.Update(scannerCfg.Delay, scannerCfg.MaxWait))

	// Update all dynamic config values in memory.
	gl.ServerConfigMu.Lock()
	defer gl.ServerConfigMu.Unlock()
	if gl.ServerConfig != nil {
		for k := range config.SubSystemsDynamic {
			gl.ServerConfig[k] = s[k]
		}
	}
	return nil
}

func (gl *Globals) newServerConfig() config.Config {
	return config.New()
}

// newSrvConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func (gl *Globals) newSrvConfig(objAPI ObjectLayer) error {
	// Initialize server config.
	srvCfg := gl.newServerConfig()

	// hold the mutex lock before a new config is assigned.
	gl.ServerConfigMu.Lock()
	gl.ServerConfig = srvCfg
	gl.ServerConfigMu.Unlock()

	// Save config into file.
	return gl.saveServerConfig(GlobalContext, objAPI, gl.ServerConfig)
}

func (gl *Globals) getValidConfig(objAPI ObjectLayer) (config.Config, error) {
	return gl.readServerConfig(GlobalContext, objAPI)
}

// loadConfig - loads a new config from disk, overrides params
// from env if found and valid
func (gl *Globals) loadConfig(objAPI ObjectLayer) error {
	srvCfg, err := gl.getValidConfig(objAPI)
	if err != nil {
		return err
	}

	// Override any values from ENVs.
	gl.lookupConfigs(srvCfg, objAPI.SetDriveCounts())

	// hold the mutex lock before a new config is assigned.
	gl.ServerConfigMu.Lock()
	gl.ServerConfig = srvCfg
	gl.ServerConfigMu.Unlock()

	return nil
}
