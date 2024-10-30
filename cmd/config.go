/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"bytes"
	"context"
	"encoding/json"
	"path"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/madmin"
)

const (
	minioConfigPrefix = "config"

	kvPrefix = ".kv"

	// Captures all the previous SetKV operations and allows rollback.
	minioConfigHistoryPrefix = minioConfigPrefix + "/history"

	// MinIO configuration file.
	minioConfigFile = "config.json"
)

func (gl *Globals) saveServerConfig(ctx context.Context, objAPI ObjectLayer, config interface{}) error {
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}

	if gl.ConfigEncrypted {
		data, err = madmin.EncryptData(gl.ActiveCred.String(), data)
		if err != nil {
			return err
		}
	}

	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	// Save the new config in the std config path
	return gl.saveConfig(ctx, objAPI, configFile, data)
}

func (gl *Globals) readServerConfig(ctx context.Context, objAPI ObjectLayer) (config.Config, error) {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	configData, err := gl.readConfig(ctx, objAPI, configFile)
	if err != nil {
		// Config not found for some reason, allow things to continue
		// by initializing a new fresh config in safe mode.
		if err == errConfigNotFound && gl.newObjectLayerFn() == nil {
			return gl.newServerConfig(), nil
		}
		return nil, err
	}

	if gl.ConfigEncrypted && !utf8.Valid(configData) {
		configData, err = madmin.DecryptData(gl.ActiveCred.String(), bytes.NewReader(configData))
		if err != nil {
			if err == madmin.ErrMaliciousData {
				return nil, config.ErrInvalidCredentialsBackendEncrypted(nil)
			}
			return nil, err
		}
	}

	var srvCfg = config.New()
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(configData, &srvCfg); err != nil {
		return nil, err
	}

	// Add any missing entries
	return srvCfg.Merge(), nil
}

// ConfigSys - config system.
type ConfigSys struct {
	*Globals
}

// Load - load config.json.
func (sys *ConfigSys) Load(objAPI ObjectLayer) error {
	return sys.Init(objAPI)
}

// Init - initializes config system from config.json.
func (sys *ConfigSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	return sys.initConfig(objAPI)
}

// NewConfigSys - creates new config system object.
func NewConfigSys(g *Globals) *ConfigSys {
	return &ConfigSys{Globals: g}
}

// Initialize and load config from remote etcd or local config directory
func (gl *Globals) initConfig(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	if isFile(gl.getConfigFile()) {
		if err := gl.migrateConfig(); err != nil {
			return err
		}
	}

	// Migrates ${HOME}/.minio/config.json or config.json.deprecated
	// to '<export_path>/.minio.sys/config/config.json'
	// ignore if the file doesn't exist.
	// If etcd is set then migrates /config/config.json
	// to '<export_path>/.minio.sys/config/config.json'
	if err := gl.migrateConfigToMinioSys(objAPI); err != nil {
		return err
	}

	// Migrates backend '<export_path>/.minio.sys/config/config.json' to latest version.
	if err := gl.migrateMinioSysConfig(objAPI); err != nil {
		return err
	}

	// Migrates backend '<export_path>/.minio.sys/config/config.json' to
	// latest config format.
	if err := gl.migrateMinioSysConfigToKV(objAPI); err != nil {
		return err
	}

	return gl.loadConfig(objAPI)
}
