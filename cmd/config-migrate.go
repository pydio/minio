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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/config/compress"
	"github.com/minio/minio/cmd/config/identity/openid"
	"github.com/minio/minio/cmd/config/policy/opa"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/quick"
)

// DO NOT EDIT following message template, please open a github issue to discuss instead.
var configMigrateMSGTemplate = "Configuration file %s migrated from version '%s' to '%s' successfully."

// Save config file to corresponding backend
func (gl *Globals) Save(configFile string, data interface{}) error {
	return quick.SaveConfig(data, configFile)
}

// Load config from backend
func (gl *Globals) Load(configFile string, data interface{}) (quick.Config, error) {
	return quick.LoadConfig(configFile, data)
}

// GetVersion gets config version from backend
func (gl *Globals) GetVersion(configFile string) (string, error) {
	return quick.GetVersion(configFile)
}

// Migrates all config versions from "1" to "28".
func (gl *Globals) migrateConfig() error {
	// Purge all configs with version '1',
	// this is a special case since version '1' used
	// to be a filename 'fsUsers.json' not 'config.json'.
	if err := gl.purgeV1(); err != nil {
		return err
	}

	// Load only config version information.
	version, err := gl.GetVersion(gl.getConfigFile())
	if err != nil {
		if osIsNotExist(err) || osIsPermission(err) {
			return nil
		}
		return err
	}

	// Conditional to migrate only relevant config versions.
	// Upon success migration continues to the next version in sequence.
	switch version {
	case "2":
		// Migrate version '2' to '3'.
		if err = gl.migrateV2ToV3(); err != nil {
			return err
		}
		fallthrough
	case "3":
		// Migrate version '3' to '4'.
		if err = gl.migrateV3ToV4(); err != nil {
			return err
		}
		fallthrough
	case "4":
		// Migrate version '4' to '5'.
		if err = gl.migrateV4ToV5(); err != nil {
			return err
		}
		fallthrough
	case "5":
		// Migrate version '5' to '6.
		if err = gl.migrateV5ToV6(); err != nil {
			return err
		}
		fallthrough
	case "6":
		// Migrate version '6' to '7'.
		if err = gl.migrateV6ToV7(); err != nil {
			return err
		}
		fallthrough
	case "7":
		// Migrate version '7' to '8'.
		if err = gl.migrateV7ToV8(); err != nil {
			return err
		}
		fallthrough
	case "8":
		// Migrate version '8' to '9'.
		if err = gl.migrateV8ToV9(); err != nil {
			return err
		}
		fallthrough
	case "9":
		// Migrate version '9' to '10'.
		if err = gl.migrateV9ToV10(); err != nil {
			return err
		}
		fallthrough
	case "10":
		// Migrate version '10' to '11'.
		if err = gl.migrateV10ToV11(); err != nil {
			return err
		}
		fallthrough
	case "11":
		// Migrate version '11' to '12'.
		if err = gl.migrateV11ToV12(); err != nil {
			return err
		}
		fallthrough
	case "12":
		// Migrate version '12' to '13'.
		if err = gl.migrateV12ToV13(); err != nil {
			return err
		}
		fallthrough
	case "13":
		// Migrate version '13' to '14'.
		if err = gl.migrateV13ToV14(); err != nil {
			return err
		}
		fallthrough
	case "14":
		// Migrate version '14' to '15'.
		if err = gl.migrateV14ToV15(); err != nil {
			return err
		}
		fallthrough
	case "15":
		// Migrate version '15' to '16'.
		if err = gl.migrateV15ToV16(); err != nil {
			return err
		}
		fallthrough
	case "16":
		// Migrate version '16' to '17'.
		if err = gl.migrateV16ToV17(); err != nil {
			return err
		}
		fallthrough
	case "17":
		// Migrate version '17' to '18'.
		if err = gl.migrateV17ToV18(); err != nil {
			return err
		}
		fallthrough
	case "18":
		// Migrate version '18' to '19'.
		if err = gl.migrateV18ToV19(); err != nil {
			return err
		}
		fallthrough
	case "19":
		if err = gl.migrateV19ToV20(); err != nil {
			return err
		}
		fallthrough
	case "20":
		if err = gl.migrateV20ToV21(); err != nil {
			return err
		}
		fallthrough
	case "21":
		if err = gl.migrateV21ToV22(); err != nil {
			return err
		}
		fallthrough
	case "22":
		if err = gl.migrateV22ToV23(); err != nil {
			return err
		}
		fallthrough
	case "23":
		if err = gl.migrateV23ToV24(); err != nil {
			return err
		}
		fallthrough
	case "24":
		if err = gl.migrateV24ToV25(); err != nil {
			return err
		}
		fallthrough
	case "25":
		if err = gl.migrateV25ToV26(); err != nil {
			return err
		}
		fallthrough
	case "26":
		if err = gl.migrateV26ToV27(); err != nil {
			return err
		}
		fallthrough
	case "27":
		if err = gl.migrateV27ToV28(); err != nil {
			return err
		}
		fallthrough
	case "33":
		// No migration needed. this always points to current version.
		err = nil
	}
	return err
}

// Version '1' is not supported anymore and deprecated, safe to delete.
func (gl *Globals) purgeV1() error {
	configFile := filepath.Join(gl.CliContext.ConfigDir.Get(), "fsUsers.json")

	cv1 := &configV1{}
	_, err := gl.Load(configFile, cv1)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘1’. %w", err)
	}
	if cv1.Version != "1" {
		return fmt.Errorf("unrecognized config version ‘%s’", cv1.Version)
	}

	os.RemoveAll(configFile)
	logger.Info("Removed unsupported config version ‘1’.")
	return nil
}

// Version '2' to '3' config migration adds new fields and re-orders
// previous fields. Simplifies config for future additions.
func (gl *Globals) migrateV2ToV3() error {
	configFile := gl.getConfigFile()

	cv2 := &configV2{}
	_, err := gl.Load(configFile, cv2)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘2’. %w", err)
	}
	if cv2.Version != "2" {
		return nil
	}

	cred, err := auth.CreateCredentials(cv2.Credentials.AccessKey, cv2.Credentials.SecretKey)
	if err != nil {
		return fmt.Errorf("Invalid credential in V2 configuration file. %w", err)
	}

	srvConfig := &configV3{}
	srvConfig.Version = "3"
	srvConfig.Addr = ":9000"
	srvConfig.Credential = cred
	srvConfig.Region = cv2.Credentials.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature V4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = consoleLoggerV1{
		Enable: true,
		Level:  "fatal",
	}
	flogger := fileLoggerV1{}
	flogger.Level = "error"
	if cv2.FileLogger.Filename != "" {
		flogger.Enable = true
		flogger.Filename = cv2.FileLogger.Filename
	}
	srvConfig.Logger.File = flogger

	slogger := syslogLoggerV3{}
	slogger.Level = "debug"
	if cv2.SyslogLogger.Addr != "" {
		slogger.Enable = true
		slogger.Addr = cv2.SyslogLogger.Addr
	}
	srvConfig.Logger.Syslog = slogger

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv2.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv2.Version, srvConfig.Version)
	return nil
}

// Version '3' to '4' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func (gl *Globals) migrateV3ToV4() error {
	configFile := gl.getConfigFile()

	cv3 := &configV3{}
	_, err := gl.Load(configFile, cv3)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘3’. %w", err)
	}
	if cv3.Version != "3" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV4{}
	srvConfig.Version = "4"
	srvConfig.Credential = cv3.Credential
	srvConfig.Region = cv3.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv3.Logger.Console
	srvConfig.Logger.File = cv3.Logger.File
	srvConfig.Logger.Syslog = cv3.Logger.Syslog

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv3.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv3.Version, srvConfig.Version)
	return nil
}

// Version '4' to '5' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func (gl *Globals) migrateV4ToV5() error {
	configFile := gl.getConfigFile()

	cv4 := &configV4{}
	_, err := gl.Load(configFile, cv4)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘4’. %w", err)
	}
	if cv4.Version != "4" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV5{}
	srvConfig.Version = "5"
	srvConfig.Credential = cv4.Credential
	srvConfig.Region = cv4.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv4.Logger.Console
	srvConfig.Logger.File = cv4.Logger.File
	srvConfig.Logger.Syslog = cv4.Logger.Syslog
	srvConfig.Logger.AMQP.Enable = false
	srvConfig.Logger.ElasticSearch.Enable = false
	srvConfig.Logger.Redis.Enable = false

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv4.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv4.Version, srvConfig.Version)
	return nil
}

// Version '5' to '6' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func (gl *Globals) migrateV5ToV6() error {
	configFile := gl.getConfigFile()

	cv5 := &configV5{}
	_, err := gl.Load(configFile, cv5)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘5’. %w", err)
	}
	if cv5.Version != "5" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV6{}
	srvConfig.Version = "6"
	srvConfig.Credential = cv5.Credential
	srvConfig.Region = cv5.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv5.Logger.Console
	srvConfig.Logger.File = cv5.Logger.File
	srvConfig.Logger.Syslog = cv5.Logger.Syslog

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv5.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv5.Version, srvConfig.Version)
	return nil
}

// Version '6' to '7' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func (gl *Globals) migrateV6ToV7() error {
	configFile := gl.getConfigFile()

	cv6 := &configV6{}
	_, err := gl.Load(configFile, cv6)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘6’. %w", err)
	}
	if cv6.Version != "6" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &serverConfigV7{}
	srvConfig.Version = "7"
	srvConfig.Credential = cv6.Credential
	srvConfig.Region = cv6.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv6.Logger.Console
	srvConfig.Logger.File = cv6.Logger.File
	srvConfig.Logger.Syslog = cv6.Logger.Syslog

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv6.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv6.Version, srvConfig.Version)
	return nil
}

// Version '7' to '8' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func (gl *Globals) migrateV7ToV8() error {
	configFile := gl.getConfigFile()

	cv7 := &serverConfigV7{}
	_, err := gl.Load(configFile, cv7)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘7’. %w", err)
	}
	if cv7.Version != "7" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &serverConfigV8{}
	srvConfig.Version = "8"
	srvConfig.Credential = cv7.Credential
	srvConfig.Region = cv7.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv7.Logger.Console
	srvConfig.Logger.File = cv7.Logger.File
	srvConfig.Logger.Syslog = cv7.Logger.Syslog

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv7.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv7.Version, srvConfig.Version)
	return nil
}

// Version '8' to '9' migration. Adds postgresql notifier
// configuration, but it's otherwise the same as V8.
func (gl *Globals) migrateV8ToV9() error {
	configFile := gl.getConfigFile()

	cv8 := &serverConfigV8{}
	_, err := gl.Load(configFile, cv8)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘8’. %w", err)
	}
	if cv8.Version != "8" {
		return nil
	}

	// Copy over fields from V8 into V9 config struct
	srvConfig := &serverConfigV9{}
	srvConfig.Version = "9"
	srvConfig.Credential = cv8.Credential
	srvConfig.Region = cv8.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv8.Logger.Console
	srvConfig.Logger.Console.Level = "error"
	srvConfig.Logger.File = cv8.Logger.File
	srvConfig.Logger.Syslog = cv8.Logger.Syslog

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv8.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv8.Version, srvConfig.Version)
	return nil
}

// Version '9' to '10' migration. Remove syslog config
// but it's otherwise the same as V9.
func (gl *Globals) migrateV9ToV10() error {
	configFile := gl.getConfigFile()

	cv9 := &serverConfigV9{}
	_, err := gl.Load(configFile, cv9)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘9’. %w", err)
	}
	if cv9.Version != "9" {
		return nil
	}

	// Copy over fields from V9 into V10 config struct
	srvConfig := &serverConfigV10{}
	srvConfig.Version = "10"
	srvConfig.Credential = cv9.Credential
	srvConfig.Region = cv9.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv9.Logger.Console
	srvConfig.Logger.File = cv9.Logger.File

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv9.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv9.Version, srvConfig.Version)
	return nil
}

// Version '10' to '11' migration. Add support for Kafka
// notifications.
func (gl *Globals) migrateV10ToV11() error {
	configFile := gl.getConfigFile()

	cv10 := &serverConfigV10{}
	_, err := gl.Load(configFile, cv10)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘10’. %w", err)
	}
	if cv10.Version != "10" {
		return nil
	}

	// Copy over fields from V10 into V11 config struct
	srvConfig := &serverConfigV11{}
	srvConfig.Version = "11"
	srvConfig.Credential = cv10.Credential
	srvConfig.Region = cv10.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv10.Logger.Console
	srvConfig.Logger.File = cv10.Logger.File

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv10.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv10.Version, srvConfig.Version)
	return nil
}

// Version '11' to '12' migration. Add support for NATS streaming
// notifications.
func (gl *Globals) migrateV11ToV12() error {
	configFile := gl.getConfigFile()

	cv11 := &serverConfigV11{}
	_, err := gl.Load(configFile, cv11)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘11’. %w", err)
	}
	if cv11.Version != "11" {
		return nil
	}

	// Copy over fields from V11 into V12 config struct
	srvConfig := &serverConfigV12{}
	srvConfig.Version = "12"
	srvConfig.Credential = cv11.Credential
	srvConfig.Region = cv11.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv11.Logger.Console
	srvConfig.Logger.File = cv11.Logger.File

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv11.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv11.Version, srvConfig.Version)
	return nil
}

// Version '12' to '13' migration. Add support for custom webhook endpoint.
func (gl *Globals) migrateV12ToV13() error {
	configFile := gl.getConfigFile()

	cv12 := &serverConfigV12{}
	_, err := gl.Load(configFile, cv12)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘12’. %w", err)
	}
	if cv12.Version != "12" {
		return nil
	}

	// Copy over fields from V12 into V13 config struct
	srvConfig := &serverConfigV13{
		Logger: &loggerV7{},
		Notify: &notifierV3{},
	}
	srvConfig.Version = "13"
	srvConfig.Credential = cv12.Credential
	srvConfig.Region = cv12.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv12.Logger.Console
	srvConfig.Logger.File = cv12.Logger.File

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv12.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv12.Version, srvConfig.Version)
	return nil
}

// Version '13' to '14' migration. Add support for browser param.
func (gl *Globals) migrateV13ToV14() error {
	configFile := gl.getConfigFile()

	cv13 := &serverConfigV13{}
	_, err := gl.Load(configFile, cv13)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘13’. %w", err)
	}
	if cv13.Version != "13" {
		return nil
	}

	// Copy over fields from V13 into V14 config struct
	srvConfig := &serverConfigV14{
		Logger: &loggerV7{},
		Notify: &notifierV3{},
	}
	srvConfig.Version = "14"
	srvConfig.Credential = cv13.Credential
	srvConfig.Region = cv13.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv13.Logger.Console
	srvConfig.Logger.File = cv13.Logger.File

	// Set the new browser parameter to true by default
	srvConfig.Browser = true

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv13.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv13.Version, srvConfig.Version)
	return nil
}

// Version '14' to '15' migration. Add support for MySQL notifications.
func (gl *Globals) migrateV14ToV15() error {
	configFile := gl.getConfigFile()

	cv14 := &serverConfigV14{}
	_, err := gl.Load(configFile, cv14)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘14’. %w", err)
	}
	if cv14.Version != "14" {
		return nil
	}

	// Copy over fields from V14 into V15 config struct
	srvConfig := &serverConfigV15{
		Logger: &loggerV7{},
		Notify: &notifierV3{},
	}
	srvConfig.Version = "15"
	srvConfig.Credential = cv14.Credential
	srvConfig.Region = cv14.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv14.Logger.Console
	srvConfig.Logger.File = cv14.Logger.File

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv14.Browser

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv14.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv14.Version, srvConfig.Version)
	return nil
}

// Version '15' to '16' migration. Remove log level in loggers
// and rename 'fileName' filed in File logger to 'filename'
func (gl *Globals) migrateV15ToV16() error {
	configFile := gl.getConfigFile()

	cv15 := &serverConfigV15{}
	_, err := gl.Load(configFile, cv15)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘15’. %w", err)
	}
	if cv15.Version != "15" {
		return nil
	}

	// Copy over fields from V15 into V16 config struct
	srvConfig := &serverConfigV16{
		Logger: &loggers{},
		Notify: &notifierV3{},
	}
	srvConfig.Version = "16"
	srvConfig.Credential = cv15.Credential
	srvConfig.Region = cv15.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv15.Browser

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv15.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv15.Version, srvConfig.Version)
	return nil
}

// Version '16' to '17' migration. Adds "format" configuration
// parameter for database targets.
func (gl *Globals) migrateV16ToV17() error {
	configFile := gl.getConfigFile()

	cv16 := &serverConfigV16{}
	_, err := gl.Load(configFile, cv16)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘16’. %w", err)
	}
	if cv16.Version != "16" {
		return nil
	}

	// Copy over fields from V16 into V17 config struct
	srvConfig := &serverConfigV17{
		Logger: &loggers{},
		Notify: &notifierV3{},
	}
	srvConfig.Version = "17"
	srvConfig.Credential = cv16.Credential
	srvConfig.Region = cv16.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	srvConfig.Logger.Console = cv16.Logger.Console
	srvConfig.Logger.File = cv16.Logger.File

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv16.Browser

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv16.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv16.Version, srvConfig.Version)
	return nil
}

// Version '17' to '18' migration. Adds "deliveryMode" configuration
// parameter for AMQP notification target
func (gl *Globals) migrateV17ToV18() error {
	configFile := gl.getConfigFile()

	cv17 := &serverConfigV17{}
	_, err := gl.Load(configFile, cv17)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘17’. %w", err)
	}
	if cv17.Version != "17" {
		return nil
	}

	// Copy over fields from V17 into V18 config struct
	srvConfig := &serverConfigV17{
		Logger: &loggers{},
		Notify: &notifierV3{},
	}
	srvConfig.Version = "18"
	srvConfig.Credential = cv17.Credential
	srvConfig.Region = cv17.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	srvConfig.Logger.Console = cv17.Logger.Console
	srvConfig.Logger.File = cv17.Logger.File

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv17.Browser

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv17.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv17.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV18ToV19() error {
	configFile := gl.getConfigFile()

	cv18 := &serverConfigV18{}
	_, err := gl.Load(configFile, cv18)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘18’. %w", err)
	}
	if cv18.Version != "18" {
		return nil
	}

	// Copy over fields from V18 into V19 config struct
	srvConfig := &serverConfigV18{
		Logger: &loggers{},
		Notify: &notifierV3{},
	}
	srvConfig.Version = "19"
	srvConfig.Credential = cv18.Credential
	srvConfig.Region = cv18.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	srvConfig.Logger.Console = cv18.Logger.Console
	srvConfig.Logger.File = cv18.Logger.File

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv18.Browser

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv18.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv18.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV19ToV20() error {
	configFile := gl.getConfigFile()

	cv19 := &serverConfigV19{}
	_, err := gl.Load(configFile, cv19)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘18’. %w", err)
	}
	if cv19.Version != "19" {
		return nil
	}

	// Copy over fields from V19 into V20 config struct
	srvConfig := &serverConfigV20{
		Logger: &loggers{},
		Notify: &notifierV3{},
	}
	srvConfig.Version = "20"
	srvConfig.Credential = cv19.Credential
	srvConfig.Region = cv19.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	srvConfig.Logger.Console = cv19.Logger.Console
	srvConfig.Logger.File = cv19.Logger.File

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv19.Browser

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv19.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv19.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV20ToV21() error {
	configFile := gl.getConfigFile()

	cv20 := &serverConfigV20{}
	_, err := gl.Load(configFile, cv20)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘20’. %w", err)
	}
	if cv20.Version != "20" {
		return nil
	}

	// Copy over fields from V20 into V21 config struct
	srvConfig := &serverConfigV21{
		Notify: &notifierV3{},
	}
	srvConfig.Version = "21"
	srvConfig.Credential = cv20.Credential
	srvConfig.Region = cv20.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv20.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv20.Domain

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv20.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv20.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV21ToV22() error {
	configFile := gl.getConfigFile()

	cv21 := &serverConfigV21{}
	_, err := gl.Load(configFile, cv21)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘21’. %w", err)
	}
	if cv21.Version != "21" {
		return nil
	}

	// Copy over fields from V21 into V22 config struct
	srvConfig := &serverConfigV22{
		Notify: notifierV3{},
	}
	srvConfig.Version = "22"
	srvConfig.Credential = cv21.Credential
	srvConfig.Region = cv21.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv21.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv21.Domain

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv21.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv21.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV22ToV23() error {
	configFile := gl.getConfigFile()

	cv22 := &serverConfigV22{}
	_, err := gl.Load(configFile, cv22)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘22’. %w", err)
	}
	if cv22.Version != "22" {
		return nil
	}

	// Copy over fields from V22 into V23 config struct
	srvConfig := &serverConfigV23{
		Notify: notifierV3{},
	}
	srvConfig.Version = "23"
	srvConfig.Credential = cv22.Credential
	srvConfig.Region = cv22.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv22.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv22.Domain

	// Load storage class config from existing storage class config in the file
	srvConfig.StorageClass.RRS = cv22.StorageClass.RRS
	srvConfig.StorageClass.Standard = cv22.StorageClass.Standard

	// Init cache config.For future migration, Cache config needs to be copied over from previous version.
	srvConfig.Cache.Drives = []string{}
	srvConfig.Cache.Exclude = []string{}
	srvConfig.Cache.Expiry = 90

	if err = gl.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv22.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv22.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV23ToV24() error {
	configFile := gl.getConfigFile()

	cv23 := &serverConfigV23{}
	_, err := quick.LoadConfig(configFile, cv23)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘23’. %w", err)
	}
	if cv23.Version != "23" {
		return nil
	}

	// Copy over fields from V23 into V24 config struct
	srvConfig := &serverConfigV24{
		Notify: notifierV3{},
	}
	srvConfig.Version = "24"
	srvConfig.Credential = cv23.Credential
	srvConfig.Region = cv23.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv23.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv23.Domain

	// Load storage class config from existing storage class config in the file.
	srvConfig.StorageClass.RRS = cv23.StorageClass.RRS
	srvConfig.StorageClass.Standard = cv23.StorageClass.Standard

	// Load cache config from existing cache config in the file.
	srvConfig.Cache.Drives = cv23.Cache.Drives
	srvConfig.Cache.Exclude = cv23.Cache.Exclude
	srvConfig.Cache.Expiry = cv23.Cache.Expiry

	if err = quick.SaveConfig(srvConfig, configFile); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv23.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv23.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV24ToV25() error {
	configFile := gl.getConfigFile()

	cv24 := &serverConfigV24{}
	_, err := quick.LoadConfig(configFile, cv24)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘24’. %w", err)
	}
	if cv24.Version != "24" {
		return nil
	}

	// Copy over fields from V24 into V25 config struct
	srvConfig := &serverConfigV25{
		Notify: notifierV3{},
	}
	srvConfig.Version = "25"
	srvConfig.Credential = cv24.Credential
	srvConfig.Region = cv24.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv24.Browser

	// New field should be turned-off by default.
	srvConfig.Worm = false // cv25.Worm should be used here
	// for the next migration from v25 to v26 to persist
	// local config value.

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv24.Domain

	// Load storage class config from existing storage class config in the file.
	srvConfig.StorageClass.RRS = cv24.StorageClass.RRS
	srvConfig.StorageClass.Standard = cv24.StorageClass.Standard

	// Load cache config from existing cache config in the file.
	srvConfig.Cache.Drives = cv24.Cache.Drives
	srvConfig.Cache.Exclude = cv24.Cache.Exclude
	srvConfig.Cache.Expiry = cv24.Cache.Expiry

	if err = quick.SaveConfig(srvConfig, configFile); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv24.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv24.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV25ToV26() error {
	configFile := gl.getConfigFile()

	cv25 := &serverConfigV25{}
	_, err := quick.LoadConfig(configFile, cv25)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘25’. %w", err)
	}
	if cv25.Version != "25" {
		return nil
	}

	// Copy over fields from V25 into V26 config struct
	srvConfig := &serverConfigV26{
		Notify: notifierV3{},
	}
	srvConfig.Version = "26"
	srvConfig.Credential = cv25.Credential
	srvConfig.Region = cv25.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv25.Browser

	// Load worm config from existing config in the file.
	srvConfig.Worm = cv25.Worm

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv25.Domain

	// Load storage class config from existing storage class config in the file.
	srvConfig.StorageClass.RRS = cv25.StorageClass.RRS
	srvConfig.StorageClass.Standard = cv25.StorageClass.Standard

	// Load cache config from existing cache config in the file.
	srvConfig.Cache.Drives = cv25.Cache.Drives
	srvConfig.Cache.Exclude = cv25.Cache.Exclude
	srvConfig.Cache.Expiry = cv25.Cache.Expiry

	// Add predefined value to new server config.
	srvConfig.Cache.MaxUse = 80

	if err = quick.SaveConfig(srvConfig, configFile); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %w", cv25.Version, srvConfig.Version, err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, cv25.Version, srvConfig.Version)
	return nil
}

func (gl *Globals) migrateV26ToV27() error {
	configFile := gl.getConfigFile()

	// config V27 is backward compatible with V26, load the old
	// config file in serverConfigV27 struct and put some examples
	// in the new `logger` field
	srvConfig := &serverConfigV27{}
	_, err := quick.LoadConfig(configFile, srvConfig)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config file. %w", err)
	}

	if srvConfig.Version != "26" {
		return nil
	}

	srvConfig.Version = "27"
	// Enable console logging by default to avoid breaking users
	// current deployments
	srvConfig.Logger.Console.Enabled = true
	srvConfig.Logger.HTTP = make(map[string]logger.HTTP)
	srvConfig.Logger.HTTP["1"] = logger.HTTP{}

	if err = quick.SaveConfig(srvConfig, configFile); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘26’ to ‘27’. %w", err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, "26", "27")
	return nil
}

func (gl *Globals) migrateV27ToV28() error {
	configFile := gl.getConfigFile()

	// config V28 is backward compatible with V27, load the old
	// config file in serverConfigV28 struct and initialize KMSConfig

	srvConfig := &serverConfigV28{}
	_, err := quick.LoadConfig(configFile, srvConfig)
	if osIsNotExist(err) || osIsPermission(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config file. %w", err)
	}

	if srvConfig.Version != "27" {
		return nil
	}

	srvConfig.Version = "28"
	srvConfig.KMS = crypto.KMSConfig{}
	if err = quick.SaveConfig(srvConfig, configFile); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘27’ to ‘28’. %w", err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, "27", "28")
	return nil
}

// Migrates ${HOME}/.minio/config.json to '<export_path>/.minio.sys/config/config.json'
// if etcd is configured then migrates /config/config.json to '<export_path>/.minio.sys/config/config.json'
func (gl *Globals) migrateConfigToMinioSys(objAPI ObjectLayer) (err error) {
	// Construct path to config.json for the given bucket.
	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	defer func() {
		if err == nil {
			os.Rename(gl.getConfigFile(), gl.getConfigFile()+".deprecated")
		}
	}()

	// Verify if backend already has the file (after holding lock)
	if err = gl.checkConfig(GlobalContext, objAPI, configFile); err != errConfigNotFound {
		return err
	} // if errConfigNotFound proceed to migrate..

	var configFiles = []string{
		gl.getConfigFile(),
		gl.getConfigFile() + ".deprecated",
		configFile,
	}
	var config = &serverConfigV27{}
	for _, cfgFile := range configFiles {
		if _, err = gl.Load(cfgFile, config); err != nil {
			if !osIsNotExist(err) && !osIsPermission(err) {
				return err
			}
			continue
		}
		break
	}
	if osIsPermission(err) {
		logger.Info("Older config found but not readable %s, proceeding to initialize new config anyways", err)
	}
	if osIsNotExist(err) || osIsPermission(err) {
		// Initialize the server config, if no config exists.
		return gl.newSrvConfig(objAPI)
	}
	return gl.saveServerConfig(GlobalContext, objAPI, config)
}

// Migrates '.minio.sys/config.json' to v33.
func (gl *Globals) migrateMinioSysConfig(objAPI ObjectLayer) error {
	// Construct path to config.json for the given bucket.
	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	// Check if the config version is latest, if not migrate.
	ok, _, err := gl.checkConfigVersion(objAPI, configFile, "33")
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	if err := gl.migrateV27ToV28MinioSys(objAPI); err != nil {
		return err
	}
	if err := gl.migrateV28ToV29MinioSys(objAPI); err != nil {
		return err
	}
	if err := gl.migrateV29ToV30MinioSys(objAPI); err != nil {
		return err
	}
	if err := gl.migrateV30ToV31MinioSys(objAPI); err != nil {
		return err
	}
	if err := gl.migrateV31ToV32MinioSys(objAPI); err != nil {
		return err
	}
	return gl.migrateV32ToV33MinioSys(objAPI)
}

func (gl *Globals) checkConfigVersion(objAPI ObjectLayer, configFile string, version string) (bool, []byte, error) {
	data, err := gl.readConfig(GlobalContext, objAPI, configFile)
	if err != nil {
		return false, nil, err
	}

	if gl.ConfigEncrypted && !utf8.Valid(data) {
		data, err = madmin.DecryptData(gl.ActiveCred.String(), bytes.NewReader(data))
		if err != nil {
			if err == madmin.ErrMaliciousData {
				return false, nil, config.ErrInvalidCredentialsBackendEncrypted(nil)
			}
			return false, nil, err
		}
	}

	var versionConfig struct {
		Version string `json:"version"`
	}

	vcfg := &versionConfig
	if err = json.Unmarshal(data, vcfg); err != nil {
		return false, nil, err
	}
	return vcfg.Version == version, data, nil
}

func (gl *Globals) migrateV27ToV28MinioSys(objAPI ObjectLayer) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	ok, data, err := gl.checkConfigVersion(objAPI, configFile, "27")
	if err == errConfigNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config file. %w", err)
	}
	if !ok {
		return nil
	}

	cfg := &serverConfigV28{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return err
	}

	cfg.Version = "28"
	cfg.KMS = crypto.KMSConfig{}

	if err = gl.saveServerConfig(GlobalContext, objAPI, cfg); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘27’ to ‘28’. %w", err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, "27", "28")
	return nil
}

func (gl *Globals) migrateV28ToV29MinioSys(objAPI ObjectLayer) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	ok, data, err := gl.checkConfigVersion(objAPI, configFile, "28")
	if err == errConfigNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config file. %w", err)
	}
	if !ok {
		return nil
	}

	cfg := &serverConfigV29{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return err
	}

	cfg.Version = "29"
	if err = gl.saveServerConfig(GlobalContext, objAPI, cfg); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘28’ to ‘29’. %w", err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, "28", "29")
	return nil
}

func (gl *Globals) migrateV29ToV30MinioSys(objAPI ObjectLayer) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	ok, data, err := gl.checkConfigVersion(objAPI, configFile, "29")
	if err == errConfigNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config file. %w", err)
	}
	if !ok {
		return nil
	}

	cfg := &serverConfigV30{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return err
	}

	cfg.Version = "30"
	// Init compression config.For future migration, Compression config needs to be copied over from previous version.
	cfg.Compression.Enabled = false
	cfg.Compression.Extensions = strings.Split(compress.DefaultExtensions, config.ValueSeparator)
	cfg.Compression.MimeTypes = strings.Split(compress.DefaultMimeTypes, config.ValueSeparator)

	if err = gl.saveServerConfig(GlobalContext, objAPI, cfg); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘29’ to ‘30’. %w", err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, "29", "30")
	return nil
}

func (gl *Globals) migrateV30ToV31MinioSys(objAPI ObjectLayer) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	ok, data, err := gl.checkConfigVersion(objAPI, configFile, "30")
	if err == errConfigNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config file. %w", err)
	}
	if !ok {
		return nil
	}

	cfg := &serverConfigV31{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return err
	}

	cfg.Version = "31"
	cfg.OpenID = openid.Config{}
	cfg.OpenID.JWKS.URL = &xnet.URL{}

	cfg.Policy.OPA = opa.Args{
		URL:       &xnet.URL{},
		AuthToken: "",
	}

	if err = gl.saveServerConfig(GlobalContext, objAPI, cfg); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘30’ to ‘31’. %w", err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, "30", "31")
	return nil
}

func (gl *Globals) migrateV31ToV32MinioSys(objAPI ObjectLayer) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	ok, data, err := gl.checkConfigVersion(objAPI, configFile, "31")
	if err == errConfigNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config file. %w", err)
	}
	if !ok {
		return nil
	}

	cfg := &serverConfigV32{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return err
	}

	if err = gl.saveServerConfig(GlobalContext, objAPI, cfg); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘31’ to ‘32’. %w", err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, "31", "32")
	return nil
}

func (gl *Globals) migrateV32ToV33MinioSys(objAPI ObjectLayer) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	ok, data, err := gl.checkConfigVersion(objAPI, configFile, "32")
	if err == errConfigNotFound {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config file. %w", err)
	}
	if !ok {
		return nil
	}

	cfg := &serverConfigV33{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return err
	}

	cfg.Version = "33"

	if err = gl.saveServerConfig(GlobalContext, objAPI, cfg); err != nil {
		return fmt.Errorf("Failed to migrate config from '32' to '33' . %w", err)
	}

	logger.Info(configMigrateMSGTemplate, configFile, "32", "33")
	return nil
}

func (gl *Globals) migrateMinioSysConfigToKV(objAPI ObjectLayer) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	// Check if the config version is latest, if not migrate.
	ok, data, err := gl.checkConfigVersion(objAPI, configFile, "33")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	cfg := &serverConfigV33{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return err
	}

	newCfg := gl.newServerConfig()

	config.SetCredentials(newCfg, cfg.Credential)
	config.SetRegion(newCfg, cfg.Region)

	storageclass.SetStorageClass(newCfg, cfg.StorageClass)

	for k, loggerArgs := range cfg.Logger.HTTP {
		logger.SetLoggerHTTP(newCfg, k, loggerArgs)
	}
	for k, auditArgs := range cfg.Logger.Audit {
		logger.SetLoggerHTTPAudit(newCfg, k, auditArgs)
	}

	crypto.SetKMSConfig(newCfg, cfg.KMS)
	openid.SetIdentityOpenID(newCfg, cfg.OpenID)
	opa.SetPolicyOPAConfig(newCfg, cfg.Policy.OPA)
	cache.SetCacheConfig(newCfg, cfg.Cache)
	compress.SetCompressionConfig(newCfg, cfg.Compression)

	if err = gl.saveServerConfig(GlobalContext, objAPI, newCfg); err != nil {
		return err
	}

	logger.Info("Configuration file %s migrated from version '%s' to new KV format successfully.",
		configFile, "33")
	return nil
}
