package service_config

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zhangsq-ax/nacos-helper-go/options"
	"github.com/zhangsq-ax/nacos_config"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
)

type ConfigSource int8

const (
	ConfigSource_FILE  ConfigSource = 0
	ConfigSource_NACOS ConfigSource = 1
)

type ConfigFormat int8

const (
	ConfigFormat_JSON ConfigFormat = 0
	ConfigFormat_YAML ConfigFormat = 1
)

type NacosEnvKey struct {
	ConfigFile       string
	NacosHost        string
	NacosPort        string
	NacosScheme      string
	NacosContextPath string
	NacosUsername    string
	NacosPassword    string
	NacosNamespaceId string
	NacosDataId      string
	NacosGroup       string
}

type ConfigProviderOptions struct {
	ConfigFormat    ConfigFormat
	ConfigGenerator func() interface{}
	EnvKey          *NacosEnvKey
}

type ConfigProvider struct {
	source ConfigSource
	conf   interface{}
	nacos  *nacos_config.NacosConfig
	opts   ConfigProviderOptions
}

var cProvider *ConfigProvider

func NewConfigProviderOptions(configFormat ConfigFormat, generator func() interface{}) *ConfigProviderOptions {
	return &ConfigProviderOptions{
		ConfigFormat:    configFormat,
		ConfigGenerator: generator,
		EnvKey: &NacosEnvKey{
			ConfigFile:       "CONFIG_FILE",
			NacosHost:        "NACOS_HOST",
			NacosPort:        "NACOS_PORT",
			NacosScheme:      "NACOS_SCHEME",
			NacosContextPath: "NACOS_CONTEXT_PATH",
			NacosUsername:    "NACOS_USERNAME",
			NacosPassword:    "NACOS_PASSWORD",
			NacosNamespaceId: "NACOS_NAMESPACE_ID",
			NacosDataId:      "NACOS_DATA_ID",
			NacosGroup:       "NACOS_GROUP",
		},
	}
}

// GetConfigProvider 获取 ConfigProvider
func GetConfigProvider(opts *ConfigProviderOptions, forceRefresh bool) (*ConfigProvider, error) {
	if cProvider == nil || forceRefresh {
		var (
			source    ConfigSource
			conf      interface{}
			nacos     *nacos_config.NacosConfig
			nacosOpts *options.NacosOptions
			err       error
		)
		configFile := os.Getenv(opts.EnvKey.ConfigFile)
		if configFile == "" {
			// 使用 Nacos 提供配置
			source = ConfigSource_NACOS
			nacosOpts, err = options.GetNacosOptionsByEnv()
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Failed to get Nacos options: %v", err))
			}

			nacos, err = nacos_config.NewNacosConfig(nacosOpts, os.Getenv(opts.EnvKey.NacosDataId), os.Getenv(opts.EnvKey.NacosGroup))
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Failed to init NacosConfig: %v", err))
			}
		} else {
			// 使用配置文件
			source = ConfigSource_FILE
			data, err := ioutil.ReadFile(configFile)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Failed to read config file(%s): %v", configFile, err))
			}

			conf = opts.ConfigGenerator()

			err = marshalConfig(opts.ConfigFormat, data, conf)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Failed to unmarshal config: %v", err))
			}
		}

		cProvider = &ConfigProvider{
			source: source,
			conf:   conf,
			nacos:  nacos,
			opts:   *opts,
		}
	}

	return cProvider, nil
}

// Config 获取配置内容，JSON 或 YAML 格式
func (p *ConfigProvider) Config() (interface{}, error) {
	if p.source == ConfigSource_FILE {
		return p.conf, nil
	} else {
		confStr, err := p.nacos.GetConfigString()
		if err != nil {
			return nil, err
		}
		cfg := p.opts.ConfigGenerator()
		err = marshalConfig(p.opts.ConfigFormat, []byte(confStr), cfg)
		return cfg, err
	}
}

func marshalConfig(format ConfigFormat, data []byte, conf interface{}) error {
	if format == ConfigFormat_JSON {
		return json.Unmarshal(data, conf)
	} else if format == ConfigFormat_YAML {
		return yaml.Unmarshal(data, conf)
	}
	return errors.New(fmt.Sprintf("Invalid format identifer: %v", format))
}
