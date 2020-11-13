package service_config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	//"service_config/nacos_config"
	"github.com/zhangsq-ax/nacos_config"
	"gopkg.in/yaml.v3"
	"strconv"
)

type ConfigSource int8

const (
	ConfigSource_FILE ConfigSource = 0
	ConfigSource_NACOS ConfigSource = 1
)

type ConfigFormat int8

const (
	ConfigFormat_JSON ConfigFormat = 0
	ConfigFormat_YAML ConfigFormat = 1
)

type ConfigProviderOptions struct {
	ConfigFormat ConfigFormat
	ConfigGenerator func() interface{}
	EnvKey struct {
		ConfigFile string
		NacosHost string
		NacosPort string
		NacosNamespaceId string
		NacosDataId string
		NacosGroup string
	}
}

type ConfigProvider struct {
	source ConfigSource
	conf interface{}
	nacos *nacos_config.NacosConfig
	opts ConfigProviderOptions
}

var cProvider *ConfigProvider

func NewConfigProviderOptions(configFormat ConfigFormat, generator func () interface{}) *ConfigProviderOptions {
	return &ConfigProviderOptions{
		ConfigFormat: configFormat,
		ConfigGenerator: generator,
		EnvKey: struct {
			ConfigFile       string
			NacosHost        string
			NacosPort        string
			NacosNamespaceId string
			NacosDataId      string
			NacosGroup       string
		}{
			ConfigFile: "CONFIG_FILE",
			NacosHost: "NACOS_HOST",
			NacosPort: "NACOS_PORT",
			NacosNamespaceId: "NACOS_NAMESPACE_ID",
			NacosDataId: "NACOS_DATA_ID",
			NacosGroup: "NACOS_GROUP",
		},
	}
}

func GetConfigProvider(opts *ConfigProviderOptions, forceRefresh bool) (*ConfigProvider, error) {
	if cProvider == nil || forceRefresh {
		var (
			source ConfigSource
			conf interface{}
			nacos *nacos_config.NacosConfig
		)
		configFile := os.Getenv(opts.EnvKey.ConfigFile)
		if configFile == "" {
			// 使用 Nacos 提供配置
			source = ConfigSource_NACOS
			portStr := os.Getenv(opts.EnvKey.NacosPort)
			port, err := strconv.ParseUint(portStr, 0, 64)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Invalid NACOS_CONFIG_PORT: %s", portStr))
			}

			nacos, err = nacos_config.NewNacosConfig(nacos_config.NacosOptions{
				Host: os.Getenv(opts.EnvKey.NacosHost),
				Port: port,
				NamespaceId: os.Getenv(opts.EnvKey.NacosNamespaceId),
				DataId: os.Getenv(opts.EnvKey.NacosDataId),
				Group: os.Getenv(opts.EnvKey.NacosGroup),
			})
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
			conf: conf,
			nacos: nacos,
			opts: *opts,
		}
	}

	return cProvider, nil
}

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