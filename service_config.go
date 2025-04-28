package service_config

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/zhangsq-ax/nacos-helper-go"
	"github.com/zhangsq-ax/nacos-helper-go/options"
	"gopkg.in/yaml.v3"
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
	Watch           bool
}

type ConfigProvider struct {
	source            ConfigSource
	conf              interface{}
	configFile        string
	nacosConfigClient config_client.IConfigClient
	opts              ConfigProviderOptions
}

var (
	cProvider   *ConfigProvider
	fileWatcher *fsnotify.Watcher
)

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
func GetConfigProvider(opts *ConfigProviderOptions) (*ConfigProvider, error) {
	if cProvider == nil {
		configFile := os.Getenv(opts.EnvKey.ConfigFile)
		provider := &ConfigProvider{
			configFile: configFile,
			opts:       *opts,
		}
		if configFile == "" {
			// 使用 Nacos 提供配置
			provider.source = ConfigSource_NACOS
			nacosOpts, err := options.GetNacosOptionsByEnv()
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Failed to get Nacos options: %v", err))
			}

			configClient, err := nacos_helper.GetConfigClient(nacosOpts)
			if err != nil {
				return nil, fmt.Errorf("failed to get init nacos config client: %v", err)
			}
			provider.nacosConfigClient = *configClient
		} else {
			// 使用配置文件
			provider.source = ConfigSource_FILE
		}
		err := provider.ready()
		if err != nil {
			return nil, err
		}
		cProvider = provider
	}

	return cProvider, nil
}

func (p *ConfigProvider) ready() error {
	if p.opts.Watch {
		var err error
		if p.source == ConfigSource_NACOS {
			// 监听 Nacos 配置
			err = nacos_helper.SubscribeConfig(&p.nacosConfigClient, &options.SubscribeConfigOptions{
				DataId: os.Getenv(p.opts.EnvKey.NacosDataId),
				Group:  os.Getenv(p.opts.EnvKey.NacosGroup),
				OnChange: func(namespace, group, dataId, data string) {
					_, _ = p.updateConfig(data)
				},
			})
			if err != nil {
				return fmt.Errorf("subscribe nacos config failed: %w", err)
			}
		} else {
			// 监听配置文件
			var watcher *fsnotify.Watcher
			watcher, err = getFileWatcher()
			if err != nil {
				return fmt.Errorf("subscribe config file failed: %w", err)
			}
			err = watcher.Add(p.configFile)
			if err != nil {
				return fmt.Errorf("subscribe config file failed: %w", err)
			}
			go func() {
				defer func() {
					_ = watcher.Close()
				}()
				for {
					select {
					case event := <-watcher.Events:
						if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
							confStr, err := p.getConfigString()
							if err != nil {
								fmt.Printf("[ERROR] failed to get config string: %v\n", err)
								continue
							}
							_, err = p.updateConfig(confStr)
						}
						continue
					case err = <-watcher.Errors:
						fmt.Printf("[ERROR] watcher error: %v\n", err)
						continue
					}
				}
			}()
			return nil
		}
	}
	return nil
}

func (p *ConfigProvider) getConfigString() (confStr string, err error) {
	if p.source == ConfigSource_NACOS {
		confStr, err = p.nacosConfigClient.GetConfig(vo.ConfigParam{
			DataId: os.Getenv(p.opts.EnvKey.NacosDataId),
			Group:  os.Getenv(p.opts.EnvKey.NacosGroup),
		})
	} else {
		var data []byte
		data, err = os.ReadFile(p.configFile)
		if err != nil {
			return "", err
		}
		confStr = string(data)
	}
	if err != nil {
		return "", err
	}
	return confStr, nil
}

// Config 获取配置内容，JSON 或 YAML 格式
func (p *ConfigProvider) Config() (interface{}, error) {
	if p.conf == nil {
		confStr, err := p.getConfigString()
		if err != nil {
			return nil, err
		}
		return p.updateConfig(confStr)
	}
	return p.conf, nil
}

func (p *ConfigProvider) updateConfig(confStr string) (interface{}, error) {
	cfg := p.opts.ConfigGenerator()
	err := marshalConfig(p.opts.ConfigFormat, []byte(confStr), cfg)
	if err != nil {
		return nil, err
	}
	p.conf = cfg
	return cfg, err
}

func marshalConfig(format ConfigFormat, data []byte, conf interface{}) error {
	if format == ConfigFormat_JSON {
		return json.Unmarshal(data, conf)
	} else if format == ConfigFormat_YAML {
		return yaml.Unmarshal(data, conf)
	}
	return errors.New(fmt.Sprintf("Invalid format identifer: %v", format))
}

func getFileWatcher() (*fsnotify.Watcher, error) {
	if fileWatcher == nil {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return nil, err
		}
		fileWatcher = watcher
	}
	return fileWatcher, nil
}
