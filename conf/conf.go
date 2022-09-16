package conf

import (
	"context"
	"encoding/json"
	"fmt"
	vault "github.com/hashicorp/vault/api"
	auth "github.com/hashicorp/vault/api/auth/approle"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"os"
)

var LOG, _ = zap.NewDevelopment()

func LoadConfiguration(url, vaultUrl string) (AppConfig, error) {
	LOG.Sugar().Infof("Loading configuration from %s ", url)
	resp, err := http.Get(url)
	appConfig := AppConfig{}

	if err != nil {
		return appConfig, fmt.Errorf("could not load configuration error: %v", err)
	}
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return appConfig, fmt.Errorf("error while parsing response body: %v", err)
	}

	secrets := AppSecrets{}
	err = secrets.loadSecretsFromVault(vaultUrl)
	if err != nil {
		return appConfig, fmt.Errorf("error while loading certificates from vault %v", err)
	}

	appConfig = createConfiguration(body, &secrets)
	return appConfig, nil

}

func (s *AppSecrets) loadSecretsFromVault(vaultUrl string) error {
	var vaultConfig = vault.DefaultConfig()
	vaultConfig.MaxRetries = 2
	vaultConfig.Address = vaultUrl

	client, err := vault.NewClient(vaultConfig)
	if err != nil {
		return fmt.Errorf("unable to initialize Vault client: %w", err)
	}

	roleID := os.Getenv("ENV_ROLE_ID")
	if roleID == "" {
		return fmt.Errorf("no role ID was provided in APPROLE_ROLE_ID env var")
	}

	secretID := &auth.SecretID{FromEnv: "ENV_SECRET_ID"}

	appRoleAuth, err := auth.NewAppRoleAuth(roleID, secretID)
	if err != nil {
		return fmt.Errorf("unable to initialize AppRole auth method: %w", err)
	}

	authInfo, err := client.Auth().Login(context.Background(), appRoleAuth)
	if err != nil {
		return fmt.Errorf("unable to login to AppRole auth method: %w", err)
	}
	if authInfo == nil {
		return fmt.Errorf("no auth info was returned after login")
	}

	vaultSecret, err := client.KVv2("kv").Get(context.Background(), "/twitter")

	vaultData, err := json.Marshal(vaultSecret.Data)
	if err == nil {
		err = json.Unmarshal(vaultData, s)
	}

	return nil
}

func createConfiguration(configServerResponseBody []byte, secrets *AppSecrets) AppConfig {
	var cloudConfig CloudConfig
	err := json.Unmarshal(configServerResponseBody, &cloudConfig)

	if err != nil {
		panic("cannot parse config " + err.Error())
	}
	applicationConfiguration := cloudConfig.PropertySource[0].Source
	applicationConfiguration.AppSecrets = *secrets
	return applicationConfiguration
}

type AppConfig struct {
	Name             string `json:"name"`
	GroupId          string `json:"group_id"`
	Topic            string `json:"topic"`
	KafkaWorkersNum  int    `json:"kafka_workers_num,string"`
	KafkaJobsNum     int    `json:"kafka_jobs_num,string"`
	BootstrapServers string `json:"bootstrap_servers"`
	AppSecrets       AppSecrets
}

type AppSecrets struct {
	TwitterConsumerKey    string `json:"consumerKey"`
	TwitterConsumerSecret string `json:"consumerSecret"`
	TwitterToken          string `json:"token"`
	TwitterTokenSecret    string `json:"tokenSecret"`
}

type PropertySource struct {
	Name   string    `json:"name"`
	Source AppConfig `json:"source"`
}

type CloudConfig struct {
	Name           string           `json:"name"`
	Profiles       []string         `json:"profiles"`
	Label          string           `json:"label"`
	Version        string           `json:"version"`
	PropertySource []PropertySource `json:"propertySources"`
}

type Option func(secrets *AppSecrets)

func WithToken() Option {
	return func(s *AppSecrets) {

	}
}

func WithAppRole() Option {
	return func(s *AppSecrets) {
	}

}
