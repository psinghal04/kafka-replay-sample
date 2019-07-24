package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

//Configuration models the configuration for the app
type Configuration struct {
	BrokerHostEndpoint string `json:"broker-host-endpoint"`
	ConsumeGroup       string `json:"consumer-group"`
	ConsumeTopic       string `json:"topic"`
	ReplayMode         bool   `json:"replay-mode"`
	ReplayFrom         string `json:"replay-from"`
	ReplayType         string `json:"replay-type"`
}

//GetConfig gets the configuration values for the api using the file in the supplied configPath. If the file is not found,
//or if the file is missing certain required config value, the function initializes config values from environment variables.
func GetConfig(configPath string) (Configuration, error) {
	c := Configuration{}
	return loadConfigFromFile(c, configPath)
}

//if the config loaded from the file errors, no defaults will be loaded and the app will exit.
func loadConfigFromFile(c Configuration, configPath string) (Configuration, error) {
	file, err := os.Open(configPath)
	if err != nil {
		fmt.Printf("error opening config file: %v", err)
		return c, err
	}

	defer file.Close()
	err = json.NewDecoder(file).Decode(&c)
	if err != nil {
		log.Printf("Error decoding config file: %v", err)
		return c, err
	}

	return c, err
}
