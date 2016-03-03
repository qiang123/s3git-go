package config

import (
	"os"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"strings"
)

const LIFEDRIVE_CONFIG = ".lifedrive.config"
const CONFIG = "config"

var Config ConfigObject

type ConfigObject struct {
	LdVersion     int    `json:"ldVersion"`
	LdType        string `json:"ldType"` // config

	LdCasPath	  string `json:"ldCasPath"`

	LdS3Bucket    string `json:"ldS3Bucket"`
	LdS3Region    string `json:"ldS3Region"`
	LdS3AccessKey string `json:"ldS3AccessKey"`
	LdS3SecretKey string `json:"ldS3SecretKey"`

	LdMinioEndpoint string `json:"ldMinioEndpoint"`
	LdMinioInsecure bool   `json:"ldMinioInsecure"`

	LdAcdRefreshToken string `json:"ldAcdRefreshToken"`
}

func getConfigFile(dir string) string {
	return dir + "/" + LIFEDRIVE_CONFIG
}

func LoadConfig(dir string) (bool, error) {

	configFile := getConfigFile(dir)
	byteConfig, err := ioutil.ReadFile(configFile)
	if err != nil { // No config found, fine, ignore
		return false, nil
	}

	dec := json.NewDecoder(strings.NewReader(string(byteConfig)))
	if err := dec.Decode(&Config); err != nil {
		return false, err
	}

	return true, nil
}

func SaveConfig(dir string) error {

	configObject := ConfigObject{LdVersion: 1, LdType: CONFIG, LdCasPath:dir,
    	LdS3Bucket: "test", LdS3Region: "us-east-1", LdS3AccessKey: "ACCESSKEY", LdS3SecretKey: "SECRETKEY", LdMinioEndpoint: "localhost:9000", LdMinioInsecure: true }

	buf := new(bytes.Buffer)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(configObject); err != nil {
		return err
	}

	err := ioutil.WriteFile(getConfigFile(dir), buf.Bytes(), os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
