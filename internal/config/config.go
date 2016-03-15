package config

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
)

const S3GIT_CONFIG = ".s3git.config"
const CONFIG = "config"

var Config ConfigObject

type ConfigObject struct {
	Version         int            `json:"s3gitVersion"`
	Type            string         `json:"s3gitType"` // config
	CasPath         string         `json:"s3gitCasPath"`
	RollingHashBits int            `json:"s3gitRollingHashBits"`
	RollingHashMin  int            `json:"s3gitRollingHashMin"`
	Remotes         []RemoteObject `json:"s3gitRemotes"`
}

type RemoteObject struct {
	Name		string `json:"Name"`
	Hydrate     bool   `json:"Hydrate"`
	S3Bucket    string `json:"S3Bucket"`
	S3Region    string `json:"S3Region"`
	S3AccessKey string `json:"S3AccessKey"`
	S3SecretKey string `json:"S3SecretKey"`

	MinioEndpoint string `json:"MinioEndpoint"`
	MinioInsecure bool   `json:"MinioInsecure"`

	AcdRefreshToken string `json:"AcdRefreshToken"`
}

func getConfigFile(dir string) string {
	return dir + "/" + S3GIT_CONFIG
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

	bucket := getDefaultValue("test", "S3GIT_S3_BUCKET")
	region := getDefaultValue("us-east-1", "S3GIT_S3_REGION")
	accessKey := getDefaultValue("", "S3GIT_S3_ACCESS_KEY")
	secretKey := getDefaultValue("", "S3GIT_S3_SECRET_KEY")

	configObject := ConfigObject{Version: 1, Type: CONFIG, CasPath: dir}
	configObject.Remotes = append(configObject.Remotes, RemoteObject{Name: "primary", S3Bucket: bucket, S3Region: region, S3AccessKey: accessKey, S3SecretKey: secretKey, MinioEndpoint: "localhost:9000", MinioInsecure: true})

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

func getDefaultValue(def, envName string) string {

	val := def

	envVal := os.Getenv(envName)
	if envVal != "" {
		val = envVal
	}
	return val
}
