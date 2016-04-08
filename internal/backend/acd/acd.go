/*
 * Copyright 2016 Frank Wessels <fwessels@xs4all.nl>
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

// Backend connection to Amazon Cloud Drive
package acd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
	"github.com/s3git/s3git-go/internal/config"
	"os"
)

type Client struct {
	config struct {
		AccessToken  string
		RefreshToken string
		ContentUrl   string
		MetaDataUrl  string
		ClientId     string
		ClientSecret string
	}
}

func MakeClient(remote config.RemoteObject) *Client {

	client := Client{}

	client.config.AccessToken = ""
	client.config.RefreshToken = remote.AcdRefreshToken
	client.config.ContentUrl = "https://content-na.drive.amazonaws.com/cdproxy/"
	client.config.MetaDataUrl = "https://cdws.us-east-1.amazonaws.com/drive/v1/"

	// Refresh token (ignoring error, if the call fails then subsequent call that use the token will fail as well)
	client.refreshToken() // Ignore error

	// Have the token automatically refreshed every 59 minutes
	ticker := time.NewTicker(time.Second * 60 * 59)
	go func() {
		for range ticker.C {
			client.refreshToken()
		}
	}()

	return &client
}

// Upload a file to Amazon Cloud Drive
func (c *Client) UploadWithReader(hash string, r io.Reader) error {

	// Upload stream to Amazon Cloud Drive
	extraParams := map[string]string{
		"metadata": `{"name":"` + hash + `","kind":"FILE", "labels":["BLAKE2b-` + hash + `"]}`,
	}
	request, err := c.fileUploadRequest(c.config.ContentUrl+"nodes?suppress=deduplication", extraParams, "content", hash, r)
	if err != nil {
		return err
	}
	request.Header.Add("Authorization", "Bearer "+c.config.AccessToken)

	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body := &bytes.Buffer{}
	_, err = body.ReadFrom(resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func streamingUpload(params map[string]string, paramName, pseudoFilename string, body io.Writer, inputStream io.Reader) string {

	writer := multipart.NewWriter(body)

	for key, val := range params {
		_ = writer.WriteField(key, val)
	}

	part, err := writer.CreateFormFile(paramName, pseudoFilename)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return ""
	}

	if _, err = io.Copy(part, inputStream); err != nil {
		fmt.Fprint(os.Stderr, err)
		return ""
	}

	err = writer.Close()
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return ""
	}

	return writer.FormDataContentType()
}

// Creates a new file upload http request with optional extra params
func (c *Client) fileUploadRequest(uri string, params map[string]string, paramName, pseudoFilename string, inputStream io.Reader) (*http.Request, error) {

	body := &bytes.Buffer{}
	contentType := streamingUpload(params, paramName, pseudoFilename, body, inputStream)

	request, _ := http.NewRequest("POST", uri, body)
	request.Header.Set("Content-Type", contentType)

	return request, nil
}

// Create a folder at Amazon Cloud Drive
func (c *Client) CreateFolder(folder string) error {

	bodystr := `{"name":"` + folder + `", "kind":"FOLDER"}`
	req, _ := http.NewRequest("POST", c.config.MetaDataUrl+"nodes", strings.NewReader(bodystr))
	req.Header.Set("Content-Type", "multipart/form-data")
	req.Header.Add("Authorization", "Bearer "+c.config.AccessToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body := &bytes.Buffer{}
	_, err = body.ReadFrom(resp.Body)
	if err != nil {
		return err
	}

	return nil
}

// Verify the existence of a hash in Amazon Cloud Drive
func (c *Client) VerifyHash(hash string) (bool, error) {

	req, err := http.NewRequest("GET", c.config.MetaDataUrl+"nodes?filters=labels:BLAKE2b-"+hash, nil)
	req.Header.Add("Authorization", "Bearer "+c.config.AccessToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	var lst listStruct
	if err := dec.Decode(&lst); err != nil {
		return false, err
	}

	return lst.Count != 0, nil
}

// List with a prefix string in Amazon Cloud Drive
func (c *Client) List(_ string, action func(key string)) ([]string, error) {

	return nil, errors.New("To be implemented")
}

func (c *Client) DownloadWithWriter(_ string, _ io.WriterAt) error {

	return errors.New("To be implemented")
}

// Get endpoint urls for Amazon Cloud Drive (both for content and meta data access)
func (c *Client) GetEndpoint() error {
	req, err := http.NewRequest("GET", "https://drive.amazonaws.com/drive/v1/account/endpoint", nil)
	req.Header.Add("Authorization", "Bearer "+c.config.AccessToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.Status != "200 OK" {
		return errors.New("Unsuccessful response for getting endpoint")
	}

	dec := json.NewDecoder(resp.Body)
	var ep endpointStruct
	if err := dec.Decode(&ep); err != nil {
		panic(err)
	}

	c.config.ContentUrl = ep.ContentUrl
	c.config.MetaDataUrl = ep.MetaDataUrl

	return nil
}

// Refresh the token for Amazon Cloud Drive (valid for the next hour)
func (c *Client) refreshToken() error {
	url := "https://api.amazon.com/auth/o2/token"
	body := "grant_type=refresh_token"
	body += "&refresh_token=" + c.config.RefreshToken
	body += "&client_id=" + c.config.ClientId
	body += "&client_secret= " + c.config.ClientSecret

	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	var rt refreshToken_struct
	if err := dec.Decode(&rt); err != nil {
		return err
	}

	c.config.AccessToken = rt.AccessToken
	return nil
}

type refreshToken_struct struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
}

type endpointStruct struct {
	ContentUrl  string `json:"contentUrl"`
	MetaDataUrl string `json:"metadataUrl"`
}

type listStruct struct {
	Count int `json:"count"`
}
