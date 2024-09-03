package gard

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient() (*Client, error) {
	// TODO: Configure these options based on your Gard setup
	return &Client{
		baseURL: "http://localhost:8081",
		httpClient: &http.Client{
			Timeout: time.Second * 30,
		},
	}, nil
}

func (c *Client) Close() error {
	// No need to close the http.Client
	return nil
}

func (c *Client) UploadFile(filename string, reader io.Reader) error {
	url := fmt.Sprintf("%s/upload/%s", c.baseURL, filename)
	req, err := http.NewRequest("POST", url, reader)
	if err != nil {
		return fmt.Errorf("failed to create upload request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload failed with status: %s", resp.Status)
	}

	return nil
}

func (c *Client) DownloadFile(filename string, writer io.Writer) error {
	url := fmt.Sprintf("%s/download/%s", c.baseURL, filename)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed with status: %s", resp.Status)
	}

	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write downloaded file: %w", err)
	}

	return nil
}

func (c *Client) SendDownloadSpeed(speed float64) error {
	url := fmt.Sprintf("%s/speed?value=%f", c.baseURL, speed)
	resp, err := c.httpClient.Post(url, "application/json", nil)
	if err != nil {
		return fmt.Errorf("failed to send download speed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send download speed with status: %s", resp.Status)
	}

	return nil
}