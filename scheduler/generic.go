package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// Functione encode JSON encodes and writes given object with given status.
func encode[T any](w http.ResponseWriter, status int, v T) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	return nil
}

// Function decode decodes given HTTP request body into an expected type.
func decode[T any](r *http.Request) (T, error) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		return v, fmt.Errorf("decode json: %w", err)
	}
	return v, nil
}

// Generic HTTP GET request with resp body deserialization from JSON into given
// type.
func httpGetJSON[T any](client *http.Client, url string) (*T, int, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, http.StatusBadRequest,
			fmt.Errorf("failed to perform GET request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode,
			fmt.Errorf("received non-200 response: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode,
			fmt.Errorf("failed to read response body: %w", err)
	}

	var result T
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, resp.StatusCode,
			fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return &result, resp.StatusCode, nil
}

// Send HTTP POST request with given object serialized to JSON.
func httpPost[T any](httpClient *http.Client, url string, v T) error {
	jsonInput, jErr := json.Marshal(v)
	if jErr != nil {
		return fmt.Errorf("cannot serialize input: %s",
			jErr.Error())
	}
	req, rErr := http.NewRequest("POST", url, bytes.NewBuffer(jsonInput))
	if rErr != nil {
		return fmt.Errorf("cannot create POST request: %s", rErr.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	defer req.Body.Close()

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cannot perform POST request: %s", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, rErr := io.ReadAll(resp.Body)
		if rErr != nil {
			msg := fmt.Sprintf(
				"unexpected status code: %d, "+
					"and cannot read the response body: %s",
				resp.StatusCode, rErr.Error(),
			)
			return errors.New(msg)
		}
		return fmt.Errorf("unexpected status code: %d. Body: %s",
			resp.StatusCode, string(body))
	}
	return nil
}
