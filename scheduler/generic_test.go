package scheduler

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

type point struct {
	X int     `json:"x"`
	Y float64 `json:"y"`
}

func TestEncode(t *testing.T) {
	tests := []struct {
		name         string
		inputStatus  int
		inputObject  interface{}
		expectedJSON string
		expectedCode int
	}{
		{
			name:         "Test Encoding a Simple Object",
			inputStatus:  http.StatusOK,
			inputObject:  map[string]string{"key": "value"},
			expectedJSON: `{"key":"value"}` + "\n",
			expectedCode: http.StatusOK,
		},
		{
			name:        "Test Encoding a Complex Object",
			inputStatus: http.StatusCreated,
			inputObject: struct {
				Name string `json:"name"`
			}{Name: "GoLang"},
			expectedJSON: `{"name":"GoLang"}` + "\n",
			expectedCode: http.StatusCreated,
		},
		{
			name:         "Test Encoding list of numbers",
			inputStatus:  http.StatusCreated,
			inputObject:  []int{42, 123, 52},
			expectedJSON: `[42,123,52]` + "\n",
			expectedCode: http.StatusCreated,
		},
		{
			name:        "Test Encoding list of structs",
			inputStatus: http.StatusCreated,
			inputObject: []point{
				{42, 3.14},
				{-13, 43.43},
			},
			expectedJSON: `[{"x":42,"y":3.14},{"x":-13,"y":43.43}]` + "\n",
			expectedCode: http.StatusCreated,
		},
		{
			name:         "Test Encoding with Empty Object",
			inputStatus:  http.StatusNoContent,
			inputObject:  struct{}{},
			expectedJSON: `{}` + "\n",
			expectedCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			err := encode(rr, tt.inputStatus, tt.inputObject)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			contentType := rr.Header().Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("Expected Content-Type application/json, got %v",
					contentType)
			}
			if status := rr.Code; status != tt.expectedCode {
				t.Errorf("Expected status code %v, got %v", tt.expectedCode,
					status)
			}
			if body := rr.Body.String(); body != tt.expectedJSON {
				t.Errorf("Expected JSON %v, got %v", tt.expectedJSON, body)
			}
		})
	}
}

func TestDecodeIntoMap(t *testing.T) {
	tests := []struct {
		name          string
		inputBody     string
		expectedValue map[string]string
		expectedError bool
	}{
		{
			name:          "Decode Simple Object",
			inputBody:     `{"key":"value","x":"y"}`,
			expectedValue: map[string]string{"key": "value", "x": "y"},
			expectedError: false,
		},
		{
			name:          "Decode Invalid JSON",
			inputBody:     `{"name": "GoLang",}`,
			expectedValue: map[string]string{},
			expectedError: true,
		},
		{
			name:          "Decode Empty Body",
			inputBody:     ``,
			expectedValue: map[string]string{},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new request with the specified body
			req := httptest.NewRequest("POST", "/", bytes.NewBufferString(tt.inputBody))

			var result map[string]string
			result, err := decode[map[string]string](req)
			if (err != nil) != tt.expectedError {
				t.Fatalf("Expected error: %v, got: %v", tt.expectedError, err)
			}
			if !tt.expectedError && !equalMaps(result, tt.expectedValue) {
				t.Errorf("Expected: %v, got: %v", tt.expectedValue, result)
			}
		})
	}
}

// Helper function to compare two maps
func equalMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
