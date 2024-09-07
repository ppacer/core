package scheduler

import (
	"fmt"
	"net/http"
	"strconv"
)

// getPathValueInt parses URL path argument and casts it into integer.
func getPathValueInt(r *http.Request, argName string) (int, error) {
	argValue := r.PathValue(argName)
	if argValue == "" {
		return -1, fmt.Errorf("parameter %s is unexpectedly empty", argName)
	}
	argInt, castErr := strconv.Atoi(argValue)
	if castErr != nil {
		return -1, fmt.Errorf("cannot cast parameter %s (%s) into integer",
			argName, argValue)
	}
	return argInt, nil
}

// getPathValueStr parses URL path argument and checks if it's not empty.
func getPathValueStr(r *http.Request, argName string) (string, error) {
	argValue := r.PathValue(argName)
	if argValue == "" {
		return "", fmt.Errorf("parameter %s is unexpectedly empty", argName)
	}
	return argValue, nil
}
