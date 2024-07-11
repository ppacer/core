package dag

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestNewNodeWithConfig(t *testing.T) {
	var task Task
	n1 := NewNode(task)

	if n1.Config != DefaultTaskConfig {
		t.Errorf("Default case should have default config, but has %+v",
			n1.Config)
	}

	newTimeout := 30 * time.Second
	n2 := NewNode(task, WithTaskTimeout(newTimeout))
	if n2.Config.TimeoutSeconds != 30.0 {
		t.Errorf("Expected %v timeout, but got: %f.2 timeout seconds",
			newTimeout, n2.Config.TimeoutSeconds)
	}

	newRetries := 1
	n3 := NewNode(task, WithTaskRetries(newRetries))
	if n3.Config.Retries != newRetries {
		t.Errorf("Expected %d retires, but got: %d",
			newRetries, n3.Config.Retries)
	}

	n4 := NewNode(task, WithTaskSendAlertOnRetries)
	if !n4.Config.SendAlertOnRetry {
		t.Error("Expected to send alerts on retires for this node")
	}

	n5 := NewNode(task, WithTaskNotSendAlertsOnFailures)
	if n5.Config.SendAlertOnFailure {
		t.Error("Expected to not send alerts on failures for this node")
	}

	delay := 100 * time.Millisecond
	n6 := NewNode(task, WithTaskRetriesDelay(delay))
	if n6.Config.RetriesDelaySeconds != 0.1 {
		t.Errorf("Expected 0.1s RetriesDelaySeconds, but got: %f.2",
			n6.Config.RetriesDelaySeconds)
	}
}

func TestDefaultTaskConfig(t *testing.T) {
	if DefaultTaskConfig.TimeoutSeconds < 10*60 {
		t.Error("DefaultTaskConfig.TimeoutSeconds should be at least an hour")
	}
	if DefaultTaskConfig.Retries != 0 {
		t.Error("DefaultTaskConfig.Retries should be set to 0")
	}
	if DefaultTaskConfig.SendAlertOnRetry {
		t.Error("By default ppacer shouldn't send alerts on retries")
	}
	if !DefaultTaskConfig.SendAlertOnFailure {
		t.Error("By default ppacer should send alerts on failures")
	}
}

func TestDefaultTaskConfigSerialization(t *testing.T) {
	jsonBytes, jErr := json.Marshal(DefaultTaskConfig)
	if jErr != nil {
		t.Errorf("Error while serializing into JSON: %s", jErr.Error())
	}
	if len(jsonBytes) == 0 {
		t.Error("Expected non-empty JSON")
	}
	const timestampPhrase = `"timeoutSeconds":`
	timeoutInJson := bytes.Contains(jsonBytes, []byte(timestampPhrase))
	if !timeoutInJson {
		t.Errorf("Expected phares [%s] in serialized JSON, but not found in %s",
			timestampPhrase, string(jsonBytes))
	}
}
