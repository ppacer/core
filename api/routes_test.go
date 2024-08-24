package api

import (
	"testing"
)

func TestRoutesContainsAllEndpoints(t *testing.T) {
	expectedEndpoints := []EndpointID{
		EndpointDagTaskPop,
		EndpointDagTaskUpdate,
		EndpointState,
		EndpointUiDagrunStats,
		EndpointUiDagrunLatest,
		EndpointUiDagrunDetails,
	}

	routes := Routes()

	for _, endpointId := range expectedEndpoints {
		if _, exist := routes[endpointId]; !exist {
			t.Errorf("Expected endpointId %d does not exist in Routes",
				endpointId)
		}
	}
}
