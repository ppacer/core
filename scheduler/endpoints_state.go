package scheduler

import (
	"encoding/json"
	"net/http"
)

// HTTP handler for getting the current Scheduler State.
func (s *Scheduler) currentState(w http.ResponseWriter, _ *http.Request) {
	s.Lock()
	state := s.state
	s.Unlock()

	forJson := struct {
		StateString string `json:"state"`
	}{state.String()}

	stateJson, jsonErr := json.Marshal(forJson)
	if jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(stateJson))
}
