package domain

// DrainingError indicates the instance is draining and is not accepting new
// queries. Transports match it via errors.AsType and map it to a 503 (REST) /
// Unavailable (gRPC) status so orchestrators can retry on another instance.
type DrainingError struct{}

func (DrainingError) Error() string {
	return "service is draining: new queries are not accepted"
}

// ReloadReport summarizes the database changes applied during a config reload.
type ReloadReport struct {
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
	Updated []string `json:"updated"`
}
