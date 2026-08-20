package domain

// DrainingError indicates the instance is draining and is not accepting new
// queries. Transports match it via errors.AsType and map it to a 503 (REST) /
// Unavailable (gRPC) status so orchestrators can retry on another instance.
type DrainingError struct{}

func (DrainingError) Error() string {
	return "service is draining: new queries are not accepted"
}

// ValidationError marks a client-side input error. Transports match it via
// errors.AsType and map it to 400 (REST) / InvalidArgument (gRPC).
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return "invalid " + e.Field + ": " + e.Reason
}

// NotFoundError marks a reference to something that does not exist. Transports
// map it to 404 (REST) / NotFound (gRPC).
type NotFoundError struct {
	Resource string
	ID       string
}

func (e NotFoundError) Error() string {
	return e.Resource + " " + e.ID + " not found"
}

// UnavailableError marks a dependency that is temporarily unreachable. The
// message deliberately carries no driver text: a DSN with host, user and
// connection parameters must not reach the client. Transports map it to
// 503 (REST) / Unavailable (gRPC).
type UnavailableError struct {
	Resource string
}

func (e UnavailableError) Error() string {
	return e.Resource + " is unreachable"
}

// ResourceExhaustedError marks a request rejected by a capacity limit rather
// than by its own contents. Transports map it to 429 (REST) /
// ResourceExhausted (gRPC).
type ResourceExhaustedError struct {
	Reason string
}

func (e ResourceExhaustedError) Error() string {
	return "resource exhausted: " + e.Reason
}

// ReloadReport summarizes the database changes applied during a config reload.
type ReloadReport struct {
	Added    []string `json:"added"`
	Removed  []string `json:"removed"`
	Updated  []string `json:"updated"`
	Ignored  []string `json:"ignored,omitempty"`
	Failures []string `json:"failures,omitempty"`
}
