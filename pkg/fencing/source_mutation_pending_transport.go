package fencing

import (
	"net/http"
	"net/url"
)

func sourceMutationEndpointPath(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return parsed.Path
}

func sourceMutationPendingResponse(response *http.Response, path string) error {
	if response == nil || response.StatusCode != http.StatusAccepted || !sourceMutationPendingReasonEndpoint(path) {
		return ErrPending
	}
	reasons := response.Header.Values(MutationPendingReasonHeader)
	if len(reasons) != 1 || reasons[0] != MutationPreDispatchContentionReason {
		return ErrPending
	}
	return ErrMutationPreDispatchContention
}

func sourceMutationPendingReasonEndpoint(path string) bool {
	return path == "/v1/source-mutations/execute" || path == "/v1/source-mutations/admit"
}
