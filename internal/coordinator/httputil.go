package coordinator

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// retryAfter is the Retry-After (seconds) on every 503 the coordinator
// writes itself.
const retryAfter = "1"

// maxBody bounds a request or storage-node answer the coordinator buffers.
const maxBody = 4 << 20

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "err", err)
	}
}

// writeError writes a non-retryable error that was never proposed.
func writeError(w http.ResponseWriter, status int, code internalapi.ErrorCode, msg string) {
	writeJSON(w, status, internalapi.Error{Code: code, Message: msg})
}

// writeUnavailable writes a retryable 503. unknown is set only for a
// mutation that may have been proposed (OUTCOME_UNKNOWN); anything else is
// NO_LEADER: nothing was proposed, or the call was a read.
func writeUnavailable(w http.ResponseWriter, unknown bool, msg string) {
	w.Header().Set("Retry-After", retryAfter)
	code := internalapi.NOLEADER
	if unknown {
		code = internalapi.OUTCOMEUNKNOWN
	}
	writeJSON(w, http.StatusServiceUnavailable, internalapi.Error{Code: code, Message: msg, Retryable: true, OutcomeUnknown: unknown})
}

func writeRingMismatch(w http.ResponseWriter, have, got uint64) {
	writeJSON(w, http.StatusConflict, internalapi.Error{Code: internalapi.RINGVERSIONMISMATCH, Retryable: true, RingVersion: &have,
		Message: "ringVersion " + strconv.FormatUint(got, 10) + " does not match the coordinator's " + strconv.FormatUint(have, 10)})
}

// hopByHop headers are never copied between a caller and a storage node.
var hopByHop = []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Content-Length"}

func forwardHeader(h http.Header) http.Header {
	out := h.Clone()
	for _, k := range hopByHop {
		out.Del(k)
	}
	return out
}
