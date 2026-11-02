//go:build seams

package storagenode

import "net/http"

// registerSeamRoutes adds the test-seam routes (S4.6, #88) to the mgmt
// mux. Operator token required:
//
//	POST /mgmt/test/seams                 {kind, operationIds, timeoutMs} → 201 {id, kind, state}
//	GET  /mgmt/test/seams/{id}            {id, kind, state, point, appended}
//	POST /mgmt/test/seams/{id}/release    → the seam's status
//	POST /mgmt/test/seams/{id}/abort      → the seam's status
func registerSeamRoutes(mux *http.ServeMux, s *server) {}
