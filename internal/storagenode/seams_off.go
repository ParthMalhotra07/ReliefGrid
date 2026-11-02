//go:build !seams

package storagenode

import "net/http"

// registerSeamRoutes adds nothing: the /mgmt/test/seams routes exist only
// in the seams build (seams_on.go).
func registerSeamRoutes(*http.ServeMux, *server) {}
