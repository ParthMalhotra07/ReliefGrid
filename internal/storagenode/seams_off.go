//go:build !seams

package storagenode

import "net/http"

// seamsBuild reports whether this binary was built with the seams tag.
const seamsBuild = false

// seamRegistry holds armed boundaries in the seams build; empty here.
type seamRegistry struct{} //nolint:unused // server.seams; filled in the seams build

// registerSeamRoutes adds nothing: the /mgmt/test/seams routes exist only
// in the seams build (seams_on.go).
func registerSeamRoutes(*http.ServeMux, *server) {}
