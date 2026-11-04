//go:build !seams

package raftstore

// seamsBuild reports whether this binary was built with the seams tag.
const seamsBuild = false

// armedSeams holds runtime boundaries in the seams build; empty here.
type armedSeams struct{} //nolint:unused // Node.armed; filled in the seams build
