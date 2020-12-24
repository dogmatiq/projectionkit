package patchprojection

// State represents the full state of a projection.
type State interface {
	ApplyPatch(Patch)
}

// Patch represents an incremental change to the state of a projection.
type Patch interface {
}
