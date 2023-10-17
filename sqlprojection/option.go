package sqlprojection

// An Option configures the optional behavior of an SQL projection.
type Option struct {
	applyToCandidateSet func(*candidateSet)
}
