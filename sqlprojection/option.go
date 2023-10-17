package sqlprojection

// An Option configures the optional behavior of an SQL projection.
type Option struct {
	applyToAdaptor      func(*adaptor)
	applyToCandidateSet func(*candidateSet)
}
