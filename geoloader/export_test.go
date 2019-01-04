package geoloader

// These are exported for testing.
var NewDirectory = newDirectory

/*
// Make a public version of AnnWrapper for testing.
type AnnWrapper struct {
	annWrapper
}

func (ae *AnnWrapper) UpdateLastUsed() {
	ae.updateLastUsed()
}
func (ae *AnnWrapper) GetLastUsed() time.Time {
	return ae.getLastUsed()
}
func (ae *AnnWrapper) Status() error {
	return ae.status()
}
func (ae *AnnWrapper) ReserveForLoad() bool {
	return ae.reserveForLoad()
}
func (ae *AnnWrapper) SetAnnotator(ann api.Annotator, err error) error {
	return ae.setAnnotator(ann, err)
}
func (ae *AnnWrapper) GetAnnotator() (ann api.Annotator, err error) {
	return ae.getAnnotator()
}
func (ae *AnnWrapper) Unload() {
	ae.unload()
}
func NewAnnWrapper() AnnWrapper {
	return AnnWrapper{newAnnWrapper()}
}
*/
