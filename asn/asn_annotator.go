package asn

import (
	"errors"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/iputils"
)

var (
	maxErrorCountPerFile = 50 // the maximum allowed error per import file

	timeComponentsFromFileNameRegex = regexp.MustCompile(`.*(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2}).*`) // the regex, which helps to extract the time from the file name

	// ErrorIllegalIPNodeType raised when the ASNDataset contains IPNode which is not an ASNIPNode
	ErrorIllegalIPNodeType = errors.New("Illegal IPNode type found")
)

var lastLogTime = time.Time{}

// Annotate expects an IP string and an api.GeoData pointer to find the ASN
// and populate the data into the GeoData.ASN struct
func (asn *ASNDataset) Annotate(ip string, ann *api.GeoData) error {
	if asn == nil {
		return errors.New("ErrNilASNDataset") // TODO
	}
	if ann.ASN != nil {
		return errors.New("ErrAlreadyPopulated") // TODO
	}

	ipNodeGetter := func(idx int) iputils.IPNode {
		return &asn.IPList[idx]
	}

	node, err := iputils.SearchBinary(ip, len(asn.IPList), ipNodeGetter)
	if err != nil {
		// ErrNodeNotFound is super spammy - 10% of requests, so suppress those.
		if err != iputils.ErrNodeNotFound {
			// Horribly noisy now.
			if time.Since(lastLogTime) > time.Minute {
				log.Println(err, ip)
				lastLogTime = time.Now()
			}
		}
		//TODO metric here
		return err
	}

	asnNode, ok := node.(*ASNIPNode)
	if !ok {
		return ErrorIllegalIPNodeType
	}

	result := []api.ASNElement{}

	// split the set on comas
	for _, asn := range strings.Split(asnNode.ASNString, ",") {
		// split the set elements on _ (multi-origin ASNs)
		asnList := strings.Split(asn, "_")
		newElement := api.ASNElement{ASNList: asnList, ASNListType: api.ASNSingle}
		if len(asnList) > 1 {
			newElement.ASNListType = api.ASNMultiOrigin
		}
		result = append(result, newElement)
	}
	ann.ASN = result
	return nil
}

// AnnotatorDate The date associated with the dataset.
func (asn *ASNDataset) AnnotatorDate() time.Time {
	return asn.Start
}