package legacy_test

import (
	"testing"

	. "gopkg.in/check.v1"

	"github.com/m-lab/annotation-service/legacy"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type GeoIPSuite struct {
}

var _ = Suite(&GeoIPSuite{})

func (s *GeoIPSuite) TestOpenAndFree(c *C) {
	file := "./testdata/GeoLiteCity.dat"

	gi, err := legacy.Open(file, "GeoLiteCity.dat")

	c.Check(gi, NotNil)
	c.Check(err, IsNil)
	gi.Free()

	c.Check(gi.Check(), Equals, false)
}
