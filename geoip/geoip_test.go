package geoip_test

import (
	"testing"

	. "gopkg.in/check.v1"

	"github.com/m-lab/annotation-service/handler/geoip"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type GeoIPSuite struct {
}

var _ = Suite(&GeoIPSuite{})

func (s *GeoIPSuite) TestOpenAndFree(c *C) {
	file := "./ex/GeoLiteCity.dat"

	gi, err := geoip.Open(file, "GeoLiteCity.dat")

	c.Check(gi, NotNil)
	c.Check(err, IsNil)
	gi.Free()

	c.Check(gi.Check(), Equals, false)
}
