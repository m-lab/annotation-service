package parser_test

import (
	"os"
	"reflect"
	"testing"
	"net"

	"github.com/m-lab/annotation-service/parser"
)


//tests correct parsing of createList
func TestCreateListIPv4(t *testing.T) {
	r, _ := os.Open("testdata/sample.csv")
	list, _ := parser.CreateList(r)
	var listComp = []parser.Node{
		parser.Node{
			net.IP{1,0,1,0},
			net.ParseIP("1.0.3.255"),
			"CN",
			"China",
		},
		parser.Node{
			net.ParseIP("1.0.4.0"),
			net.ParseIP("1.0.7.255"),
			"AU",
			"Australia",
		},
		parser.Node{
			net.ParseIP("1.0.8.0"),
			net.ParseIP("1.0.15.255"),
			"CN",
			"China",
		},
		parser.Node{
			net.ParseIP("1.0.16.0"),
			net.ParseIP("1.0.31.255"),
			16781312,
			16785407,
			"JP",
			"Japan",
		},
	}
	if !reflect.DeepEqual(list, listComp) {
		t.Errorf("lists are not equal.\n")
	}
}
/*func TestCreateListIPv6(t *testing.T) {
	r, _ := os.Open("testdata/IPv6SAAMPLE.csv")
	list, _ := parser.CreateList(r)
	var listComp = []parser.Node{
		parser.Node{
			"2001:5::",
			"2001:5:ffff:ffff:ffff:ffff:ffff:ffff",
			0,
			0,
			//42540488558116655331872044393019998208,
			//42540488637344817846136381986563948543,
			"EU",
		},
		parser.Node{
			"2001:200::",
			"2001:200:ffff:ffff:ffff:ffff:ffff:ffff",
			42540528726795050063891204319802818560,
			42540528806023212578155541913346768895,
			"JP",
		},
		parser.Node{
			"2001:208::",
			"2001:208:ffff:ffff:ffff:ffff:ffff:ffff",
			42540529360620350178005905068154421248,
			42540529439848512692270242661698371583,
			"SG",
		},
		parser.Node{
			"2001:218::",
			"2001:218:ffff:ffff:ffff:ffff:ffff:ffff",
			42540530628270950406235306564857626624,
			42540530707499112920499644158401576959,
			"JP",
		},
	}
	if !reflect.DeepEqual(list, listComp) {
		t.Errorf("lists are not equal.\n")
	}
}*/

