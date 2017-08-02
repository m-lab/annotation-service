package parser_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/m-lab/annotation-service/parser"
)


//tests correct parsing of createList
func TestCreateList(t *testing.T) {
	r, _ := os.Open("testdata/sample.csv")
	list, _ := parser.CreateList(r)
	var listComp = []parser.Node{
		parser.Node{
			"1.0.1.0",
			"1.0.3.255",
			16777472,
			16778239,
			"CN",
			"China",
		},
		parser.Node{
			"1.0.4.0",
			"1.0.7.255",
			16778240,
			16779263,
			"AU",
			"Australia",
		},
		parser.Node{
			"1.0.8.0",
			"1.0.15.255",
			16779264,
			16781311,
			"CN",
			"China",
		},
		parser.Node{
			"1.0.16.0",
			"1.0.31.255",
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
