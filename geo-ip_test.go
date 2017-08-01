package annotator

import (
	"os"
	"reflect"
	"testing"
)

//tests correct parsing of createList
func TestCreateList(t *testing.T) {
	r, _ := os.Open("testdata/sample.csv")

	list, _ := createList(r)

	var listComp = []Node{
		Node{
			lowRangeBin:  "1.0.1.0",
			highRangeBin: "1.0.3.255",
			lowRangeNum:  16777472,
			highRangeNum: 16778239,
			countryAbrv:  "CN",
			countryName:  "China",
		},
		Node{
			lowRangeBin:  "1.0.4.0",
			highRangeBin: "1.0.7.255",
			lowRangeNum:  16778240,
			highRangeNum: 16779263,
			countryAbrv:  "AU",
			countryName:  "Australia",
		},
		Node{
			lowRangeBin:  "1.0.8.0",
			highRangeBin: "1.0.15.255",
			lowRangeNum:  16779264,
			highRangeNum: 16781311,
			countryAbrv:  "CN",
			countryName:  "China",
		},
		Node{
			lowRangeBin:  "1.0.16.0",
			highRangeBin: "1.0.31.255",
			lowRangeNum:  16781312,
			highRangeNum: 16785407,
			countryAbrv:  "JP",
			countryName:  "Japan",
		},
	}

	if !reflect.DeepEqual(list, listComp) {
		t.Errorf("lists are not equal.\n")
	}
}
