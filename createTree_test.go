package annotator

import (
	"testing"
	"fmt"
)

func Test_createBin(t *testing.T) {
	str :=  createBin(142) 
	fmt.Println("%v %T",str,str) 
} 
func Test_insertion(t* testing.T){
	/*var listComp = []Node{
                Node{
                        lowRangeBin:  "1.0.1.0",
                        highRangeBin: "1.0.3.255",
                        lowRangeNum:  16777472,
                        highRangeNum: 16778239,
                        countryAbrv:  "CN",
                        countryName:  "China",
                        low2Bin: "1000000000000000100000000", 
                        high2Bin: "1000000000000001111111111",
                },
                Node{
                        lowRangeBin:  "1.0.4.0",
                        highRangeBin: "1.0.7.255",
                        lowRangeNum:  16778240,
                        highRangeNum: 16779263,
                        countryAbrv:  "AU",
                        countryName:  "Australia",
                        low2Bin:"1000000000000010000000000",
                        high2Bin:"1000000000000011111111111",
                },
                Node{
                        lowRangeBin:  "1.0.8.0",
                        highRangeBin: "1.0.15.255",
                        lowRangeNum:  16779264,
                        highRangeNum: 16781311,
                        countryAbrv:  "CN",
                        countryName:  "China",
                        low2Bin: "1000000000000100000000000",
                        high2Bin: "1000000000000111111111111",
                },
                Node{
                        lowRangeBin:  "1.0.16.0",
                        highRangeBin: "1.0.31.255",
                        lowRangeNum:  16781312,
                        highRangeNum: 16785407,
                        countryAbrv:  "JP", 
                        countryName:  "Japan",
                        low2Bin:"1000000000001000000000000",
                        high2Bin: "1000000000001111111111111",
                },
        }*/

	var listComp2 = []Node{
                Node{
                        lowRangeBin:  "1.0.1.0",
                        highRangeBin: "1.0.3.255",
                        lowRangeNum:  1,
                        highRangeNum: 3,
                        countryAbrv:  "CN",
                        countryName:  "China",
                        low2Bin: "1", 
                        high2Bin: "11",
                },
                Node{
                        lowRangeBin:  "1.0.4.0",
                        highRangeBin: "1.0.7.255",
                        lowRangeNum:  4,
                        highRangeNum: 7,
                        countryAbrv:  "AU",
                        countryName:  "Australia",
                        low2Bin:"100",
                        high2Bin:"111",
                },
                Node{
                        lowRangeBin:  "1.0.8.0",
                        highRangeBin: "1.0.15.255",
                        lowRangeNum:  10,
                        highRangeNum: 16,
                        countryAbrv:  "CN",
                        countryName:  "China",
                        low2Bin: "1010",
                        high2Bin: "1000",
                },
                Node{
                        lowRangeBin:  "1.0.16.0",
                        highRangeBin: "1.0.31.255",
                        lowRangeNum:  20,
                        highRangeNum: 25,
                        countryAbrv:  "JP", 
                        countryName:  "Japan",
                        low2Bin:"10100",
                        high2Bin: "11001",
                },
        }
	var root *TreeNode 
	root = root.insert(&listComp2[0]) 
	if root == nil {
		t.Errorf("root is nil\n") 
	}
	tn := TreeNode{nil,nil,&listComp2[0],-1}
	if root.left != tn.left || root.right != tn.right || root.value != tn.value || root.bin != tn.bin {
		t.Error("bad root insert\n")
	}
	fmt.Println("- - - - - TEST 1 ROOT INSERT PASS\n") 
	root = root.insert(&listComp2[1])
	if root == nil{
		t.Errorf("bad 2nd insert: root is nil\n")
	}
	if root.left == nil || root.right == nil{
		t.Errorf("bad 2nd insert: child is nil\n") 
	}
	if root.left.value != &listComp2[0] && root.right.value != &listComp2[1] {
		t.Error("bad 2nd node inser\n")
	}
	fmt.Println("- - - - - TEST 2 ROOT INSERT PASS\n") 
	root = root.insert(&listComp2[3]) 
	root = root.insert(&listComp2[2])

}

