package annotator

import (
	"fmt"
	"strconv"
)

type TreeNode struct {
	left  *TreeNode
	right *TreeNode
	value *Node
	bin   int
}

//converts decimal to a int64 binary num
func createBin(ipDec int) string {
	i64 := int64(ipDec)
	return strconv.FormatInt(i64, 2)



//change to root = root.insert
func (root *TreeNode) insert(v *Node) *TreeNode {
	if root == nil{
		fmt.Println("no existing root, starting fresh.\n") 
		return insertLeafNode(nil,nil,v,-1)
	}else{
		fmt.Println("existing root.. calling normal insertion\n")
		return insertLeafNode(nil,root,v,-1)
	}
}
func insertLeafNode(parent *TreeNode, root *TreeNode, v *Node, bin int) *TreeNode {
	if root == nil {
		fmt.Println("start to make a leaf\n")
		tn := &TreeNode{nil, nil, v, -1}

		//first node insertion
		if parent == nil {
			fmt.Println("new root\n") 
			root = &TreeNode{nil, nil, v, bin}

			if root == nil {
				fmt.Println("new root is null?\n")
			}
		} else if bin == 0 {
			fmt.Println("switch with left\n") 
			root = &TreeNode{tn, parent, v, bin}
		} else if bin == 1 {
			fmt.Println("switch with right\n")
			root = &TreeNode{parent, tn, v,  bin}
		}
		if root == nil{
			fmt.Println("bad -1\n")
		}
		fmt.Println("leaf inserted\n")

	} else {
		fmt.Println("traverse: \n")
		switch {
		case v.low2Bin[index] < root.value.low2Bin[index]:
			fmt.Println("left case\n") 
			root = insertLeafNode(root, root.left, v, 0)
		case v.high2Bin[index] > root.value.high2Bin[index]:
			fmt.Println("right case\n") 
			root = insertLeafNode(root, root.right, v, 1)
		}
	}

	if root == nil{
		fmt.Println("bad root\n")
	}
	return root
}
