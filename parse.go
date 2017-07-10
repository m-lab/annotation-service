package main

import (
	"bufio"
	"encoding/csv"
	"os"
	"fmt"
	"io"
	"strconv"
)

type Node struct{
	lowRangeBin string
	highRangeBin string
	lowRangeNum int 
	highRangeNum int 
	countryAbrv string 
	countryName string
}
func search(){
	list := createList() 
	*Node = searchList(list)
}
func createList() []*Node {
	//fmt.Println(len(os.Args),os.Args) 
	
	list  := []*Node{}

	f, _ := os.Open("GeoIPCountryWhois.csv") 

	r := csv.NewReader(bufio.NewReader(f))
	r.TrimLeadingSpace = true
	for{
		record, err := r.Read()
		if err == io.EOF{
			break
		}
		

		//fmt.Println(record) 
		//fmt.Println(len(record))

		newNode := new(Node) 

		for value := range record{
			//fmt.Printf(" %v\n", record[value])
			
			//GO enum version of this? 
			if value == 0 {
				newNode.lowRangeBin = record[value]	
			}else if value == 1 {
				newNode.highRangeBin = record[value]
			}else if value ==  2{
				temp, err := strconv.Atoi(record[value])
				if err != nil{
					break 
				}
				newNode.lowRangeNum = temp
			}else if value == 3{
				temp, err := strconv.Atoi(record[value])
				if err != nil{
					break 
				}
				newNode.highRangeNum = temp
			}else if value == 4{
				newNode.countryAbrv = record[value]
			}else if value == 5{
				newNode.countryName = record[value]
				list = append(list,newNode)
				//fmt.Printf("  %v\n",newNode)

			}
		}

	}

	return list 
}
func searchList(list []*Node) *Node{
	
	userIp,err := strconv.Atoi(os.Args[2])
	if err != nil{
		return nil
	}
	for value := range list{
		if userIp >= list[value].lowRangeNum &&  userIp <= list[value].highRangeNum {
			fmt.Println("FOUND:",os.Args[2],"\n",list[value])
			return list[value]
		}
	}
	return nil
}
