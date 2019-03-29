package iputils

import (
	"encoding/csv"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBuildIPNodeList
func TestBuildIPNodeList(t *testing.T) {
	inputCSV := `1.0.0.0/24	custom1
1.0.0.2/26	custom2
1.0.10.0/24	custom3
1.0.10.124/30	custom4
2.1.0.0/8	cuustom5`

	expectedResult := []IPNode{
		toIPNodeWithProp(t, "1.0.0.0", "1.0.0.1", "custom1"),
		toIPNodeWithProp(t, "1.0.0.2", "1.0.0.63", "custom2"),
		toIPNodeWithProp(t, "1.0.0.64", "1.0.0.255", "custom1"),
		toIPNodeWithProp(t, "1.0.10.0", "1.0.10.123", "custom3"),
		toIPNodeWithProp(t, "1.0.10.124", "1.0.10.127", "custom4"),
		toIPNodeWithProp(t, "1.0.10.128", "1.0.10.255", "custom3"),
		toIPNodeWithProp(t, "2.1.0.0", "2.255.255.255", "custom5"),
	}

	r := strings.NewReader(inputCSV)
	p := TestParser{list: []TestIPNode{}}

	err := BuildIPNodeList(r, &p)
	assert.Nil(t, err)
	assert.Equal(t, len(expectedResult), len(p.list))
	assertEqualTestIPNodes(t, expectedResult, p.list)
}

// TestBuildIPNodeListWithMerge
func TestBuildIPNodeListWithMerge(t *testing.T) {
	inputCSV := `1.0.0.0/24	custom1
1.0.0.2/26	custom1
1.0.10.0/24	custom3
1.0.10.124/30	custom4
2.1.0.0/8	cuustom5`

	expectedResult := []IPNode{
		toIPNodeWithProp(t, "1.0.0.0", "1.0.0.255", "custom1"),
		toIPNodeWithProp(t, "1.0.10.0", "1.0.10.123", "custom3"),
		toIPNodeWithProp(t, "1.0.10.124", "1.0.10.127", "custom4"),
		toIPNodeWithProp(t, "1.0.10.128", "1.0.10.255", "custom3"),
		toIPNodeWithProp(t, "2.1.0.0", "2.255.255.255", "custom5"),
	}

	r := strings.NewReader(inputCSV)
	p := TestParser{list: []TestIPNode{}}

	err := BuildIPNodeList(r, &p)
	assert.Nil(t, err)
	assert.Equal(t, len(expectedResult), len(p.list))
	assertEqualTestIPNodes(t, expectedResult, p.list)
}

func TestSearchBinary(t *testing.T) {
	inputCSV := `1.0.0.0/24	custom1
1.0.0.2/26	custom2
1.0.10.0/24	custom3
1.0.10.124/30	custom4
2.1.0.0/8	cuustom5`

	r := strings.NewReader(inputCSV)
	p := TestParser{list: []TestIPNode{}}

	err := BuildIPNodeList(r, &p)
	assert.Nil(t, err)
	assert.Equal(t, 7, len(p.list))

	queries := []string{
		"1.0.0.1",
		"1.0.0.60",
		"1.0.0.67",
		"1.0.10.123",
		"1.0.10.124",
		"1.0.10.200",
		"2.2.155.43",
	}

	expectedResult := []IPNode{
		toIPNodeWithProp(t, "1.0.0.0", "1.0.0.1", "custom1"),
		toIPNodeWithProp(t, "1.0.0.2", "1.0.0.63", "custom2"),
		toIPNodeWithProp(t, "1.0.0.64", "1.0.0.255", "custom1"),
		toIPNodeWithProp(t, "1.0.10.0", "1.0.10.123", "custom3"),
		toIPNodeWithProp(t, "1.0.10.124", "1.0.10.127", "custom4"),
		toIPNodeWithProp(t, "1.0.10.128", "1.0.10.255", "custom3"),
		toIPNodeWithProp(t, "2.1.0.0", "2.255.255.255", "custom5"),
	}

	gotResult := []IPNode{}

	ipNodeGetter := func(idx int) IPNode {
		return &p.list[idx]
	}

	for _, q := range queries {
		got, err := SearchBinary(q, len(p.list), ipNodeGetter)
		assert.Nil(t, err)
		gotResult = append(gotResult, got)
	}

	assertEqual(t, expectedResult, gotResult)

	// test not found
	_, err = SearchBinary("192.4.1.123", len(p.list), ipNodeGetter)
	assert.Equal(t, ErrNodeNotFound, err)

	// test wrong input error
	_, err = SearchBinary("badip", len(p.list), ipNodeGetter)
	assert.Equal(t, errors.New("ErrInvalidIP"), err)

}

// TestRangeCIDR tests the rangeCIDR function
func TestRangeCIDR(t *testing.T) {
	inputs := []string{
		"192.0.2.1/24",
		"192.0.2.1/8",
		"192.0.2.1/32",
		"192.0.2.1/5",
	}

	expectedOutcomes := []IPNode{
		toIPNode(t, "192.0.2.1", "192.0.2.255"),
		toIPNode(t, "192.0.2.1", "192.255.255.255"),
		toIPNode(t, "192.0.2.1", "192.0.2.1"),
		toIPNode(t, "192.0.2.1", "199.255.255.255"),
	}

	for idx, value := range inputs {
		lower, upper, err := rangeCIDR(value)
		assert.Nil(t, err)

		exp := expectedOutcomes[idx]
		assert.True(t, lower.Equal(exp.GetLowIP()), "Test idx: %d, lower, expected: %s, got: %s", idx, exp.GetLowIP().String(), lower.String())
		assert.True(t, upper.Equal(exp.GetHighIP()), "Test idx: %d, upper, expected: %s, got: %s", idx, exp.GetHighIP().String(), upper.String())
	}

	_, _, err := rangeCIDR("hey/network 'sup?")
	assert.Equal(t, errors.New("Invalid CIDR IP range"), err)
}

// TestHandleStackNoIntersection tests the handleStack function - no intersection
func TestHandleStackNoIntersection(t *testing.T) {
	parser := &TestParser{list: []TestIPNode{}}
	stack := []IPNode{}

	// test no intersection
	i1 := toIPNode(t, "1.0.0.0", "1.0.1.0")
	i2 := toIPNode(t, "1.0.1.1", "1.0.1.12")
	i3 := toIPNode(t, "1.0.1.100", "1.0.1.112")

	stack = handleStack(stack, parser, i1)
	stack = handleStack(stack, parser, i2)
	stack = handleStack(stack, parser, i3)
	stack = finalizeStackAndList(stack, parser)

	assertEqual(t, []IPNode{}, stack)
	assertEqualTestIPNodes(t, []IPNode{
		toIPNode(t, "1.0.0.0", "1.0.1.0"),
		toIPNode(t, "1.0.1.1", "1.0.1.12"),
		toIPNode(t, "1.0.1.100", "1.0.1.112"),
	}, parser.list)
}

// TestHandleStackNestedNetworks tests the handleStack function - multiple embedded ranges
func TestHandleStackNestedNetworks(t *testing.T) {
	parser := &TestParser{list: []TestIPNode{}}
	stack := []IPNode{}

	// test no intersection
	i1 := toIPNode(t, "1.0.0.0", "1.0.1.0")   // no overlap
	i2 := toIPNode(t, "1.0.1.1", "1.0.1.100") // parent of overlaps
	i3 := toIPNode(t, "1.0.1.10", "1.0.1.20") // first overlap
	i4 := toIPNode(t, "1.0.1.30", "1.0.1.80") // second overlap
	i5 := toIPNode(t, "1.0.2.1", "1.0.2.112") // no overlap

	stack = handleStack(stack, parser, i1)
	stack = handleStack(stack, parser, i2)
	stack = handleStack(stack, parser, i3)
	stack = handleStack(stack, parser, i4)
	stack = handleStack(stack, parser, i5)
	stack = finalizeStackAndList(stack, parser)

	assertEqual(t, []IPNode{}, stack)
	assertEqualTestIPNodes(t, []IPNode{
		toIPNode(t, "1.0.0.0", "1.0.1.0"),    // first non overlapping
		toIPNode(t, "1.0.1.1", "1.0.1.9"),    // beginning of parent range till the first subrange
		toIPNode(t, "1.0.1.10", "1.0.1.20"),  // first subrange
		toIPNode(t, "1.0.1.21", "1.0.1.29"),  // parent range between first and second subrange
		toIPNode(t, "1.0.1.30", "1.0.1.80"),  // second subrange
		toIPNode(t, "1.0.1.81", "1.0.1.100"), // parent range from the end of second suubrange till the end of parent range
		toIPNode(t, "1.0.2.1", "1.0.2.112"),  // third non overlapping
	}, parser.list)
}

// TestHandleStackNestedNetworks tests the handleStack function - intersection
func TestHandleStackIntersection(t *testing.T) {
	parser := &TestParser{list: []TestIPNode{}}
	stack := []IPNode{}

	// test no intersection
	i1 := toIPNode(t, "1.0.0.0", "1.0.1.0")
	i2 := toIPNode(t, "1.0.0.150", "1.0.3.1")

	stack = handleStack(stack, parser, i1)
	stack = handleStack(stack, parser, i2)
	stack = finalizeStackAndList(stack, parser)

	assertEqual(t, []IPNode{}, stack)
	assertEqualTestIPNodes(t, []IPNode{
		toIPNode(t, "1.0.0.0", "1.0.0.149"),
		toIPNode(t, "1.0.0.150", "1.0.3.1"),
	}, parser.list)
}

// TestPlusOneMinusOne tests the plusOne and minusOne functions
func TestPlusOneMinusOne(t *testing.T) {
	sourceIps := []string{
		"192.0.0.1",
		"1.1.255.255",
		"1.0.0.0",
	}
	expectedPlusResult := []string{
		"192.0.0.2",
		"1.2.0.0",
		"1.0.0.1",
	}
	expectedMinusResult := []string{
		"192.0.0.0",
		"1.1.255.254",
		"0.255.255.255",
	}
	for idx, source := range sourceIps {
		gotPlus, gotMinus := plusOne(net.ParseIP(source)), minusOne(net.ParseIP(source))
		expPlus, expMinus := net.ParseIP(expectedPlusResult[idx]), net.ParseIP(expectedMinusResult[idx])
		assert.True(t, expPlus.Equal(gotPlus), "%s + 1 should be %s, but got %s", source, expPlus.String(), gotPlus.String())
		assert.True(t, expMinus.Equal(gotMinus), "%s - 1 should be %s, but got %s", source, expMinus.String(), gotMinus.String())
	}
}

// TestLessThan tests the lessThan method
func TestLessThan(t *testing.T) {
	assert.True(t, lessThan(net.ParseIP("1.0.0.0"), net.ParseIP("1.0.0.1")))
	assert.False(t, lessThan(net.ParseIP("1.0.2.0"), net.ParseIP("1.0.0.1")))
	assert.True(t, lessThan(net.ParseIP("1.0.0.255"), net.ParseIP("1.0.1.0")))
	assert.False(t, lessThan(net.ParseIP("1.0.0.0"), net.ParseIP("1.0.0.0")))
}

// dumpStackAndList - logs the contents of the stack and list IPNodes
func dumpStackAndList(t *testing.T, stack, list []TestIPNode) {
	t.Log("Stack nodes:")
	for _, v := range stack {
		t.Logf("\t lower: %s, upper: %s", v.GetLowIP().String(), v.GetHighIP().String())
	}
	t.Log("List nodes:")
	for _, v := range list {
		t.Logf("\t lower: %s, upper: %s", v.GetLowIP().String(), v.GetHighIP().String())
	}
}

func assertEqualTestIPNodes(t *testing.T, expected []IPNode, got []TestIPNode) {
	gotIPNodes := []IPNode{}
	for _, v := range got {
		vCpy := v
		gotIPNodes = append(gotIPNodes, &vCpy)
	}
	assertEqual(t, expected, gotIPNodes)
}

// assertEqual - helps to assert if IPNodes are equal
func assertEqual(t *testing.T, expected []IPNode, got []IPNode) {
	assert.Equal(t, len(expected), len(got))
	for idx, exp := range expected {
		got := got[idx]
		assert.True(t, exp.GetLowIP().Equal(got.GetLowIP()), "List idx: %d, lower, expected: %s, got: %s", idx, exp.GetLowIP().String(), got.GetLowIP().String())
		assert.True(t, exp.GetHighIP().Equal(got.GetHighIP()), "List idx: %d, upper, expected: %s, got: %s", idx, exp.GetHighIP().String(), got.GetHighIP().String())
		expStruct, ok1 := exp.(*TestIPNode)
		gotStruct, ok2 := exp.(*TestIPNode)
		if !ok1 || !ok2 {
			assert.Fail(t, "The got nodes are not instances of TestIPNode!")
		}
		assert.Equal(t, expStruct.CustomData, gotStruct.CustomData)
	}
}

// toIPNode - helper function, returns an IP node
func toIPNode(t *testing.T, lowerIPStr, upperIPStr string) IPNode {
	return &TestIPNode{BaseIPNode: BaseIPNode{IPAddressLow: net.ParseIP(lowerIPStr), IPAddressHigh: net.ParseIP(upperIPStr)}}
}

func toIPNodeWithProp(t *testing.T, lowerIPStr, upperIPStr, customData string) IPNode {
	node := toIPNode(t, lowerIPStr, upperIPStr).(*TestIPNode)
	node.CustomData = customData
	return node
}

// The IPNode implementation used for testing
type TestIPNode struct {
	BaseIPNode
	CustomData string
}

// Clone implementation for the basic type
func (n *TestIPNode) Clone() IPNode {
	return &TestIPNode{BaseIPNode: BaseIPNode{IPAddressHigh: n.IPAddressHigh, IPAddressLow: n.IPAddressLow}, CustomData: n.CustomData}
}

// DataEquals implementation for the basic type
func (n *TestIPNode) DataEquals(other IPNode) bool {
	otherNode := other.(*TestIPNode)
	return n.CustomData == otherNode.CustomData
}

// TestParser - a dummy parser for testing purposes
type TestParser struct {
	list []TestIPNode
}

func (p *TestParser) PreconfigureReader(reader *csv.Reader) error {
	reader.FieldsPerRecord = 2
	reader.Comma = '\t'
	return nil
}
func (p *TestParser) ValidateRecord(record []string) error {
	return nil
}

func (p *TestParser) ExtractIP(record []string) string {
	return record[0]
}

func (p *TestParser) PopulateRecordData(record []string, node IPNode) error {
	d, ok := node.(*TestIPNode)
	if !ok {
		return errors.New("Got node is not TestIPNode")
	}
	d.CustomData = record[1]
	return nil
}

func (p *TestParser) CreateNode() IPNode {
	return &TestIPNode{}
}

func (p *TestParser) NodeListLen() int {
	return len(p.list)
}

func (p *TestParser) AppendNode(node IPNode) {
	n := node.(*TestIPNode)
	p.list = append(p.list, *n)
}

func (p *TestParser) GetNode(idx int) IPNode {
	return &p.list[idx]
}
