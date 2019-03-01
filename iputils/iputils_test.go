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
	p := TestParser{}

	got, err := BuildIPNodeList(r, &p)
	assert.Nil(t, err)

	assertEqual(t, expectedResult, got)
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
	assert.Error(t, errors.New("Invalid CIDR IP range"), err)
}

// TestHandleStackNoIntersection tests the handleStack function - no intersection
func TestHandleStackNoIntersection(t *testing.T) {
	stack := []IPNode{}
	list := []IPNode{}

	// test no intersection
	i1 := toIPNode(t, "1.0.0.0", "1.0.1.0")
	i2 := toIPNode(t, "1.0.1.1", "1.0.1.12")
	i3 := toIPNode(t, "1.0.1.100", "1.0.1.112")

	stack, list = handleStack(stack, list, i1)
	stack, list = handleStack(stack, list, i2)
	stack, list = handleStack(stack, list, i3)
	stack, list = finalizeStackAndList(stack, list)

	dumpStackAndList(t, stack, list)

	assertEqual(t, []IPNode{}, stack)
	assertEqual(t, []IPNode{
		toIPNode(t, "1.0.0.0", "1.0.1.0"),
		toIPNode(t, "1.0.1.1", "1.0.1.12"),
		toIPNode(t, "1.0.1.100", "1.0.1.112"),
	}, list)
}

// TestHandleStackNestedNetworks tests the handleStack function - multiple embedded ranges
func TestHandleStackNestedNetworks(t *testing.T) {
	stack := []IPNode{}
	list := []IPNode{}

	// test no intersection
	i1 := toIPNode(t, "1.0.0.0", "1.0.1.0")   // no overlap
	i2 := toIPNode(t, "1.0.1.1", "1.0.1.100") // parent of overlaps
	i3 := toIPNode(t, "1.0.1.10", "1.0.1.20") // first overlap
	i4 := toIPNode(t, "1.0.1.30", "1.0.1.80") // second overlap
	i5 := toIPNode(t, "1.0.2.1", "1.0.2.112") // no overlap

	stack, list = handleStack(stack, list, i1)
	stack, list = handleStack(stack, list, i2)
	stack, list = handleStack(stack, list, i3)
	stack, list = handleStack(stack, list, i4)
	stack, list = handleStack(stack, list, i5)
	stack, list = finalizeStackAndList(stack, list)

	dumpStackAndList(t, stack, list)

	assertEqual(t, []IPNode{}, stack)
	assertEqual(t, []IPNode{
		toIPNode(t, "1.0.0.0", "1.0.1.0"),    // first non overlapping
		toIPNode(t, "1.0.1.1", "1.0.1.9"),    // beginning of parent range till the first subrange
		toIPNode(t, "1.0.1.10", "1.0.1.20"),  // first subrange
		toIPNode(t, "1.0.1.21", "1.0.1.29"),  // parent range between first and second subrange
		toIPNode(t, "1.0.1.30", "1.0.1.80"),  // second subrange
		toIPNode(t, "1.0.1.81", "1.0.1.100"), // parent range from the end of second suubrange till the end of parent range
		toIPNode(t, "1.0.2.1", "1.0.2.112"),  // third non overlapping
	}, list)
}

// TestHandleStackNestedNetworks tests the handleStack function - intersection
func TestHandleStackIntersection(t *testing.T) {
	stack := []IPNode{}
	list := []IPNode{}

	// test no intersection
	i1 := toIPNode(t, "1.0.0.0", "1.0.1.0")
	i2 := toIPNode(t, "1.0.0.150", "1.0.3.1")

	stack, list = handleStack(stack, list, i1)
	stack, list = handleStack(stack, list, i2)
	stack, list = finalizeStackAndList(stack, list)
	dumpStackAndList(t, stack, list)

	assertEqual(t, []IPNode{}, stack)
	assertEqual(t, []IPNode{
		toIPNode(t, "1.0.0.0", "1.0.0.149"),
		toIPNode(t, "1.0.0.150", "1.0.3.1"),
	}, list)
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
		gotPlus, gotMinus := plusOne(toNetIP(t, source)), minusOne(toNetIP(t, source))
		expPlus, expMinus := toNetIP(t, expectedPlusResult[idx]), toNetIP(t, expectedMinusResult[idx])
		assert.True(t, expPlus.Equal(gotPlus), "%s + 1 should be %s, but got %s", source, expPlus.String(), gotPlus.String())
		assert.True(t, expMinus.Equal(gotMinus), "%s - 1 should be %s, but got %s", source, expMinus.String(), gotMinus.String())
	}
}

// TestLessThan tests the lessThan method
func TestLessThan(t *testing.T) {
	assert.True(t, lessThan(toNetIP(t, "1.0.0.0"), toNetIP(t, "1.0.0.1")))
	assert.False(t, lessThan(toNetIP(t, "1.0.2.0"), toNetIP(t, "1.0.0.1")))
	assert.True(t, lessThan(toNetIP(t, "1.0.0.255"), toNetIP(t, "1.0.1.0")))
	assert.False(t, lessThan(toNetIP(t, "1.0.0.0"), toNetIP(t, "1.0.0.0")))
}

// dumpStackAndList - logs the contents of the stack and list IPNodes
func dumpStackAndList(t *testing.T, stack, list []IPNode) {
	t.Log("Stack nodes:")
	for _, v := range stack {
		t.Logf("\t lower: %s, upper: %s", v.GetLowIP().String(), v.GetHighIP().String())
	}
	t.Log("List nodes:")
	for _, v := range list {
		t.Logf("\t lower: %s, upper: %s", v.GetLowIP().String(), v.GetHighIP().String())
	}
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
func toIPNode(t *testing.T, lowerIpStr, upperIpStr string) IPNode {
	return &TestIPNode{BaseIPNode: BaseIPNode{IPAddressLow: toNetIP(t, lowerIpStr), IPAddressHigh: toNetIP(t, upperIpStr)}}
}

func toIPNodeWithProp(t *testing.T, lowerIpStr, upperIpStr, customData string) IPNode {
	node := toIPNode(t, lowerIpStr, upperIpStr).(*TestIPNode)
	node.CustomData = customData
	return node
}

// toNetIP - helper function, creates a net.IP from string
func toNetIP(t *testing.T, addrStr string) net.IP {
	ip, _, err := net.ParseCIDR(addrStr + "/0")
	assert.Nil(t, err)
	return ip
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

// TestParser - a dummy parser for testing purposes
type TestParser struct{}

func (p *TestParser) PreconfigureReader(reader *csv.Reader) error {
	reader.FieldsPerRecord = 2
	reader.Comma = '\t'
	return nil
}

func (p *TestParser) NewNode() IPNode {
	return &TestIPNode{}
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
