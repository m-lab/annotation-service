/* demo code to show how to use the C-Wrapper of legacy Maxmind API.
   originally forked from github.com/abh/geoip
*/
package main

import (
	"fmt"

	"github.com/m-lab/annotation-service/handler/geoip"
)

func main() {

	file6 := "./GeoIPv6.dat"

	gi6, err := geoip.Open(file6, "default")
	if err != nil {
		fmt.Printf("Could not open GeoIPv6 database: %s\n", err)
	}

	gi, err := geoip.Open("./GeoLiteCity.dat", "default")
	if err != nil {
		fmt.Printf("Could not open GeoIP database: %s\n", err)
	}

	giasn, err := geoip.Open("./GeoIPASNum.dat", "default")
	if err != nil {
		fmt.Printf("Could not open GeoIPASN database: %s\n", err)
	}

	giasn6, err := geoip.Open("./GeoIPASNumv6.dat", "default")
	if err != nil {
		fmt.Printf("Could not open GeoIPASN database: %s\n", err)
	}

	if giasn != nil {
		ip := "207.171.7.51"
		asn, netmask := giasn.GetName(ip)
		fmt.Printf("%s: %s (netmask /%d)\n", ip, asn, netmask)

	}

	if gi != nil {
		record := gi.GetRecord("207.171.7.51", true)
		fmt.Printf("%v\n", record)
	}
	if gi6 != nil {
		ip := "2607:f238:2::5"
		country, netmask := gi6.GetCountry_v6(ip)
		var asn string
		var asn_netmask int
		if giasn6 != nil {
			asn, asn_netmask = giasn6.GetNameV6(ip)
		}
		fmt.Printf("%s: %s/%d %s/%d\n", ip, country, netmask, asn, asn_netmask)
	}

	gi6.Free()

	if gi6 != nil {
		fmt.Printf("Free() did not release memory correctly.")
	}

}
