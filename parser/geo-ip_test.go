package parser_test

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/m-lab/annotation-service/parser"
)

// Returns nil if two IP Lists are equal
func isEqualIPLists(listComp, list []parser.IPNode) error {
	for index, element := range list {
		err := parser.IsEqualIPNodes(element, listComp[index])
		if err != nil {
			return err
		}
	}
	return nil
}

// Returns nil if two Location lists are equal
func isEqualLocLists(list, listComp []parser.LocationNode) error {
	for index, element := range list {
		if index >= len(listComp) {
			output := fmt.Sprint("Out of range:", index)
			log.Println(output)
			return errors.New(output)
		}
		if element.GeonameID != listComp[index].GeonameID {
			output := strings.Join([]string{"GeonameID inconsistent\ngot:", strconv.Itoa(element.GeonameID), " \nwanted:", strconv.Itoa(listComp[index].GeonameID)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.ContinentCode != listComp[index].ContinentCode {
			output := strings.Join([]string{"Continent code inconsistent\ngot:", element.ContinentCode, " \nwanted:", listComp[index].ContinentCode}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CountryCode != listComp[index].CountryCode {
			output := strings.Join([]string{"Country code inconsistent\ngot:", element.CountryCode, " \nwanted:", listComp[index].CountryCode}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CountryName != listComp[index].CountryName {
			output := strings.Join([]string{"Country name inconsistent\ngot:", element.CountryName, " \nwanted:", listComp[index].CountryName}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.MetroCode != listComp[index].MetroCode {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", strconv.FormatInt(element.MetroCode, 16), " \nwanted:", strconv.FormatInt(listComp[index].MetroCode, 16)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CityName != listComp[index].CityName {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", element.CityName, " \nwanted:", listComp[index].CityName}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.RegionName != listComp[index].RegionName {
			output := strings.Join([]string{"RegionName inconsistent\ngot:", element.RegionName, " \nwanted:", listComp[index].RegionName}, "")
			log.Println(output)
			return errors.New(output)
		}
	}
	return nil
}
