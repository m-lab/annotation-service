package geoloader

import (
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
)

//https://storage.cloud.google.com/downloader-mlab-sandbox/RouteViewIPv6/2010/04/routeviews-rv6-20100401-1200.pfx2as.gz?_ga=2.203048593.-1629863070.1538756657&_gac=1.91191784.1549956065.Cj0KCQiA14TjBRD_ARIsAOCmO9bXr012UcoLzU_Ndo-gPl6-wHS-RucM7cT2HaVRb8kd_-lWjbpsJZQaAuOoEALw_wcB
var (
	asnRegexV4 = regexp.MustCompile(`RouteViewIPv4/\d{4}/\d{2}/routeviews-(oix|rv2)-\d{8}-\d{4}\.pfx2as\.gz`)
	asnRegexV6 = regexp.MustCompile(`RouteViewIPv6/\d{4}/\d{2}/routeviews-rv6-\d{8}-\d{4}\.pfx2as\.gz`)
)

func ASNv4Loader(
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) api.CachingLoader {
	return newCachingLoader(
		func(file *storage.ObjectAttrs) error {
			return filter(file, asnRegexV4, time.Time{})
		},
		loader,
		api.RouteViewPrefix)
}

func ASNv6Loader(
	loader func(*storage.ObjectAttrs) (api.Annotator, error)) api.CachingLoader {
	return newCachingLoader(
		func(file *storage.ObjectAttrs) error {
			return filter(file, asnRegexV6, time.Time{})
		},
		loader,
		api.RouteViewPrefix)
}
