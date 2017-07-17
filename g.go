package annotator 

import (
	"net/http" 
	"fmt" 
	"log"
	"cloud.google.com/go/storage"
	"google.golang.org/appengine"
)

func lookupAndRespond(r *http.Request, w http.ResponseWriter, ip string, time_milli int64) {
	createList(r,w) 
} 

func createList(r *http.Request, w http.ResponseWriter) {

	ctx :=  appengine.NewContext(r)
	client, err := storage.NewClient(ctx)
	
//	client, err := storage.GetStorageClient(false) 

	if err != nil{
		fmt.Fprintf(w, "BAAD\n") 
		log.Fatal(err) 
	}

	if client != nil{
		fmt.Fprintf(w, "GOOD\n") 
	}


	bkt := client.Bucket("gs://m-lab-sandbox") 
	
	obj := bkt.Object("/annotator-data/GeoIPCountryWhois.csv") 
	r,err := obj.NewReader(ctx) 
	if err != nil{
		fmt.Fprintf(w, "badd\n") 
	}
	defer r.Close()  
}
