package annotator 

import (
	"net/http" 
	"fmt" 
	"log"
	"cloud.google.com/go/storage"
	"google.golang.org/appengine"
	"io" 
	//"os"
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

	bkt := client.Bucket("m-lab-sandbox") 
	
	obj := bkt.Object("annotator-data/testMe.csv") 
	reader ,err := obj.NewReader(ctx) 
	if err != nil{
		fmt.Fprintf(w, "badd\n") 
		log.Fatal(err) 
	}

	if _, err := io.Copy(w, reader); err != nil {
		log.Fatal(err) 
	} 

	defer reader.Close()  
}
