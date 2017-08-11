package downloader

import (
	"cloud.google.com/go/storage"
	"errors"
	"golang.org/x/net/context"
	"log"

	"github.com/m-lab/annotation-service/parser"
)


//Creates list of IP address Nodes
func InitializeTable(ctx context.Context, GCSFolder, GCSFile string, IPVersion int) ([]parser.Node, error) {
	var geoData []parser.Node
	if ctx == nil {
		ctx = context.Background()
	}
	storageReader, err := createReader(GCSFolder, GCSFile, ctx)
	if err != nil {
		log.Println("storage reader failed")
		return geoData, errors.New("Storage Reader Failed")
	}
	geoData, err = parser.CreateList(storageReader, IPVersion)
	if err != nil {
		log.Println("geoData createList failed")
		return geoData, errors.New("geoData createList Failed")
	}
	return geoData, nil
}

//creates generic reader
func createReader(bucket string, bucketObj string, ctx context.Context) (*storage.Reader, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	obj := client.Bucket(bucket).Object(bucketObj)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return reader, nil
}

/*list := []Node{}	
	fmt.Println("GOT THIS FAR THO.")	
	
	gr, err := gzip.NewReader(reader) 
	if err != nil{
		fmt.Println("GZIP.NEWREADER failed.")
		log.Fatal(err) 
	}
	defer gr.Close()
	fmt.Println("HOW ABOUT THIS FAR??")
	
	r := csv.NewReader(gr)
	if r == nil{
		fmt.Println("gzip new reader for csv failed")
	}*/

