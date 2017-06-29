package annotator

import (
	"fmt"
	"io"
	"net/http"

	"github.com/golang/protobuf/proto"
	pb "github.com/m-lab/annotation-service/proto"
)

const appkey = "Temp Key"

func init() {
	http.HandleFunc("/", handler)
	http.HandleFunc("/search_location", search_location)
}

func search_location(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength == 0 {
		fmt.Fprint(w, "EMPTY BODY!")
		return
	}

	body_buffer := make([]byte, r.ContentLength)
	_, err := io.ReadFull(r.Body, body_buffer)

	if err != nil {
		fmt.Fprint(w, "ERROR READING BODY")
		return
	}

	location_request := &pb.LocationRequest{}
	err = proto.Unmarshal(body_buffer, location_request)

	if err != nil {
		fmt.Fprint(w, "CANNOT PARSE REQUEST")
		return
	}

	fmt.Fprint(w, location_request.IP_Addr)

	fmt.Fprint(w, "\n\nWe're at the end now...\n")
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Hello, world!")
}
