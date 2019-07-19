package directory_test

import (
	"log"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/directory"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type fakeAnn struct {
	api.Annotator
	startDate time.Time
}

func (f *fakeAnn) AnnotatorDate() time.Time {
	return f.startDate
}

func (f *fakeAnn) String() string {
	return "fake:" + f.AnnotatorDate().Format("20060102")

}

func newFake(date string) *fakeAnn {
	d, _ := time.Parse("20060102", date)
	return &fakeAnn{startDate: d}
}

// TODO - this is a pretty ugly test implementation.  Make it better?
func TestBuild(t *testing.T) {
	day := 24 * time.Hour
	week := 7 * day

	start, _ := time.Parse("20060102", "20090208")

	input := make([]api.Annotator, 0, 5)
	input = append(input, &fakeAnn{startDate: start})
	input = append(input, &fakeAnn{startDate: start.Add(50 * week)})
	input = append(input, &fakeAnn{startDate: start.Add(100 * week)})
	input = append(input, &fakeAnn{startDate: start.Add(200 * week)})
	input = append(input, &fakeAnn{startDate: start.Add(278 * week)})
	input = append(input, &fakeAnn{startDate: start.Add(478 * week)})
	dir := directory.Build(input)

	tests := []struct {
		testDate string // date
		want     string // date
	}{
		{"20170101", "20140608"},
		{"20110101", "20100124"},
		{"20180501", "20180408"},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.testDate, func(t *testing.T) {
			date, _ := time.Parse("20060102", tt.testDate)
			want, _ := time.Parse("20060102", tt.want)
			ann, err := dir.GetAnnotator(date)
			if err != nil || !ann.AnnotatorDate().Equal(want) {
				t.Error("want", tt.want, "got", ann.AnnotatorDate(), err)
				//	dir.Dump()
			}
		})
	}
}

func TestCompositeAnnotator_String(t *testing.T) {
	tests := []struct {
		name       string
		annotators []api.Annotator
		want       string
	}{
		{"simple", []api.Annotator{newFake("20100203"), newFake("20110304")}, "[20100203][20110304]"},
		// TODO: Add test cases.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ca := directory.NewCompositeAnnotator(tt.annotators)

			if got := ca.(directory.CompositeAnnotator).String(); got != tt.want {
				t.Errorf("CompositeAnnotator.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeAnnotators(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name  string
		lists [][]api.Annotator
		want  string
	}{
		{name: "foobar",
			lists: [][]api.Annotator{
				// Three unique dates should result in three CompositeAnnotators in output...
				{newFake("20100203"), newFake("20110405")},
				{newFake("20100101"), newFake("20110101")}},
			// Here are the three expected CAs.
			want: "([20100203][20100101])([20100203][20110101])([20110405][20110101])",
			// TODO: Add test cases.
		},
	}
	fake := newFake("20100203")
	log.Println(fake.AnnotatorDate())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := directory.MergeAnnotators(tt.lists[0], tt.lists[1])
			// This is just a hack to allow us to create a useful signature.
			expectedDate := make([]string, 3)
			expectedDate[0] = "[20100101]"
			expectedDate[1] = "[20100203]"
			expectedDate[2] = "[20110101]"
			for i, ann := range got {
				if expectedDate[i] != ann.AnnotatorDate().Format("[20060102]") {
					t.Errorf("Expect AnnotateDate %s got %s", expectedDate[i], ann.AnnotatorDate().Format("[20060102]"))
				}
			}
			gotString := directory.NewCompositeAnnotator(got).(directory.CompositeAnnotator).String()
			if gotString != tt.want {
				t.Errorf("MergeAnnotators() =\n %v want:\n %v", gotString, tt.want)
			}
		})
	}
}
