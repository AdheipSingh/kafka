package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/gorilla/mux"
)

const (
	// CsvName file Name
	CsvName = "cdr.csv"
)

// CsvLine create the csv data object
type CsvLine struct {
	Column1 string
	Column2 string
}

// Topic creates topic name object
type Topic struct {
	Name            string `json:"name"`
	Bootstrapserver string `json:"bootstrapserver"`
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/stream", Stream).Methods("POST")

	log.Fatal(http.ListenAndServe(":3000", r))
}

// Stream shall create a topic with a given name and csv as message data
func Stream(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	req, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		panic(err)
	}

	var data Topic

	err = json.Unmarshal(req, &data)
	if err != nil {
		http.Error(w, err.Error(), 500)
		fmt.Printf("Server error unmarshall")
		return
	}

	Producer(data)

	convertjson, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), 500)
		fmt.Printf("Server error marshall")
		return
	}

	w.Write(convertjson)

}

// Producer takes in the topic name
func Producer(name Topic) {

	prod, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": os.Getenv(name.Bootstrapserver)})
	if err != nil {
		panic(err)
	}

	topic := name.Name
	log.Printf("Creating Topic Name: %s", topic)

	//Reading CSV
	lines, err := ReadCsv(CsvName)
	if err != nil {
		panic(err)
	}

	for _, line := range lines {
		data := CsvLine{
			Column1: line[0],
			Column2: line[1],
		}

		value := TypeConv(data)

		prod.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
		}, nil)
	}

}

// TypeConv shall convert to string type
func TypeConv(d CsvLine) string {

	data := CsvLine{
		Column1: d.Column1,
		Column2: d.Column2,
	}

	return fmt.Sprintf("%v", data)
}

// ReadCsv Opens and Reads a Csv File
func ReadCsv(filename string) ([][]string, error) {

	// Open CSV file
	f, err := os.Open(filename)
	if err != nil {
		return [][]string{}, err
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return [][]string{}, err
	}

	return lines, nil
}
