package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"os"
	"transaction_processor/pkg"
)

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Printf("Error reading config file: %s", err)
		os.Exit(1)
	}

	bufferSize := viper.GetInt("buffer_size")
	filename := viper.GetString("json_filename")
	numReaders := viper.GetInt("num_readers")

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error opening file: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	rb := pkg.NewRingBuffer(bufferSize)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record pkg.Record
		err := json.Unmarshal([]byte(scanner.Text()), &record)
		if err != nil {
			fmt.Printf("Error unmarshalling JSON: %s", err)
			continue
		}
		rb.Add(record)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %s", err)
		os.Exit(1)
	}

	pkg.ProcessRecords(rb, numReaders)

	rb.Stop()
}
