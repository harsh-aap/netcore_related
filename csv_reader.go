package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
)

func ReadCSV(jobs chan<- Contact) error {
	log.Println("📂 Starting CSV read:", CSVFilePath)

	file, err := os.Open(CSVFilePath)
	if err != nil {
		log.Println("❌ Error opening CSV:", err)
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		log.Println("❌ Error reading CSV header:", err)
		return err
	}

	rowNum := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("❌ Error reading row %d: %v", rowNum, err)
			continue
		}

		contact := Contact{
			Phone:     record[0], // MOBILE
			FirstName: record[1], // FIRST_NAME
			LastName:  record[2], // LAST_NAME
			Rashi:     record[3], // RASHI
			Age:       record[4], // AGE
			Gender:    record[5], // GENDER
		}

		log.Printf("➡ Queuing contact row %d: %s", rowNum, contact.Phone)
		jobs <- contact
		rowNum++

	}

	log.Println("✅ Finished reading CSV")
	return nil
}

