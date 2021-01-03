package main

import (
	"log"

	"gitlab.com/lilh/es-test/internal/elastic"
)

func main() {

	index := "nba"

	query, _ := elastic.CreateESQueryStatement()

	res, err := elastic.GetESDataByPerformESQueryWithScroll(query, index)
	if err != nil {
		log.Printf("%+v\n", err)
		return
	}
	log.Printf("%+v\n", len(res.Hits.Documents))
}
