package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/pkg/errors"
	"gitlab.com/lilh/es-test/internal/elastic/model"

	"github.com/elastic/go-elasticsearch/v7"
)

var client *elasticsearch.Client

func init() {
	var err error
	config := elasticsearch.Config{}
	config.Addresses = []string{"http://127.0.0.1:9200"}
	// config.Username = "elastic"
	// config.Password = "leehom123"
	client, err = elasticsearch.NewClient(config)
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

//Investor ...
type Investor struct {
	CompanyID   string
	CompanyName string
}

//Source ...
type Source struct {
	SomeStr   string //`json:"ik.some_str"`
	SomeInt   int    //`json:"ik.some_int"`
	SomeBool  bool   //`json:"ik.some_bool"`
	Timestamp int64  //`json:"ik.time_stamp"`
}

func initData() []Source {
	res := make([]Source, 0)
	timeStamp := time.Now().Unix()
	for i := 0; i < 100; i++ {
		str := fmt.Sprintf("string%d", i)
		res = append(res, Source{
			SomeStr:   str,
			SomeInt:   i,
			SomeBool:  true,
			Timestamp: timeStamp,
		})
	}
	return res
}

//CreateIndex ...
func CreateIndex() {
	body := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"companyID": map[string]interface{}{
					"type":  "keyword",
					"store": true,
				},
				"companyName": map[string]interface{}{
					"type":  "keyword",
					"store": true,
					"index": true,
				},
			},
		},
	}

	jsonBody, _ := json.Marshal(body)
	log.Println(string(jsonBody))
	req := esapi.IndicesCreateRequest{
		Index: "companies",
		Body:  bytes.NewReader(jsonBody),
	}

	res, err := req.Do(context.Background(), client)
	checkError(err)
	defer res.Body.Close()

	log.Println(res.String())
}

//CreateElasticIndex ...
func CreateElasticIndex() {
	body := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"SomeStr": map[string]interface{}{
					"type":  "keyword",
					"store": true,
				},
				"SomeInt": map[string]interface{}{
					"type":  "keyword",
					"store": true,
					"index": true,
				},
				"SomeBool": map[string]interface{}{
					"type":  "keyword",
					"store": true,
					"index": true,
				},
				"Timestamp": map[string]interface{}{
					"type":  "keyword",
					"store": true,
					"index": true,
				},
			},
		},
	}

	jsonBody, _ := json.Marshal(body)
	log.Println(string(jsonBody))
	req := esapi.IndicesCreateRequest{
		Index: "elastic_doc_v1",
		Body:  bytes.NewReader(jsonBody),
	}

	res, err := req.Do(context.Background(), client)
	checkError(err)
	defer res.Body.Close()

	log.Println(res.String())
}

//InsertElasticBatch ...
func InsertElasticBatch() {
	var bodyBuf bytes.Buffer
	elasticDocs := initData()
	for i, value := range elasticDocs {

		createLine := map[string]interface{}{
			"create": map[string]interface{}{
				"_index": "elastic_doc_v1",
				"_id":    i,
			},
		}
		jsonStr, _ := json.Marshal(createLine)
		bodyBuf.Write(jsonStr)
		bodyBuf.WriteByte('\n')

		body := map[string]interface{}{
			"some_str":   value.SomeStr,
			"some_int":   value.SomeInt,
			"some_bool":  value.SomeBool,
			"time_stamp": value.Timestamp,
		}
		jsonStr, _ = json.Marshal(body)
		bodyBuf.Write(jsonStr)
		bodyBuf.WriteByte('\n')
	}

	req := esapi.BulkRequest{
		Body: &bodyBuf,
	}
	res, err := req.Do(context.Background(), client)
	checkError(err)
	defer res.Body.Close()
	log.Println(res.String())
}

//DeleteIndex ...
func DeleteIndex() {
	req := esapi.IndicesDeleteRequest{
		Index: []string{"companies"},
	}
	res, err := req.Do(context.Background(), client)
	checkError(err)
	defer res.Body.Close()
	log.Println(res.String())
}

//CreateIndexTest ...
func CreateIndexTest() {
	body := map[string]interface{}{
		"mappings": map[string]interface{}{
			"test_type": map[string]interface{}{
				"properties": map[string]interface{}{
					"str": map[string]interface{}{
						"type": "keyword",
					},
				},
			},
		},
	}
	jsonBody, _ := json.Marshal(body)
	req := esapi.IndicesCreateRequest{
		Index: "test_index",
		Body:  bytes.NewReader(jsonBody),
	}

	res, err := req.Do(context.Background(), client)
	checkError(err)
	defer res.Body.Close()
	log.Println(res.String())
}

//SelectBySearchTest ...
func SelectBySearchTest() {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": map[string]interface{}{
					"range": map[string]interface{}{
						"num": map[string]interface{}{
							"gt": 0,
						},
					},
				},
			},
		},
		"size": 0,
		"aggs": map[string]interface{}{
			"num": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "num",
					//"size":  1,
				},
				"aggs": map[string]interface{}{
					"max_v": map[string]interface{}{
						"max": map[string]interface{}{
							"field": "v",
						},
					},
				},
			},
		},
	}
	jsonBody, _ := json.Marshal(query)

	req := esapi.SearchRequest{
		Index:        []string{"test_index"},
		DocumentType: []string{"test_type"},
		Body:         bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), client)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// PerformESQuery ....
func PerformESQuery(index string, query map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return "", errors.WithStack(err)
	}

	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		client.Search.WithIndex(index),
		client.Search.WithBody(&buf),
		client.Search.WithTrackTotalHits((true)),
		client.Search.WithPretty(),
	)
	if err != nil {
		return "", errors.WithStack(err)
	}

	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return "", fmt.Errorf("Error parsing the response body: %s", err)
		}
		return "", fmt.Errorf("[%s] %s: %s",
			res.Status(),
			e["error"].(map[string]interface{})["type"],
			e["error"].(map[string]interface{})["reason"])
	}

	var sb strings.Builder

	buffer := make([]byte, 256)
	for {
		n, err := res.Body.Read(buffer)
		sb.Write(buffer[:n])
		if err != nil {
			if err != io.EOF {
				log.Println("read error: ", err)
			}
			break
		}
	}
	return sb.String(), nil

}

//CreateESQueryStatement ...
func CreateESQueryStatement() (map[string]interface{}, error) {
	// query := map[string]interface{}{
	// 	"query": map[string]interface{}{
	// 		"match": map[string]interface{}{
	// 			"address": "mill lane",
	// 		},
	// 	},
	// 	"from": 10,
	// 	"size": 3,
	// }

	// query := map[string]interface{}{
	// 	"query": map[string]interface{}{
	// 		"bool": map[string]interface{}{
	// 			"must": map[string]interface{}{
	// 				"match": map[string]interface{}{
	// 					"age": "40",
	// 				},
	// 			},
	// 			"must_not": map[string]interface{}{
	// 				"match": map[string]interface{}{
	// 					"state": "ID",
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"size": 20,
	}
	// query := map[string]interface{}{
	// 	"query": map[string]interface{}{
	// 		"bool": map[string]interface{}{
	// 			"must": map[string]interface{}{
	// 				"match_all": map[string]interface{}{},
	// 			},
	// 			"filter": map[string]interface{}{
	// 				"range": map[string]interface{}{
	// 					"balance": map[string]interface{}{
	// 						"gte": 20000,
	// 						"lte": 30000,
	// 					},
	// 				},
	// 			},
	// 		},
	// 	},
	// }
	return query, nil
}

// PerformESQueryWithScroll ...
func PerformESQueryWithScroll(query map[string]interface{}, index string) ([]map[string]interface{}, error) {
	resultList := make([]map[string]interface{}, 0)
	var err error

	var reqBody bytes.Buffer
	err = json.NewEncoder(&reqBody).Encode(query)
	if err != nil {
		err = fmt.Errorf("encode query failed, %v", err)
		return resultList, errors.WithStack(err)
	}
	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		client.Search.WithIndex(string(index)),
		client.Search.WithBody(&reqBody),
		client.Search.WithTrackTotalHits(true),
		client.Search.WithPretty(),
		client.Search.WithTimeout(10*time.Second),
		client.Search.WithScroll(time.Minute),
	)
	if err != nil {
		err = fmt.Errorf("Error getting response: %s", err)
		return resultList, errors.WithStack(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = fmt.Errorf("Error parsing the response body: %s", err)
		} else {
			err = fmt.Errorf("[%s] %s: %s", res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"])
		}
		return resultList, errors.WithStack(err)
	}

	result := make(map[string]interface{})
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		err = fmt.Errorf("Error parsing the response body: %s", err)
	}
	resultList = append(resultList, result)
	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	if len(hits) == query["size"].(int) {
		scrollID := result["_scroll_id"].(string)
		scrollResultList, err := performESScroll(scrollID)
		if err != nil {
			return resultList, errors.WithStack(err)
		}
		resultList = append(resultList, scrollResultList...)
	}
	return resultList, err
}

func performESScroll(scrollID string) ([]map[string]interface{}, error) {
	resultList := make([]map[string]interface{}, 0)
	done := false
	for !done {
		res, err := client.Scroll(
			client.Scroll.WithContext(context.Background()),
			client.Scroll.WithPretty(),
			client.Scroll.WithScrollID(scrollID),
			client.Scroll.WithScroll(time.Minute),
		)
		if err != nil {
			err = fmt.Errorf("Error getting response: %s", err)
			return resultList, errors.WithStack(err)
		}
		defer res.Body.Close()

		if res.IsError() {
			var e map[string]interface{}
			if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
				err = fmt.Errorf("Error parsing the response body: %s", err)
			} else {
				err = fmt.Errorf("[%s] %s: %s", res.Status(),
					e["error"].(map[string]interface{})["type"],
					e["error"].(map[string]interface{})["reason"])
			}
			return resultList, errors.WithStack(err)
		}

		result := make(map[string]interface{})
		if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
			err = fmt.Errorf("Error parsing the response body: %s", err)
		}

		resultList = append(resultList, result)

		scrollID = result["_scroll_id"].(string)
		hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
		if len(hits) == 0 {
			done = true
		}
	}

	return resultList, nil
}

// GetESDataByPerformESQueryWithScroll ...
func GetESDataByPerformESQueryWithScroll(query map[string]interface{}, index string) (*model.CommonESResponse, error) {
	resultList, err := PerformESQueryWithScroll(query, index)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	response := model.CommonESResponse{}
	for _, v := range resultList {
		res := new(model.CommonESResponse)
		jsonData, err := json.Marshal(v)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		err = json.Unmarshal(jsonData, &res)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		response.Hits.Total.Value = res.Hits.Total.Value
		response.Hits.Documents = append(response.Hits.Documents, res.Hits.Documents...)
	}
	return &response, nil

}
