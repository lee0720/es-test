// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

//go:generate easyjson -all -snake_case $GOFILE

package model

import "time"

//Article ...
type Article struct {
	ID        uint
	Title     string
	Body      string
	Published time.Time
	Author    *Author
}

//Author ...
type Author struct {
	FirstName string
	LastName  string
}

//Response ...
type Response struct {
	Hits Hits `json:"hits"`
}

//Hits ...
type Hits struct {
	Total   map[string]interface{} `json:"total"`
	Content []Content              `json:"hits"`
}

//Content ...
type Content struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	ID     string                 `json:"id"`
	Source map[string]interface{} `json:"_source"`
	Sort   []float64              `json:"sort"`
}

// CommonESResponse ...
type CommonESResponse struct {
	Hits struct {
		Total     TotalDocsInfo     `json:"total"`
		Documents []DocumentContent `json:"hits"`
	} `json:"hits"`
}

//TotalDocsInfo ...
type TotalDocsInfo struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

//DocumentContent ...
type DocumentContent struct {
	ID      string         `json:"_id"`
	Content CommonDocument `json:"_source"`
}

// "countryEn" : "United States",
// "teamName" : "老鹰",
// "birthDay" : 831182400000,
// "country" : "美国",
// "teamCityEn" : "Atlanta",
// "code" : "jaylen_adams",
// "displayAffiliation" : "United States",
// "displayName" : "杰伦 亚当斯",
// "schoolType" : "College",
// "teamConference" : "东部",
// "teamConferenceEn" : "Eastern",
// "weight" : "86.2 公斤",
// "teamCity" : "亚特兰大",
// "playYear" : 1,
// "jerseyNo" : "10",
// "teamNameEn" : "Hawks",
// "draft" : 2018,
// "displayNameEn" : "Jaylen Adams",
// "heightValue" : 1.88,
// "birthDayStr" : "1996-05-04",
// "position" : "后卫",
// "age" : 23,
// "playerId" : "1629121"

//CommonDocument ...
type CommonDocument struct {
	PlayerID    string `json:"playerId"`
	Age         int    `json:"age"`
	DisplayName string `json:"displayName"`
}
