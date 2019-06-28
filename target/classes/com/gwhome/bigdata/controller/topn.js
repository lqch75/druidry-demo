[ {
	"queryType" : "topN",
	"dataSource" : "sqltest",
	"intervals" : "2015-09-11T00:00:00+00:00/2015-09-13T00:00:00+00:00",
	"granularity" : "minute",
	"dimension" : "channel",
	"threshold" : 2,
	"metric" : "count",
	"aggregations" : [ {
		"type" : "count",
		"name" : "count"
	} ]

} ]
