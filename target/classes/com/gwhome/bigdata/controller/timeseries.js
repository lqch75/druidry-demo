[ {
	"queryType" : "timeseries",
	"dataSource" : "sqltest",
	"granularity" : "all",
	"intervals" : "2015-09-11T00:00:00+00:00/2015-09-13T00:00:00+00:00",
	"filter" : {
		"type" : "and",
		"fields" : [ {
			"type" : "selector",
			"dimension" : "channel",
			"value" : "n001"
		}, {
			"type" : "selector",
			"dimension" : "cityName",
			"value" : "y"
		} ]
	},
	"aggregations" : [ {
		"type" : "count",
		"name" : "count"
	} ]
} ];
