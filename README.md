# Elasticsearch Payload score based function Plugin

Payload function plugin for function score query in Elasticsearch

The payload value is taken from termVectors if enabled ( This is faster ) or from the reverse index.

## Versions


| Elasticsearch  | Plugin         | Release date |
| -------------- | -------------- | ------------ |
| 1.7.0          | 1.7.0.0        | Sept 7, 2015 |


## Sample usecase

Sample documentA <br/>
  ( A lot of gravy and very spicy )

		{
		  "name": "chilli chicken",
		  "tasteTypes": [
		    "spicy|100",
		    "salty|10",
		    "gravy|100"
		  ]
		}

Sample documentB <br/>

 ( Lot of gravy gravy bu not very spicy )

		{
		  "name": "Chicken with nuts",
		  "tasteTypes": [
		    "spicy|20",
		    "sweet",
		    "gravy|100"
		  ]
		}


Here as you can see the level of spicy and gravy is different for different dishes.
So when i search for dishes which are spicy , it makes sense to show "chilli chicken" above "chicken with nuts" as the former is more spicy.

In such cases , using the plugin i made , we can give search as follows -

	{
	  "query" : {
	           "function_score" : {
	                    "query" : {    "term" : "spicy" },
	                     "functions" : [
	                        "payload_factor" : {
	                        "tasteTypes" : [ "spicy" ] 
	             }
	          ]
	        }
	    }
	}

The result will have documentA as the highest score.

## Installation

mvn clean install -DskipTests

The distribution would be present in target/releases

unzip and copy the zip file in there to the plugin folder

Do not forget to restart the node after installing.

To install using plugin

	./bin/plugin -install payload-scoring -url  http://factweavers.com/repository/com/factweavers/elasticsearch/plugin/elasticsearch-payload-scoring-function/1.7.0.0/elasticsearch-payload-scoring-function-1.7.0.0-plugin.zip



## Issues

All feedback is welcome! If you find issues, please post them at [Github](https://github.com/Vineeth-Mohan/elasticsearch-payload-scoring-function/issues)


## Example

	curl -XDELETE 'localhost:9200/test'

	curl -XPUT 'localhost:9200/test' -d '{
	  "settings" : {
	     "number_of_shards" : "1",
	     "number_of_replicas" : "0",
	     "analysis" : {
		    "analyzer" : {
		        "my_payload_analyzer" : {
		            "tokenizer" : "whitespace",
		            "filter" : [ "delimited_payload_filter" ]
		        }
		     }
	     }
	  }
	}'
	echo
	curl -XPUT 'localhost:9200/test/docs/_mapping' -d '{
	      "properties" : {
		"content" : {
		  "type": "string",
		  "analyzer" : "my_payload_analyzer",
		  "term_vector": "with_positions_offsets_payloads"
		}
	      }
	}'
	echo
	curl -XPUT 'localhost:9200/test/docs/1' -d '{
	    "content" : "foo bar baz|3.0"
	}'
	echo

	curl -XPUT 'localhost:9200/test/docs/2' -d '{
	    "content" : "foo bar|2.0 baz"
	}'
	echo
	sleep 2
	curl -XPOST 'http://localhost:9200/test/_search' -d '{
	  "explain": true,
	  "query": {
	    "function_score": {
	      "query": {
		"match_all": {}
	      },
	      "score_mode": "sum",
	      "boost_mode": "replace",
	      "functions": [
		{
		  "payload_factor": {
		    "field": "content",
		    "values": [
		      "bar",
		      "baz"
		    ]
		  }
		}
	      ]
	    }
	  }
	}'



# License

Elasticsearch Payload scoring function Plugin

Copyright (C) 2015 Vineeth Mohan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
you may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
