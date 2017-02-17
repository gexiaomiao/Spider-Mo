import json, os
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch,helpers
import yaml


#load settings.yaml
with open("../../yml_folder/spark.yaml", 'r') as spark_yaml:
        try:
                SPARK_settings = yaml.load(spark_yaml)

        except yaml.YAMLError as exc:
                print exc




#load settings.yaml for elastic search
with open("../../yml_folder/ES.yaml", 'r') as ES_yaml:
        try:
                ES_settings = yaml.load(ES_yaml)

        except yaml.YAMLError as exc:
                print exc




# Set the configuration for Elasticsearch 
es =  Elasticsearch(ES_settings['ES_hosts'], http_auth=(ES_settings['ES_user'], ES_settings['ES_password']), verify_certs=False)

ES_NODES = ES_settings['ES_hosts']

ES_INDEX = ES_settings['ES_index']

ES_TYPE = ES_settings['ES_type']

ES_RESOURCE = '/'.join([ES_INDEX,ES_TYPE])

def create_es_index():	
	analyszer_setting = {
      "char_filter": {
        "zwj_char_filter": {
          "type": "mapping",
          "mappings": [ 
            "\\u200D=>"
          ]
        },
        "emoticons_char_filter": {
          "type": "mapping",
          "mappings_path": "analysis/emoticons.txt"
        }
      },
      "filter": {
        "english_emoji": {
          "type": "synonym",
          "synonyms_path": "analysis/cldr-emoji-annotation-synonyms-en.txt" 
        },
        "punctuation_and_modifiers_filter": {
          "type": "pattern_replace",
          "pattern": "\\p{Punct}|\\uFE0E|\\uFE0F|\\uD83C\\uDFFB|\\uD83C\\uDFFC|\\uD83C\\uDFFD|\\uD83C\\uDFFE|\\uD83C\\uDFFF",
          "replace": ""
        },
        "remove_empty_filter": {
          "type": "length",
          "min": 1
        }
      },
      "analyzer": {
        "english_with_emoji": {
          "char_filter": ["zwj_char_filter", "emoticons_char_filter"],
          "tokenizer": "whitespace",
          "filter": [
            "lowercase",
            "punctuation_and_modifiers_filter",
            "remove_empty_filter",
            "english_emoji"
          ]
        }
      }
    } 



	es_mapping = {"venmo_data":{
		"properties":{
			"actor_id":{"type":"string"},
			"actor_name":{"type":"string"},
			"target_id":{"type":"string"},
			"target_name":{"type":"string"},
			"message":{"type":"string","analyzer": "english_with_emoji"},
			"time_sended":{"type":"date"},
			 	}
			}
		}
	es_settings = {'number_of_shards':4, 'number_of_replicas': 0, 'refresh_interval': '-1', 'index.translog.flush_threshold_size': '1gb',"analysis":analyszer_setting}
	response = es.indices.create(index=ES_INDEX, body={'settings': es_settings, 'mappings': es_mapping})

if not es.indices.exists(ES_INDEX):
	create_es_index()

es_conf = {'es.nodes': ES_NODES, 'es.resource': ES_RESOURCE, 'es.port' : '9200','es.net.http.auth.user':ES_settings['ES_user'],'es.net.http.auth.pass': ES_settings['ES_password']}



# Set the configuration for Spark
conf = SparkConf().setAppName("ExperimentStats").setMaster(SPARK_settings['master_node'])
sc = SparkContext(conf=conf)


# Read data from S3
#read_rdd  = sc.textFile("s3n://venmo-json/2017_01/venmo_2017_01_01.json")
read_rdd  = sc.textFile("s3n://venmo-json/2017*/*.json")




# Function that do filtering and extract the actor_id, target_id, timestamp and message from raw data
def get_actor_target(line):
	field = json.loads(line)
	try:
		target_id = field["transactions"][0]['target']['id']
		actor_id = field['actor']['id']
		actor_name = field['actor']['username']
		target_name = field["transactions"][0]['target']['username']
		if  target_id:
			doc = {'actor_id': actor_id, 'actor_name': actor_name, 'target_id': target_id,'target_name': target_name,'message': field['message'],'time_sended': field['updated_time']}


			save = {"_index": ES_INDEX,
			"_type": ES_TYPE,
			"_source": doc
				 }
			print ES_INDEX
			return [save]
		else:
			return []
	except:
		return []

# Distribute the map function to process the data and use API function 'saveAsNewAPIHadoopFile' to save the data to elasticsearch database.


def BulkES(partition):
	es =  Elasticsearch(ES_settings['ES_nodes'], http_auth=(ES_settings['ES_user'], ES_settings['ES_password']), verify_certs=False)
	for success, info in helpers.parallel_bulk(es, partition,thread_count=4, chunk_size = 100):
    		if not success:
        		print 'A document failed:', info

read_rdd.flatMap(get_actor_target).foreachPartition(BulkES)
