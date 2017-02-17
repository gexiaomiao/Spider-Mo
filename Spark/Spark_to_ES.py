import json, tarfile, os
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
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
	es_mapping = {"venmo_data":{"properties":{"actor_id":{"type":"string"},"target_id":{"type":"string"},"message":{"type":"string","fielddata": True },"time_sended":{"type":"date"}}}}
	es_settings = {'number_of_shards':3, 'number_of_replicas': 2, 'refresh_interval': '1s', 'index.translog.flush_threshold_size': '1gb'}
	response = es.indices.create(index=ES_INDEX, body={'settings': es_settings, 'mappings': es_mapping})

if not es.indices.exists(ES_INDEX):
	create_es_index()

es_conf = {'es.nodes': ES_NODES, 'es.resource': ES_RESOURCE, 'es.port' : '9200','es.net.http.auth.user':ES_settings['ES_user'],'es.net.http.auth.pass': ES_settings['ES_password']}



# Set the configuration for Spark
conf = SparkConf().setAppName("SparkProcessing").setMaster(SPARK_settings['master_node'])
sc = SparkContext(conf=conf)

# Read data from S3
read_rdd  = sc.textFile("s3n://venmo-json/*/*")



# Function that do filtering and extract the actor_id, target_id, timestamp and message from raw data
def get_actor_target(line):
	field = json.loads(line)
	actor_id = field['actor']['id']

	try:
		target_id = field["transactions"][0]['target']['id']
		actor_id = field['actor']['id']
		if  target_id:
			doc = {'actor_id': actor_id, 'target_id': target_id,'message': field['message'],'time_sended': field['updated_time']}
			return [('key', doc)]
		else:
			return []
	except:
		return []

# Distribute the map function to process the data and use API function 'saveAsNewAPIHadoopFile' to save the data to elasticsearch database.
read_rdd.flatMap(get_actor_target).saveAsNewAPIHadoopFile(path='-', \
                                            outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat', \
                                            keyClass='org.apache.hadoop.io.NullWritable', \
                                            valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable', \
                                            conf=es_conf)



print "done"

