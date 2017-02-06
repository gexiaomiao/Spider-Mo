import json, tarfile, os
from pyspark import SparkConf, SparkContext
from datetime import datetime
import time
from elasticsearch import Elasticsearch

# Set the configuration for Elasticsearch 
es =  Elasticsearch(['52.35.1.180'], http_auth=('elastic', 'changeme'), verify_certs=False)

ES_NODES = 'ec2-52-35-1-180.us-west-2.compute.amazonaws.com'

ES_INDEX = 'test_venmo_6'

ES_TYPE = 'inputs'

ES_RESOURCE = '/'.join([ES_INDEX,ES_TYPE])


def create_es_index():
	es_mapping = {"venmo_data":{"properties":{"actor_id":{"type":"string"},"target_id":{"type":"string"},"message":{"type":"string","fielddata": True },"time_sended":{"type":"date"}}}}
	es_settings = {'number_of_shards':3, 'number_of_replicas': 2, 'refresh_interval': '1s', 'index.translog.flush_threshold_size': '1gb'}
	response = es.indices.create(index=ES_INDEX, body={'settings': es_settings, 'mappings': es_mapping})

if not es.indices.exists(ES_INDEX):
	create_es_index()



# Set the configuration for Spark
conf = SparkConf().setAppName("ExperimentStats").setMaster("spark://ip-172-31-3-4:7077")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# Read data from S3
read_rdd  = sc.textFile("s3n://venmo-json/2017_01/*")



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


es_conf = {'es.nodes': ES_NODES, 'es.resource': ES_RESOURCE, 'es.port' : '9200','es.net.http.auth.user':'elastic','es.net.http.auth.pass':'changeme'}

read_rdd.flatMap(get_actor_target).saveAsNewAPIHadoopFile(path='-', \
                                            outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat', \
                                            keyClass='org.apache.hadoop.io.NullWritable', \
                                            valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable', \
                                            conf=es_conf)



print "done"

