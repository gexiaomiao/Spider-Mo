import json, tarfile, os
import pandas
from pyspark.sql import DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
#import boto3, re

from datetime import datetime
import time


from elasticsearch import Elasticsearch


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



def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)




conf = SparkConf().setAppName("ExperimentStats").setMaster("spark://ip-172-31-3-4:7077")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)



#file = sc.textFile("hdfs:///tmp/venmo_test")

#read_rdd = sc.textFile("hdfs://ip-172-31-3-4:9000/tmp/venmotest.json")
#read_rdd  = sc.textFile("s3n://venmo-json/2017_01/venmo_2017_01_01.json")
read_rdd  = sc.textFile("s3n://venmo-json/2017_01/*")



def get_actor_target(line):
	field = json.loads(line)
	actor_id = field['actor']['id']

	try:
		#target_id = field['mentions'][0]['user']['id']
		target_id = field["transactions"][0]['target']['id']
		actor_id = field['actor']['id']
		#print "hello"
		#ids  = set(target_id)
		if  target_id:
			doc = {'actor_id': actor_id, 'target_id': target_id,'message': field['message'],'time_sended': field['updated_time']}
		        #doc = {"venmo_data":{"properties":{'actor_id': actor_id, 'target_id': target_id,'message': field['message'],'time_stamp':time.asctime(field['updated_time'])}}}
		    	#print "hello"
			#doc = {"time_sended" : "2017-01-06T23:31:02Z", "message" : "See you in feb @Sean-Geier", "target_id" : "701661", "actor_id" : "6852670"}
			return [('key', doc)]
			#res = es.index(index= ES_INDEX, doc_type= "inputs",body=doc )
		else:

			doc = {"time_sended" : "2017-01-06T23:31:02Z", "message" : "See you in feb @Sean-Geier", "target_id" : "701661", "actor_id" : "6852670"}

			#return ('key', doc)
			return []
	except:
		i=1
		doc = {"time_sended" : "2017-01-06T23:31:02Z", "message" : "See you in feb @Sean-Geier", "target_id" : "701661", "actor_id" : "6852670"}

		#return ('key', doc)
		return []


es_conf = {'es.nodes': ES_NODES, 'es.resource': ES_RESOURCE, 'es.port' : '9200','es.net.http.auth.user':'elastic','es.net.http.auth.pass':'changeme'}
#
#es_conf = {'es.nodes': ES_NODES, 'es.resource': ES_RESOURCE, 'es.port' : '9200',  'es.batch.write.retry.count': '-1', 'es.batch.size.bytes': '0.05mb'}
#es_conf = {'es.resource': ES_INDEX}		 

#df_read = rdd.flatMap(get_actor_target).reduceByKey(lambda a, b : a.union(b))

read_rdd.flatMap(get_actor_target).saveAsNewAPIHadoopFile(path='-', \
                                            outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat', \
                                            keyClass='org.apache.hadoop.io.NullWritable', \
                                            valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable', \
                                            conf=es_conf)


   #filteredCompaniesRDD.saveAsNewAPIHadoopFile(path='-', \
   #                                         outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat', \
   #                                         keyClass='org.apache.hadoop.io.NullWritable', \
   #                                         valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable', \
#                                         conf=es_company_conf)


#df_read.saveAsTextFile('/tmp/sparktestresult')

#print_test1 = test_1.map(lambda row: (str(row[0])))

#print print_test1

print"==================================================="



#file = sqlContext.jsonRDD("hdfs://ip-172-31-3-4:9000/tmp/venmotest.json")
#file.first()

print "done"

