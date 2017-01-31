mport json, tarfile, os
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

ES_INDEX = 'test_venmo'



def create_es_index():
        es_mapping = {"venmo_data":{"properties":{"actor_id":{"type":"string"},"target_id":{"type":"string"},"message":{"type":"string"},"time_sended":{"type":"date"}}}}
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

rdd = sc.textFile("hdfs://ip-172-31-3-4:9000/tmp/venmotest.json")




def get_actor_target(line):
        field = json.loads(line)
        actor_id = field['actor']['id']

        try:
                target_id = field['mentions'][0]['user']['id']
                actor_id = field['actor']['id']
                #print "hello"
                #ids  = set(target_id)
                if  target_id:
                        es = Elasticsearch(['52.35.1.180'], http_auth=('elastic', 'changeme'), verify_certs=False)
                        doc = {'actor_id': actor_id, 'target_id': target_id,'message': field['message'],'time_sended': field['updated_time']}
                        #doc = {"venmo_data":{"properties":{'actor_id': actor_id, 'target_id': target_id,'message': field['message'],'time_stamp':time.asctime(field['updated_time'])}}}
                        print "hello"
                        res = es.index(index= ES_INDEX, doc_type= "inputs",body=doc )
                        #print(res['created'])
                        #return [(actor_id,ids)]
                #else:
                        #return[]
        except:
                i=1
                print "wrong"
                #return[]       


#df_read = rdd.flatMap(get_actor_target).reduceByKey(lambda a, b : a.union(b))
df_read = rdd.map(get_actor_target)

df_read.saveAsTextFile('/tmp/sparktestresult')

#print_test1 = test_1.map(lambda row: (str(row[0])))

#print print_test1

print"==================================================="



#file = sqlContext.jsonRDD("hdfs://ip-172-31-3-4:9000/tmp/venmotest.json")
#file.first()

print "done"

