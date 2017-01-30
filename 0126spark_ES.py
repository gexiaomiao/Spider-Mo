import json, tarfile, os
import pandas
from pyspark.sql import DataFrame
#import pyspark_cassandra
#from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
#import boto3, re

from elasticsearch import Elasticsearch

# by default we don't sniff, ever
#es = Elasticsearch()
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


#doc = {'timeStamp': 201709021230, 'wind': 20, 'wban': 528}
#es.index(index='test', doc_type='inputs',body=doc )




def unionAll(*dfs):
        return reduce(DataFrame.unionAll, dfs)

worker_public_dns = ['172.31.3.4']
CASSANDRA_KEYSPACE = 'vtest'

#cascluster = Cluster(worker_public_dns)
#casSession = cascluster.connect(CASSANDRA_KEYSPACE)


conf = SparkConf().setAppName("ExperimentStats").setMaster("spark://ip-172-31-3-4:7077")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


RESULTS_DIR='hdfs://ec2-52-89-35-171.us-west-2.compute.amazonaws.com:9000/datamill/'
DATABASE = 'datamill'

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
                        print "=========="
                        es = Elasticsearch(['35.165.191.58'], http_auth=('elastic', 'changeme'), verify_certs=False)
                        doc = {'actor_id': actor_id, 'target_id': target_id,'message': field['message']}
                        es.index(index='test_2', doc_type='inputs',body=doc )
                        #return [(actor_id,ids)]
                #else:
                        #return[]
        except:
                i=1
                #return[]       






#df_read = rdd.flatMap(get_actor_target).reduceByKey(lambda a, b : a.union(b))
df_read = rdd.map(get_actor_target)




df_read.saveAsTextFile('/tmp/sparktestout')

#print_test1 = test_1.map(lambda row: (str(row[0])))

#print print_test1

print"==================================================="



#file = sqlContext.jsonRDD("hdfs://ip-172-31-3-4:9000/tmp/venmotest.json")
#file.first()

print "done"

