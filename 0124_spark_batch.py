import csv, tarfile, os
#import pyspark_cassandra
#from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
#import boto3, re


conf = SparkConf().setAppName("ExperimentStats").setMaster("spark://ip-172-31-3-4:7077")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


RESULTS_DIR='hdfs://ec2-52-89-35-171.us-west-2.compute.amazonaws.com:9000/datamill/'
DATABASE = 'datamill'

#file = sc.textFile("hdfs:///tmp/venmo_test")

file = sqlContext.read.json("hdfs://ip-172-31-3-4:9000/tmp/venmotest.json")

file.printSchema()

print "done"
