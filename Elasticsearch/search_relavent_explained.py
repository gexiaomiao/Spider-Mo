from elasticsearch import Elasticsearch
import sys
import datetime
import json

import yaml


#load settings.yaml
with open("../../yml_folder/ES.yaml", 'r') as ES_yaml:
	try:
        	ES_settings = yaml.load(ES_yaml)

    	except yaml.YAMLError as exc:
        	print(exc)







##setting up connections to elasticsearch	
class ESHelper:
        def __init__(self):		

		hosts=[ES_settings['ES_hosts']]
		self.es = Elasticsearch(
       	    		hosts,
            		port=9200,
	    		http_auth=(ES_settings['ES_user'], ES_settings['ES_password']), 
			verify_certs=False,
            		sniff_on_start=True,    # sniff before doing anything
            		sniff_on_connection_fail=True,    # refresh nodes after a node fails to respond
            		sniffer_timeout=60, # and also every 60 seconds
            		timeout=15

		)





        def explained_search(self,topic,time_up):
                body_query =   {
				"query": {
   					"bool":{
						"must":[
   							{"match": { "message": topic }}	
							,{"range":{"time_sended":{"gte":time_up}}}
       							]
     
    						}
				 	},
				"size" : 10
				}

		response = self.es.search(index=ES_settings['ES_index'],explain=True,body= body_query )
		return response




if __name__=='__main__':

	eshelp = ESHelper()
	topics  =  sys.argv[1]
	time_up = "2016-10-01T00:00:00Z"
	print time_up
	search_result = eshelp.explained_search(topics,time_up)
	
	print "the topic is:", topics
		
	print json.dumps(search_result, indent=4, sort_keys=True)


