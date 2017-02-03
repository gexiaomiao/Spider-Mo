# jsonify creates a json representation of the response
from flask import jsonify
from app import app
from flask import render_template,request
#import Math
import json
import os
from random import randint
#import the elasticsearch drive
from elasticsearch import Elasticsearch

##setting up connections to elasticsearch	

hosts=["ec2-52-35-1-180.us-west-2.compute.amazonaws.com"]
es = Elasticsearch(
       	    		hosts,
            		port=9200,
	    		http_auth=('elastic', 'changeme'), 
	    		verify_certs=False,
            		sniff_on_start=True,    # sniff before doing anything
            		sniff_on_connection_fail=True,    # refresh nodes after a node fails to respond
            		sniffer_timeout=60, # and also every 60 seconds
            		timeout=15
			)
@app.route('/')

@app.route('/realtime')
def realtime():
 return render_template("realtime.html")

@app.route('/index')
def index():
  user = { 'nickname': 'Supeng' } # fake user
  mylist = [1,2,3,4]
  return render_template("index.html", title = 'Home',user = user, mylist = mylist)


@app.route('/api/<searchtext>/')
def get_edge(searchtext):
	response = es.search(index='test_venmo_6', body={
"query": {
   "bool": {
     "filter":[
       {"match": { "message": searchtext }},
       {"range":{
          "time_sended":{"gte":"2017-01-01T00:00:00Z"}} }
       ]
     }
    },
"size" : 9999 })
	if response['timed_out'] == True :
		jsonresponse =[]
	else:
		search_results =   response['hits']['hits']
		jsonresponse = [X['_source'] for X in search_results ]
	
	return jsonify(edges=jsonresponse)

@app.route('/email')
def email():
  return render_template("email.html")


@app.route("/email", methods=['POST'])
def email_post():
	search_text = request.form["search"]
	date = request.form["date"]

 #email entered is in emailid and date selected in dropdown is in date variable respectively 
	response = es.search(index='test_venmo_6', body={
"query": {
   "bool": {
     "filter":[
       {"match": { "message": search_text}},
       {"range":{
          "time_sended":{"gte":date}} }
       ]
     }
    },
"size" : 9999,
"sort" :"time_sended"
 })
        if response['timed_out'] == True :
                jsonresponse =[]
        else:
                search_results =   response['hits']['hits']
                jsonresponse = [X['_source'] for X in search_results ]
		actorset = [X['_source']['actor_id'] for X in search_results ]
		targetset = [X['_source']['target_id'] for X in search_results ]
		nodeset = set(actorset+targetset)
		nodelist = [{"id":X,"label":X,"x": randint(0,100000)/100.0,"y":randint(0,100000)/100.0,"size":10,"color":"rgb(255,204,102)"} for X in nodeset]
		edgelist = [{"id": str(i),"source":X['actor_id'],"target":X['target_id']} for i, X in enumerate(jsonresponse)]
		jsondata = {"nodes":nodelist,"edges":edgelist}
		#filename = "app/static/"+search_text+'.json'
		filename = "app/static/dogg.json"
		if not os.path.exists(os.path.dirname(filename)):
    			try:
        			os.makedirs(os.path.dirname(filename))
    			except OSError as exc: # Guard against race condition
        			if exc.errno != errno.EEXIST:
            				raise
		with open(filename, 'w') as outfile:
    			json.dump(jsondata, outfile)	
	return render_template("graphop.html",output = search_text)


