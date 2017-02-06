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
import networkx as nx
from networkx.readwrite import json_graph

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

@app.route('/d3test')
def d3test():
 return render_template("graphD3.html")
			
@app.route('/')

@app.route('/realtime')
def realtime():
 return render_template("realtime.html")

@app.route('/index')
def index():
  user = { 'nickname': 'Supeng' } # fake user
  mylist = [1,2,3,4]
  return render_template("index.html", title = 'Home',user = user, mylist = mylist)



@app.route('/email')
def email():
  return render_template("email.html")


@app.route("/email", methods=['POST'])
def email_post():
	search_text = request.form["search"]
	date = request.form["date"]

 #email entered is in emailid and date selected in dropdown is in date variable respectively 
	response = es.search(index='test_venmo_all',scroll = '2m', body={
"query": {
   "bool": {
     "filter":[
       {"term": { "message": search_text}},
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
		total_took = response['took']
		if response['hits']['total'] > 9999:
                        sid = response['_scroll_id']
                        scroll_size = response['hits']['total']
			while (scroll_size > 0 ):
				print "Scrolling..."
				response = es.scroll(scroll_id = sid, scroll ='2m')
                                sid = response['_scroll_id']
                                scroll_size = len(response['hits']['hits'])
                                print "scroll size: " + str(scroll_size)
                                total_took += response['took']
                                search_results = search_results + response['hits']['hits']

		
		print "searching time is:", total_took
                

		jsonresponse = [X['_source'] for X in search_results ]
		
	def add_edge_json(G,edge_json):
		for edge_add in edge_json:
                        actor = edge_add['actor_id']
                        target = edge_add['target_id']
                       	if G.has_node(actor):
                                G.add_node(actor)
                        if G.has_node(target):
                                G.add_node(target)

                        if G.has_edge(actor,target):
                                G[actor][target]['weight'] += 1
                        else:
                                G.add_edge(actor,target,weight =1)
                return G
	FG=nx.Graph()
	add_edge_json(FG,jsonresponse)
	filename = "app/static/dogg.gexf"
	if not os.path.exists(os.path.dirname(filename)):
    		try:
        		os.makedirs(os.path.dirname(filename))
    		except OSError as exc: # Guard against race condition
        		if exc.errno != errno.EEXIST:
            			raise
	nx.write_gexf(FG, filename)
	degree_sequence=sorted(nx.degree(FG).values(),reverse=True)
	num_nodes,num_edges = nx.number_of_nodes(FG),nx.number_of_edges(FG)
	density_val = nx.density(FG)
	dict_all = {'words':search_text,'time_from':date,'numnodes':num_nodes,'numedges':num_edges,'densityval':density_val,'example':jsonresponse[0:10]}
	#print dict_all
	return render_template("graphop.html", output = dict_all)


