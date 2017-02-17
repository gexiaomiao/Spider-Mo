from flask import jsonify
from app import app
from flask import render_template,request,redirect
import json
import os
from random import randint
from elasticsearch import Elasticsearch
import networkx as nx
from networkx.readwrite import json_graph
import time
from datetime import datetime
import yaml




#load settings.yaml
with open("../../../yml_folder/ES.yaml", 'r') as ES_yaml:

    try:

        ES_settings = yaml.load(ES_yaml)

    except yaml.YAMLError as exc:

        print(exc)


##setting up connections to elasticsearch	


hosts=[ES_settings['ES_hosts']]
es = Elasticsearch(
       	    		hosts,
            		port=9200,
	    		http_auth=(ES_settings['ES_user'], ES_settings['ES_password']), 
	    		verify_certs=False,
            		sniff_on_start=True,    # sniff before doing anything
            		sniff_on_connection_fail=True,    # refresh nodes after a node fails to respond
            		sniffer_timeout=60, # and also every 60 seconds
            		timeout=15

)

			



# search pages
@app.route('/')
def search():
  return render_template("search.html")



@app.route('/slides')
def slides():
  return redirect("https://goo.gl/LY0h9n")



@app.route('/github')
def github():
  return redirect("https://github.com/gexiaomiao/Spider-Mo")





# search post pages
@app.route("/", methods=['POST'])
def search_post():
	# get the data from post : keyword and time range
	search_text = request.form["search"].split( )[0].lower()
	starttime =  datetime.strptime(request.form['starttime'], '%m/%d/%Y %I:%M %p').strftime("%Y-%m-%dT%H:%M:%SZ")
	endtime =  datetime.strptime(request.form['endtime'], '%m/%d/%Y %I:%M %p').strftime("%Y-%m-%dT%H:%M:%SZ")
	searchtype = request.form["querytype"]

	# Elasticsearch query:	
	try:
		response = es.search(index=ES_settings['ES_index'],scroll = '2m', body={
		"query": {
  			 "bool": {
    				 "filter":[
       						{searchtype: { "message": search_text}},
       						{"range":{"time_sended":{"gte":starttime,"lte":endtime}} }
       					]
     				}
    			},
		"size" : 9999,
		 })
        except:
                jsonrespons = []
                return render_template("search.html")


        if response['timed_out'] == True :
                jsonresponse =[]


        else:
		search_results =   response['hits']['hits']
		total_took = response['took']
		
		#if the size is larger than 9999, do the scroll process until get the all match document
	
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
                
		# return all the match results in JSON format

		jsonresponse = [X['_source'] for X in search_results ]
	
        # If the search result is none, reload the page.
	if not jsonresponse:
                return render_template("search.html")


	#define the funtion that build graph based on the edges information 
	def add_edge_json(G,edge_json):
		for edge_add in edge_json:
                        actor = edge_add['actor_id']
                        target = edge_add['target_id']
                       	# add new node when first see it 
			if not G.has_node(actor):
                                G.add_node(actor,{"name" : edge_add['actor_name']})
                        if not G.has_node(target):
                                G.add_node(target,{"name" :  edge_add['target_name']})
			# count the weight for edges
                        if G.has_edge(actor,target):
                                G[actor][target]['weight'] += 1
                        else:
                                G.add_edge(actor,target,weight =1)
                return G

	#build the graph with edges from elasticsearch results 
	FG=nx.Graph()
	add_edge_json(FG,jsonresponse)
	
	# Calculate the degree for each nodes
	degree_sequence=sorted(nx.degree(FG).values(),reverse=True)

	# Calculate the top 10 users that have most degrees relate to the topic
	Degree_rank = [{"user_name": FG.node[X[0]]['name'],"degrees": X[1]} for X in sorted(nx.degree(FG).iteritems(),key = lambda(k,v):(-v,k))[:10]]
	
	# Generate the bar plot for Degrees. Use logithm scale for y axis. To avoid the infinite small, here I set the min value is 0.01
	degree_bar = []
        if max(degree_sequence) > 20:
                for i in range(1,20):
                        degree_bar.append([str(i),max(degree_sequence.count(i),0.01)])

                degree_bar.append(['20+', sum(x >= 20 for x in degree_sequence)])

        else :
                for i in range(1,max(degree_sequence)+1):
                        degree_bar.append([str(i),max(degree_sequence.count(i),0.01)])

	
	# Calculate the connected component
	component_all =  nx.connected_components(FG)
	
	# Calculate the number of nodes for each components
	component_count = sorted([len(X) for X in  component_all],reverse = True)
	
	component_all =  nx.connected_components(FG)	
        component_bar = []

	# Generate the bar plot for Community size:
        if max(component_count) > 20:
                for i in range(2,20):
                        component_bar.append([str(i),max(component_count.count(i),0.01)])

                component_bar.append(['20+', sum(x >= 20 for x in component_count)])

        else :
                for i in range(2,max(component_count)+1):
                        component_bar.append([str(i),max(component_count.count(i),0.01)])



	# Find the 10 largest communities
	list_10 = [X for X in component_all if len(X)>=component_count[min(10,len(component_count)-1)]][:10]

	# Build the subgraph for the top 10 largerst commnuities. label each nodes and  the size of each nodes is determined by their degrees.
	list_10_all = []
	for i,X in enumerate(list_10):
		for Y in X:
			FG.node[Y]['group']=i
			FG.node[Y]['size'] = FG.degree(Y)
		list_10_all.extend(X)

	FFG = FG.subgraph(list_10_all)


	# Count the number of nodes, edges and the density for the graph
	num_nodes,num_edges = nx.number_of_nodes(FG),nx.number_of_edges(FG)
	density_val = nx.density(FG)

	
	# Save graph into json file for D3 to display
        graphj  = json_graph.node_link_data(FFG)
        for i in range(len(graphj['links'])):
                graphj ['links'][i]['source'] = graphj['nodes'][graphj ['links'][i]['source']]['id']
                graphj ['links'][i]['target'] = graphj['nodes'][graphj ['links'][i]['target']]['id']
        filename = "app/static/D3_graph_data.json"
        if not os.path.exists(os.path.dirname(filename)):
                try:
                        os.makedirs(os.path.dirname(filename))
                except OSError as exc: # Guard against race condition
                        if exc.errno != errno.EEXIST:
                                raise
        with open(filename, 'w') as outfile:
                json.dump(graphj, outfile)


	
	
	dict_all = {'words':search_text,'time_from':starttime,'time_end':endtime,'numnodes':num_nodes,'numedges':num_edges,'densityval':density_val,'example':jsonresponse[0:20],'top_degree':Degree_rank,'componentbar':component_bar,'degreebar':degree_bar}
	
	return render_template("graphop.html", output = dict_all)


