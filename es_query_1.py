rom elasticsearch import Elasticsearch

class ESHelper:
        def __init__(self):
                hosts=["ec2-52-35-1-180.us-west-2.compute.amazonaws.com"]
                self.es = Elasticsearch(
                        hosts,
                        port=9200,
                        http_auth=('elastic', 'changeme'),
                        verify_certs=False,
                        sniff_on_start=True,    # sniff before doing anything
                        sniff_on_connection_fail=True,    # refresh nodes after a node fails to respond
                        sniffer_timeout=60, # and also every 60 seconds
                        timeout=15
                        )


        def test_search(self,topic):
                response = self.es.search(index='test_venmo_6', body={  "query": { "match": { "message": topic } },"size" : 9999 })
                #response = self.es.search(index='test_venmo_6', body={  "query": { "match": { "message": topic } },"_source": ["actor_id", "target_id"] })
                if response['timed_out'] == True :
                        return []
                else:
                        return response['hits']['hits']

if __name__=='__main__':
        eshelp = ESHelper()
        ahh = eshelp.test_search('mug')
        for ah in ahh:
                print ah['_source']
        print "hello"

