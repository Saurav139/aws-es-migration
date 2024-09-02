from elasticsearch import Elasticsearch, helpers
import os
import random
import string
from datetime import datetime

es = Elasticsearch(
    [os.getenv('ES_HOST')],
    http_auth=(os.getenv('ES_USERNAME'), os.getenv('ES_PASSWORD'))
)

def random_title(length=20):

    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def process_records(event, context):
    # Query to get records from 2400 to 4400
    query_body = {
        "from": 2400,  
        "size": 2000, 
        "query": {
            "match_all": {}
        }
    }

    response = es.search(index=os.getenv('ES_INDEX'), body=query_body)
    hits = response['hits']['hits']

    actions = []
    for hit in hits:
        doc = hit['_source']

        # Change the title to a random title
        doc['title'] = random_title()

       
        action = {
            '_op_type': 'index',  
            '_index': os.getenv('ES_INDEX'),
            '_source': doc  
        }
        actions.append(action)

    # Perform the bulk operation to insert the modified records
    if actions:
        helpers.bulk(es, actions)

    return {
        'statusCode': 200,
        'body': f'Processed and inserted {len(actions)} records with new IDs.'
    }
