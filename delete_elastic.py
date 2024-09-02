

from elasticsearch import Elasticsearch, helpers
import os

# Initialize the Elasticsearch client
es = Elasticsearch(
    [os.getenv('ES_HOST')],
    http_auth=(os.getenv('ES_USERNAME'), os.getenv('ES_PASSWORD'))
)

def delete_records(event, context):
    # Query to get records from 1000 to 2000 so that it doesn't affect update records
    query_body = {
        "from": 1000,  
        "size": 1000, 
        "query": {
            "match_all": {}
        }
    }

    response = es.search(index=os.getenv('ES_INDEX'), body=query_body)
    hits = response['hits']['hits']

    actions = []
    for hit in hits:
        doc_id = hit['_id']
        
        # Prepare the delete action
        action = {
            '_op_type': 'delete',
            '_index': os.getenv('ES_INDEX'),
            '_id': doc_id
        }
        actions.append(action)

    # Perform the bulk delete
    if actions:
        helpers.bulk(es, actions)

    return {
        'statusCode': 200,
        'body': f'Deleted {len(actions)} records.'
    }
