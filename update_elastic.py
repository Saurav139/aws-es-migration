import os
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

# Initialize the Elasticsearch client with environment variables
es = Elasticsearch(
    [os.getenv('ES_HOST')],
    http_auth=(os.getenv('ES_USERNAME'), os.getenv('ES_PASSWORD'))
)

def update_records(event, context):
    # Query Elasticsearch to get the first 500 records
    query_body = {
        "size": 500,
        "query": {
            "match_all": {}
        },
        "sort": [
            {
                "title.keyword": {
                    "order": "desc"
                }
            }
        ]
    }

    response = es.search(index=os.getenv('ES_INDEX'), body=query_body)
    hits = response['hits']['hits']

    actions = []
    for hit in hits:
        doc_id = hit['_id']
        doc = hit['_source']

        # Increment the count field by 1 in case it is not null, otherwise set it to 1
        try:
            if 'ratings_count' in doc and doc['ratings_count'] is not None:
                doc['ratings_count'] = str(int(float(doc['ratings_count'])) + 1)
    
            else:
                doc['ratings_count'] = '1'
        except ValueError:
             doc['ratings_count'] = '1'


        # Prepare the update action to the index
        action = {
            '_op_type': 'update',
            '_index': os.getenv('ES_INDEX'),
            '_id': doc_id,
            'doc': {
                'ratings_count': doc['ratings_count'],
                'updated_at': datetime.utcnow().isoformat()
            }
        }
        actions.append(action)

    # Perform the bulk updateto the index
    helpers.bulk(es, actions)

    return {
        'statusCode': 200,
        'body': f'Updated {len(actions)} records.'
    }
