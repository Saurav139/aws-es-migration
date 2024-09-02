import psycopg2
import uuid  
from elasticsearch import Elasticsearch, helpers

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="datastream.cpgmsksiy1n5.us-east-2.rds.amazonaws.com",
    database="postgres",
    user="postgres",
    password="postgresql1234"
)

# Create a cursor object
cursor = conn.cursor()

# Elasticsearch connection
es = Elasticsearch(
    ["https://e7d3fa644efb4d53a2acd19091d22232.us-east-2.aws.elastic-cloud.com:443"],
  basic_auth=('elastic', 'VYkm5wh0cvdUaQZZpEaR3nOx')
)

# Define and execute a Select all query 
query = "SELECT * FROM book"
cursor.execute(query)
rows = cursor.fetchall()

# Define a generator function to create Elasticsearch documents
def generate_docs():
	for row in rows:
        
        	doc = {
            "_index": "elastic-project",
            "_id": str(uuid.uuid4()),
            "_source": {
                "title": row[0],
                "description": row[1],
                "authors": row[2],
                "image": row[3],
                "preview_link": row[4],
                "publisher": row[5],
                "published_date": row[6],
                "info_link": row[7],
                "categories": row[8],
                "ratings_count": row[9]
       		}
        }
        	yield doc

# Bulk index the documents to Elasticsearch
helpers.bulk(es, generate_docs())

# Close the cursor and connection
cursor.close()
conn.close()

print("Data transfer completed successfully.")
