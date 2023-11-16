from flask import Flask, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch(['http://localhost:9200'])

@app.route('/recommendations/<film_title>', methods=['GET'])
def get_recommendations(film_title):
    # Perform Elasticsearch search for recommendations based on the title 
    result = es.search(index='movies', body={
        "query": {
            "match": {
                "title.keyword": film_title
            }
        }
    })

    # Extract the results
    recommendations = [hit['_source'] for hit in result['hits']['hits']]

    return jsonify(recommendations)

if __name__ == '__main__':
    app.run(debug=True)
