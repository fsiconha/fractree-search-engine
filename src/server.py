"""
Flask server for the Fractal Elasticsearch Search Engine.

This server exposes endpoints to index documents and perform searches.
"""

from flask import Flask, request, jsonify
from elastic_engine import FractalElasticsearchEngine

app = Flask(__name__)

# Create an instance of the search engine.
engine = FractalElasticsearchEngine(index_name='fractal_search')


@app.route('/index', methods=['POST'])
def index_document():
    """
    Endpoint to index a single document.
    Expects JSON with 'doc_id', 'text', and optionally 'cluster'.
    """
    data = request.get_json()
    doc_id = data.get('doc_id')
    text = data.get('text')
    cluster = data.get('cluster', 'root')

    if not doc_id or not text:
        return jsonify({'error': 'doc_id and text are required.'}), 400

    engine.add_document(doc_id, text, cluster)
    return jsonify({'message': 'Document indexed successfully.'}), 201


@app.route('/bulk_index', methods=['POST'])
def bulk_index():
    """
    Endpoint to bulk index documents.
    Expects a JSON array of documents.
    Each document should have 'doc_id', 'text', and optionally 'cluster'.
    """
    data = request.get_json()

    if not isinstance(data, list):
        return jsonify({'error': 'Expected a list of documents.'}), 400

    engine.bulk_add_documents(data)
    return jsonify({'message': 'Documents indexed successfully.'}), 201


@app.route('/search', methods=['GET'])
def search():
    """
    Endpoint to search for documents.
    Query parameters:
      - query (required): the search query string.
      - cluster (optional): filter results by cluster.
    """
    query = request.args.get('query')
    cluster_filter = request.args.get('cluster')

    if not query:
        return jsonify({'error': 'Query parameter is required.'}), 400

    results = engine.search(query, cluster_filter)
    hits = results.get('hits', {}).get('hits', [])

    # Format the search results.
    docs = [
        {
            'doc_id': hit['_source']['doc_id'],
            'text': hit['_source']['text'],
            'cluster': hit['_source']['cluster'],
            'score': hit['_score']
        }
        for hit in hits
    ]
    return jsonify({'results': docs})


if __name__ == '__main__':
    # Run the server on all interfaces at port 5000.
    app.run(host='0.0.0.0', port=5000, debug=True)
