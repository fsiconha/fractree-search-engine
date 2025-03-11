"""
Fractal Elasticsearch Engine Module

This module provides a class for interfacing with Elasticsearch using a fractal
indexing approach. Documents are indexed with a cluster identifier to simulate
fractal grouping.
"""

from elasticsearch import Elasticsearch, helpers


class FractalElasticsearchEngine:
    """
    Fractal Elasticsearch Engine for indexing documents and performing searches.
    """

    def __init__(self, index_name='fractal_search', host='localhost', port=9200):
        """
        Initialize the Elasticsearch client and create the index.

        Parameters:
            index_name (str): The name of the Elasticsearch index.
            host (str): Elasticsearch host.
            port (int): Elasticsearch port.
        """
        self.index_name = index_name
        self.es = Elasticsearch([{'host': host, 'port': port}])
        self.create_index()

    def create_index(self):
        """
        Create the Elasticsearch index with mappings.
        """
        mapping = {
            "mappings": {
                "properties": {
                    "doc_id": {"type": "keyword"},
                    "text": {"type": "text"},
                    "cluster": {"type": "keyword"}
                }
            }
        }
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=mapping)

    def add_document(self, doc_id, text, cluster='root'):
        """
        Index a single document with an associated cluster identifier.

        Parameters:
            doc_id (str): Document identifier.
            text (str): Document text.
            cluster (str): Cluster label for fractal grouping.
        """
        doc = {
            "doc_id": doc_id,
            "text": text,
            "cluster": cluster
        }
        self.es.index(index=self.index_name, id=doc_id, body=doc)

    def bulk_add_documents(self, docs):
        """
        Bulk index documents.

        Parameters:
            docs (list): List of dictionaries with keys 'doc_id', 'text',
                         and optionally 'cluster'.
        """
        actions = []
        for doc in docs:
            action = {
                "_index": self.index_name,
                "_id": doc['doc_id'],
                "_source": doc
            }
            actions.append(action)
        helpers.bulk(self.es, actions)

    def search(self, query, cluster_filter=None):
        """
        Search for documents matching the query. Optionally filter by cluster.

        Parameters:
            query (str): The search query.
            cluster_filter (str): Optional cluster filter.

        Returns:
            dict: Elasticsearch search results.
        """
        query_body = {
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "text": query
                        }
                    }
                }
            }
        }
        if cluster_filter:
            query_body["query"]["bool"]["filter"] = {
                "term": {"cluster": cluster_filter}
            }
        return self.es.search(index=self.index_name, body=query_body)
