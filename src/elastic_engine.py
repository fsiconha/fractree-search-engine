#!/usr/bin/env python3
"""
Fractal Elasticsearch Engine with Dynamic Hierarchical Clustering

This module provides an Elasticsearch engine that automatically computes
hierarchical cluster labels for documents using a recursive (fractal) clustering
algorithm. Documents are grouped based on similarity (using Jaccard similarity)
and assigned labels that represent their branch in the fractal tree.
"""

from elasticsearch import Elasticsearch, helpers

# --- Similarity and Clustering Helpers --- #

def jaccard_similarity(set1, set2):
    """Compute the Jaccard similarity between two sets."""
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0

class Document:
    """
    Represents a document with an identifier and text content.
    The document precomputes a set of words from its text.
    """
    def __init__(self, doc_id, text):
        self.doc_id = doc_id
        self.text = text
        self.words = set(text.lower().split())

    def __repr__(self):
        return f"Document({self.doc_id})"

class Cluster:
    """
    Represents a cluster of documents in a fractal (recursive) tree.
    If the number of documents exceeds max_documents, the cluster splits
    into two child clusters.
    """
    def __init__(self, documents, label='root', max_documents=3, level=0):
        self.documents = documents
        self.label = label
        self.max_documents = max_documents
        self.level = level
        self.children = []
        # If more documents than allowed, split the cluster recursively.
        if len(documents) > self.max_documents and len(documents) >= 2:
            self.split_cluster()

    def split_cluster(self):
        """
        Split the cluster into two subclusters using a simple approach.
        Two seed documents (the first two) are used to partition the documents
        based on Jaccard similarity.
        """
        seed1 = self.documents[0]
        seed2 = self.documents[1]
        group1 = []
        group2 = []
        for doc in self.documents:
            sim1 = jaccard_similarity(doc.words, seed1.words)
            sim2 = jaccard_similarity(doc.words, seed2.words)
            if sim1 >= sim2:
                group1.append(doc)
            else:
                group2.append(doc)
        self.children = [
            Cluster(group1, label=self.label + '.0', max_documents=self.max_documents, level=self.level + 1),
            Cluster(group2, label=self.label + '.1', max_documents=self.max_documents, level=self.level + 1)
        ]

    def get_cluster_mapping(self):
        """
        Recursively collect a mapping of document IDs to their cluster labels.
        """
        mapping = {}
        if self.children:
            for child in self.children:
                mapping.update(child.get_cluster_mapping())
        else:
            for doc in self.documents:
                mapping[doc.doc_id] = self.label
        return mapping

class FractalClustering:
    """
    Computes hierarchical clusters for a list of documents.
    """
    def __init__(self, max_documents=3):
        self.max_documents = max_documents

    def compute_clusters(self, documents):
        root_cluster = Cluster(documents, label='root', max_documents=self.max_documents)
        return root_cluster.get_cluster_mapping()

# --- Elasticsearch Engine with Dynamic Clustering --- #

class FractalElasticsearchEngine:
    """
    Elasticsearch engine that automatically computes hierarchical cluster labels
    for documents based on their similarity.
    """
    def __init__(self, index_name='fractal_search', host='localhost', port=9200, max_documents=3):
        self.index_name = index_name
        self.es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])
        self.documents = []  # In-memory storage for documents.
        self.max_documents = max_documents
        self.create_index()

    def create_index(self):
        """
        Create the Elasticsearch index with mappings for doc_id, text, and cluster.
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

    def add_document(self, doc_id, text):
        """
        Add a document (without cluster info) to the in-memory list.
        The cluster label will be computed when building the index.
        """
        doc = Document(doc_id, text)
        self.documents.append(doc)

    def build_index(self):
        """
        Compute hierarchical clusters dynamically and index all documents into Elasticsearch
        with their computed cluster labels.
        """
        # Compute clusters using the fractal clustering algorithm.
        clustering = FractalClustering(max_documents=self.max_documents)
        cluster_mapping = clustering.compute_clusters(self.documents)

        # Bulk index documents with their computed cluster labels.
        actions = []
        for doc in self.documents:
            doc_cluster = cluster_mapping.get(doc.doc_id, 'root')
            document_body = {
                "doc_id": doc.doc_id,
                "text": doc.text,
                "cluster": doc_cluster
            }
            action = {
                "_index": self.index_name,
                "_id": doc.doc_id,
                "_source": document_body
            }
            actions.append(action)
        helpers.bulk(self.es, actions)
        # Refresh the index to make documents available immediately for search.
        self.es.indices.refresh(index=self.index_name)

    def search(self, query, cluster_filter=None):
        """
        Search for documents matching the query.
        Optionally, filter by a specific cluster label.
        """
        query_body = {
            "query": {
                "bool": {
                    "must": {
                        "match": {"text": query}
                    }
                }
            }
        }
        if cluster_filter:
            query_body["query"]["bool"]["filter"] = {"term": {"cluster": cluster_filter}}
        return self.es.search(index=self.index_name, body=query_body)

# --- Example Usage --- #

def main():
    engine = FractalElasticsearchEngine(max_documents=2)  # Lower threshold to force splitting.
    
    # Add documents without specifying a cluster.
    engine.add_document("doc1", "Python is a versatile programming language")
    engine.add_document("doc2", "Python is used for web development and data analysis")
    engine.add_document("doc3", "Java and C++ are traditional programming languages")
    engine.add_document("doc4", "Data science involves statistics and machine learning")
    engine.add_document("doc5", "Web development uses HTML, CSS, and JavaScript")
    engine.add_document("doc6", "Automation scripts in Python make tasks easier")
    
    # Compute clusters and index documents.
    engine.build_index()
    
    # Perform a search (optionally, filter by a computed cluster, e.g., 'root.0')
    query = "Python programming"
    results = engine.search(query)
    print(f"Search results for query: '{query}'")
    for hit in results['hits']['hits']:
        source = hit['_source']
        print(f"Document: {source['doc_id']}, Cluster: {source['cluster']}, Score: {hit['_score']:.2f}")
    
if __name__ == "__main__":
    main()
