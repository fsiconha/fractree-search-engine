from elastic_engine import FractalElasticsearchEngine


def main():
    """
    Demonstrate the Fractal Elasticsearch Search Engine.
    """
    engine = FractalElasticsearchEngine(index_name='fractal_search')

    # Example documents with a simple fractal cluster label.
    docs = [
        {"doc_id": "doc1",
         "text": "Python is a programming language that lets you work quickly",
         "cluster": "programming"},
        {"doc_id": "doc2",
         "text": "Python is used for web development, data analysis, and scripting",
         "cluster": "programming"},
        {"doc_id": "doc3",
         "text": "Java and C++ are also popular programming languages",
         "cluster": "programming"},
        {"doc_id": "doc4",
         "text": "Data science involves statistics, machine learning, and data analysis",
         "cluster": "data"},
        {"doc_id": "doc5",
         "text": "Web development uses HTML, CSS, and JavaScript",
         "cluster": "web"},
        {"doc_id": "doc6",
         "text": "Scripting in Python can automate many tasks",
         "cluster": "programming"}
    ]

    # Bulk index documents.
    engine.bulk_add_documents(docs)

    # Perform a search across all clusters.
    query = "Python programming"
    results = engine.search(query)
    print("Search results for query:", query)
    for hit in results['hits']['hits']:
        source = hit['_source']
        print(f"Document: {source['doc_id']}, Score: {hit['_score']:.2f}, Text: {source['text']}")

    # Optionally, perform a search filtered by a specific cluster.
    cluster_filter = "programming"
    results_filtered = engine.search(query, cluster_filter=cluster_filter)
    print("\nFiltered search results for query:", query, "in cluster:", cluster_filter)
    for hit in results_filtered['hits']['hits']:
        source = hit['_source']
        print(f"Document: {source['doc_id']}, Score: {hit['_score']:.2f}, Text: {source['text']}")


if __name__ == "__main__":
    main()
