import unittest
from unittest.mock import MagicMock, patch
from src.elastic_engine import FractalElasticsearchEngine

class TestFractalElasticsearchEngine(unittest.TestCase):
    @patch('src.elastic_engine.Elasticsearch')
    def test_create_index(self, mock_elasticsearch):
        """
        Test that the index is created if it does not exist.
        """
        # Setup the mock for indices.exists to return False
        es_instance = MagicMock()
        es_instance.indices.exists.return_value = False
        mock_elasticsearch.return_value = es_instance

        # Initialize engine which should trigger index creation.
        engine = FractalElasticsearchEngine(index_name='test_index')
        es_instance.indices.create.assert_called_once()

    @patch('src.elastic_engine.Elasticsearch')
    def test_add_document(self, mock_elasticsearch):
        """
        Test adding a single document.
        """
        es_instance = MagicMock()
        mock_elasticsearch.return_value = es_instance

        engine = FractalElasticsearchEngine(index_name='test_index')
        engine.add_document('doc1', 'Test document', cluster='test_cluster')

        expected_doc = {
            'doc_id': 'doc1',
            'text': 'Test document',
            'cluster': 'test_cluster'
        }
        es_instance.index.assert_called_with(
            index='test_index', id='doc1', body=expected_doc
        )

    @patch('src.elastic_engine.helpers.bulk')
    @patch('src.elastic_engine.Elasticsearch')
    def test_bulk_add_documents(self, mock_elasticsearch, mock_bulk):
        """
        Test bulk adding documents.
        """
        es_instance = MagicMock()
        mock_elasticsearch.return_value = es_instance

        engine = FractalElasticsearchEngine(index_name='test_index')
        docs = [
            {'doc_id': 'doc1', 'text': 'Document 1', 'cluster': 'cluster1'},
            {'doc_id': 'doc2', 'text': 'Document 2', 'cluster': 'cluster2'},
        ]
        engine.bulk_add_documents(docs)

        # Verify that the helpers.bulk method was called once.
        mock_bulk.assert_called_once()

    @patch('src.elastic_engine.Elasticsearch')
    def test_search_without_cluster_filter(self, mock_elasticsearch):
        """
        Test the search function without a cluster filter.
        """
        es_instance = MagicMock()
        mock_elasticsearch.return_value = es_instance

        # Simulate a fake search response from Elasticsearch.
        fake_response = {
            'hits': {
                'hits': [
                    {
                        '_source': {
                            'doc_id': 'doc1',
                            'text': 'Test document',
                            'cluster': 'test_cluster'
                        },
                        '_score': 1.0
                    }
                ]
            }
        }
        es_instance.search.return_value = fake_response

        engine = FractalElasticsearchEngine(index_name='test_index')
        results = engine.search('Test')

        self.assertIn('hits', results)
        self.assertEqual(len(results['hits']['hits']), 1)
        self.assertEqual(
            results['hits']['hits'][0]['_source']['doc_id'], 'doc1'
        )

    @patch('src.elastic_engine.Elasticsearch')
    def test_search_with_cluster_filter(self, mock_elasticsearch):
        """
        Test the search function when filtering by a specific cluster.
        """
        es_instance = MagicMock()
        mock_elasticsearch.return_value = es_instance

        fake_response = {
            'hits': {
                'hits': [
                    {
                        '_source': {
                            'doc_id': 'doc2',
                            'text': 'Test document with filter',
                            'cluster': 'filtered_cluster'
                        },
                        '_score': 1.2
                    }
                ]
            }
        }
        es_instance.search.return_value = fake_response

        engine = FractalElasticsearchEngine(index_name='test_index')
        results = engine.search('Test', cluster_filter='filtered_cluster')

        self.assertEqual(len(results['hits']['hits']), 1)
        self.assertEqual(
            results['hits']['hits'][0]['_source']['cluster'],
            'filtered_cluster'
        )


if __name__ == '__main__':
    unittest.main()
