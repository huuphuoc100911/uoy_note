<?php

namespace App\Library;

use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientBuilder;
use Elastic\Elasticsearch\Exception\AuthenticationException;
use Elastic\Elasticsearch\Exception\ClientResponseException;
use Elastic\Elasticsearch\Exception\MissingParameterException;
use Elastic\Elasticsearch\Exception\ServerResponseException;
use Elastic\Elasticsearch\Helper\Iterators\SearchResponseIterator;
use Elastic\Elasticsearch\Response\Elasticsearch;
use Http\Promise\Promise;

class ElasticSearchApi
{
    protected Client $client;

    protected int $dateQuery;

    /**
     * @throws AuthenticationException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/connecting.html#auth-http
     */
    public function __construct()
    {
        $this->client = ClientBuilder::create()
            ->setHosts(['http://' . env('ELASTIC_NAME', 'etsy_elastic') . ':9200'])
            ->setBasicAuthentication('elastic', env('ELASTIC_PASS', 'etsy_elastic'))
            ->build();

        $this->dateQuery = intval(env('ELASTIC_DATE_QUERY', 60));
    }

    /**
     * @return Elasticsearch|Promise
     * @throws ClientResponseException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/connecting.html#_info_api
     */
    public function info()
    {
        return $this->client->info();
    }

    public function checkIndicesExist(string $index = '') {
        if ($index === '') return;

        return $this->client->indices()->exists(['index' => $index]);
    }

    /**
     * @param string $index
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws MissingParameterException
     * @throws ServerResponseException
     */
    public function createIndices(string $index = '')
    {
        if ($index === '') return;

        $params = [
            'index' => $index
        ];

        return $this->client->indices()->create($params);
    }

    /**
     * @param string $index
     * @param array $settings
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws ServerResponseException
     * $ref https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/index_management.html#_put_settings_api
     */
    public function putSettingsIndices(string $index = '', array $settings = [])
    {
        if ($index === '' || empty($settings)) return;

        $params = [
            'index' => $index,
            'body' => [
                'settings' => $settings
            ]
        ];

        return $this->client->indices()->putSettings($params);
    }

    /**
     * @param array $index
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/index_management.html#_get_settings_api
     */
    public function getSettingsIndices(array $index = [])
    {
        if (empty($index)) return;

        // Get settings for several indices
        $params = [
            'index' => $index
        ];

        return $this->client->indices()->getSettings($params);
    }

    /**
     * @param string $index
     * @param array $properties
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws MissingParameterException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/index_management.html#_put_mappings_api
     */
    public function putMappingIndices(string $index = '', array $properties = [])
    {
        if ($index === '' || empty($properties)) return;

        // Set the index and type
        $params = [
            'index' => $index,
            'body' => [
                '_source' => [
                    'enabled' => true
                ],
                'properties' => $properties
            ]
        ];

        // Update the index mapping
        return $this->client->indices()->putMapping($params);
    }

    /**
     * @param array $index [ 'my_index', 'my_index2' ]
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/index_management.html#_get_mappings_api
     */
    public function getMappingIndices(array $index = [])
    {
        if (empty($index)) return;

        // Get mappings for several indices
        $params = [
            'index' => $index
        ];

        return $this->client->indices()->getMapping($params);
    }

    /**
     * @param string $index
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws MissingParameterException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/index_management.html#_delete_an_index
     */
    public function deleteIndices(string $index = '')
    {
        if ($index === '') return;

        $params = [
            'index' => $index
        ];

        return $this->client->indices()->delete($params);
    }

    /**
     * @param string $index
     * @param array $body
     * @return int
     * @throws ClientResponseException
     * @throws ServerResponseException
     */
    public function countIndices(string $index = '', array $body = []): int
    {
        if (empty($index)) return 0;

        $results = $this->client->count([
            'index' => $index,
            'body'  => $body,
        ]);

        return (int) $results['count'] ?? 0;
    }

    /**
     * @param string $index
     * @return int
     * @throws ClientResponseException
     * @throws ServerResponseException
     */
    public function countIndicesAll(string $index = ''): int
    {
        if (empty($index)) return 0;

        $results = $this->client->count([
            'index' => $index,
        ]);

        return (int) $results['count'] ?? 0;
    }

    /**
     * @param string $index
     * @param string $id
     * @param array $body
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws MissingParameterException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/indexing_documents.html
     */
    public function indexing(string $index = '', string $id = '', array $body = [])
    {
        if ($index === '' || $id === '') return;

        $params = [
            'index' => $index,
            'id'    => $id,
            'body'  => $body
        ];

        // Document will be indexed to my_index/_doc/my_id
        return $this->client->index($params);
    }

    /**
     * @param array $params
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/indexing_documents.html#_bulk_indexing
     */
    public function bulkIndexing(array $params = [])
    {
        if (empty($params)) return;

        return $this->client->bulk($params);
    }

    /**
     * @param string $index
     * @param string $id
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws MissingParameterException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/getting_documents.html
     */
    public function getIndexId(string $index = '', string $id = '')
    {
        if ($index === '' || $id === '') return;

        $params = [
            'index' => $index,
            'id'    => $id
        ];

        // Get doc at /my_index/_doc/my_id
        return $this->client->get($params);
    }

    /**
     * @param string $index
     * @param string $id
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws MissingParameterException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/deleting_documents.html
     */
    public function deleteIndexId(string $index = '', string $id = '')
    {
        if ($index === '' || $id === '') return;

        $params = [
            'index' => $index,
            'id'    => $id
        ];

        // Delete doc at /my_index/_doc/my_id
        return $this->client->delete($params);
    }

    /**
     * @param string $index
     * @param int $from
     * @param int $size
     * @param array $body
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws ServerResponseException
     */
    public function searchIndices(string $index = '', int $from = 0, int $size = 100, array $body = []) {
        if ($index === '') return;

        $params = [
            'index' => $index,
            'from' => $from,
            'size' => $size,
            'body'  => $body,
        ];

        return $this->client->search($params);
    }

    /**
     * @param string $index
     * @param int $from
     * @param int $size
     * @param array $sort
     * @param array $match
     * [
     *   'testField' => 'abc'
     * ]
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/search_operations.html#_match_query
     */
    public function searchMatch(string $index = '', array $match = [], int $from = 0, int $size = 100, array $sort = [])
    {
        if ($index === '' || empty($match)) return;

        $params = [
            'index' => $index,
            'from' => $from,
            'size' => $size,
            'body'  => [
                'sort' => $sort,
                'query' => [
                    'match' => $match
                ]
            ]
        ];

        return $this->client->search($params);
    }

    /**
     * @param string $index
     * @param array $must [
     *   [ 'match' => [ 'testField' => 'abc' ] ],
     *   [ 'match' => [ 'testField2' => 'xyz' ] ],
     * ]
     * @param int $from
     * @param int $size
     * @param array $sort
     * @return Elasticsearch|Promise|void
     * @throws ClientResponseException
     * @throws ServerResponseException
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/search_operations.html#_bool_queries
     */
    public function searchBool(string $index = '', array $must = [], int $from = 0, int $size = 100, array $sort = [])
    {
        if ($index === '' || empty($must)) return;

        $params = [
            'index' => $index,
            'from' => $from,
            'size' => $size,
            'body'  => [
                'sort' => $sort,
                'query' => [
                    'bool' => [
                        'must' => $must,
                    ],
                ],
            ],
        ];

        return $this->client->search($params);
    }

    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/master/search-request-body.html#request-body-search-scroll
     *
     * @param array $params{
     *     scroll_id: string, //  The scroll ID
     *     scroll: time, // Specify how long a consistent view of the index should be maintained for scrolled search
     *     rest_total_hits_as_int: boolean, // Indicates whether hits.total should be rendered as an integer or an object in the rest search response
     *     pretty: boolean, // Pretty format the returned JSON response. (DEFAULT: false)
     *     human: boolean, // Return human readable values for statistics. (DEFAULT: true)
     *     error_trace: boolean, // Include the stack trace of returned errors. (DEFAULT: false)
     *     source: string, // The URL-encoded request definition. Useful for libraries that do not accept a request body for non-POST requests.
     *     filter_path: list, // A comma-separated list of filters used to reduce the response.
     *     body: array, //  The scroll ID if not passed by URL or query parameter.
     * }
     * @return Elasticsearch|Promise
     * @throws ClientResponseException
     * @throws ServerResponseException
     */
    public function scroll(array $params = [])
    {
        return $this->client->scroll($params);
    }

    /**
     * @return Elasticsearch|Promise
     * @throws ClientResponseException
     * @throws ServerResponseException
     */
    public function allocation()
    {
        return $this->client->cat()->allocation();
    }

    public function createDocument(string $index = '', string $id = '', array $body = [], $refresh = false)
    {
        if ($index === '') return;

        $params = [
            'index' => $index,
            'id' => $id,
            'body' => $body,
            'refresh' => $refresh
        ];

        return $this->client->create($params);
    }

    public function checkDocumentExist(string $index = '', string $id = '')
    {
        if ($index === '') return;

        $params = [
            'index' => $index,
            'id' => $id
        ];

        return $this->client->exists($params);
    }
}
