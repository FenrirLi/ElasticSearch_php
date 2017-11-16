<?php

namespace App\Utils\Elastic;

use App\Exceptions\SystemErrorException;
use Elasticsearch\ClientBuilder;
use Illuminate\Support\Facades\Config;

class ElasticSearch {

    //elastic连接
    public static $elastic_client = false;

    public static function client(){
        return new self();
    }

    public function __construct() {
        if( self::$elastic_client === false ) {
            $elastic_config = Config::get('elastic');

            $host = [
                'scheme' => $elastic_config['scheme'],
                'host' => $elastic_config['host'],
                'port' => $elastic_config['port'],
            ];
            if( $elastic_config['auth'] ) {
                $host['user'] = $elastic_config['user'];
                $host['pass'] = $elastic_config['pass'];
            }
            self::$elastic_client = ClientBuilder::create()->setHosts( [
                $host
            ] )->build();
        }
    }
    private $page = 1;

    private $page_size = 16;

    private $elastic_params = [];

    /**
     * set index
     * @param $index
     * @return $this
     */
    public function index( $index ) {
        if( !empty( $index ) ) {
            $this->elastic_params['index'] = $index;
        }
        return $this;
    }

    /**
     * set type
     * @param $type
     * @return $this
     */
    public function type( $type ) {
        if( !empty( $type ) ) {
            $this->elastic_params['type'] = $type;
        }
        return $this;
    }

    /**
     * set page info
     * @param int $page
     * @param int $page_size
     * @return $this
     */
    public function paginate( $page = 1, $page_size = 16 ) {
        if( !empty( $page ) && !empty( $page_size ) ) {
            $this->page = $page;
            $this->page_size = $page_size;
        }
        return $this;
    }

    /**
     * add sort
     * @param $field
     * @param $order
     * @return $this
     */
    public function sort( $field, $order ) {
        $this->elastic_params['body']['sort'][ $field ] = $order ;
        return $this;
    }

    /**
     * search fields
     * @param array $fields
     * @return $this
     */
    public function fields( array $fields ) {
        if( !empty( $fields ) ) {
            $this->elastic_params['body']['_source'] = $fields;
        }
        return $this;
    }

    /**
     * search condition
     * @param $field
     * @param $operator
     * @param $value
     * @return $this
     */
    public function where( $field, $operator, $value = null ) {
        if( $value === null ) {
            $this->elastic_params['body']['query']['bool']['must'][] = [ "match_phrase" => [ $field => $operator ] ];
        }
        else{
            switch ( $operator ) {
                case '=' :
                    $this->elastic_params['body']['query']['bool']['must'][] = [ "match_phrase" => [ $field => $value ] ];
                    break;
                case '>' :
                    $this->elastic_params['body']['query']['bool']['must'][] = [ "range" => [ $field => [ 'gt' => $value ] ] ];
                    break;
                case '>=' :
                    $this->elastic_params['body']['query']['bool']['must'][] = [ "range" => [ $field => [ 'gte' => $value ] ] ];
                    break;
                case '<' :
                    $this->elastic_params['body']['query']['bool']['must'][] = [ "range" => [ $field => [ 'lt' => $value ] ] ];
                    break;
                case '<=' :
                    $this->elastic_params['body']['query']['bool']['must'][] = [ "range" => [ $field => [ 'lte' => $value ] ] ];
                    break;
                case '!=' :
                    $this->elastic_params['body']['query']['bool']['must_not'][] = [ "match_phrase" => [ $field => $value ] ];
                    break;
            }
        }
        return $this;
    }

    /**
     * search
     * @return array
     * @throws \Exception
     */
    public function search() {
        //paginate
        $this->elastic_params['body']['from'] = ( $this->page - 1 ) * $this->page_size;
        $this->elastic_params['body']['size'] = $this->page_size;

        //删除字段不可有值
        $this->elastic_params['body']['query']['bool']['must_not'][] = [ "exists" => [ 'field' => 'deleted_at' ] ];

        try {
            $response = self::$elastic_client->search( $this->elastic_params );
        } catch ( \Exception $e ) {
            if( env('APP_DEBUG') ) {
                throw new SystemErrorException('搜索失败');
            }
            else{
                $response = [];
            }
        }

        return $this->getSearchData( $response );
    }

    public function getSearchData( $response ) {
        //parse response
        $data_list = [];
        $total = 0;
        if( !empty( $response['hits']['hits'] ) && !empty( $response['hits']['total'] ) ) {
            $data_list = $response['hits']['hits'];

            //page info
            $total = $response['hits']['total'];
        }
        //all data
        $response_data = [];
        foreach ( $data_list as $data ) {
            $response_data[] = $data['_source'];
        }

        //parse page info
        $page_info = [
            'total' => $total,
            'current_page' => $this->page,
            'final_page' => ceil( $total / $this->page_size ),
            'page_size' => $this->page_size,
        ];

        return [
            'page' => $page_info,
            'data' => $response_data
        ];
    }

    /**
     * get a document
     * @param $document_id
     * @return array
     * @throws \Exception
     */
    public function get( $document_id ) {
        $filter['id'] = $document_id;
        if( isset( $this->elastic_params['index'] ) ) $filter['index'] = $this->elastic_params['index'];
        if( isset( $this->elastic_params['type'] ) ) $filter['type'] = $this->elastic_params['type'];
        try {
            $response = self::$elastic_client->get( $filter );
        } catch ( \Exception $e ) {
            if( env('APP_DEBUG') ) {
                throw new SystemErrorException('信息获取失败');
            }
            else{
                $response = [];
            }
        }

        return $this->getDocumentData( $response );
    }

    /**
     * get document data
     * @param $response
     * @return array
     */
    public function getDocumentData( $response ) {
        //parse response
        $data = [];
        if( !empty( $response['_source'] ) ) {
            $data = $response['_source'];
        }

        //filter fields
        if( !empty( $this->elastic_params['body']['_source'] ) ) {
            $fields = array_flip( $this->elastic_params['body']['_source'] );
            foreach ( $data as $k => $v ) {
                if( !isset( $fields[ $k ] ) ) unset( $data[$k] );
            }
        }

        return $data;
    }

}