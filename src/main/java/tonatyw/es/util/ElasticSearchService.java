package tonatyw.es.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import tonatyw.es.common.Constants;

public class ElasticSearchService {
	private Settings settings;
	private static Client client;
    private static BulkProcessor bulkProcessor;
    private static int MAX = 100;
    private static String PRE_TAGS = "<span color='red'>";
    private static String POST_TAGS = "</span>";

    private static Map<String, String[]> indexsMap = new HashMap<String, String[]>();

    public static BulkProcessor getBulkProcessor() {
        if (bulkProcessor == null) {
            bulkProcessor = BulkProcessor.builder(client, new Listener() {

                @Override
                public void beforeBulk(long executionId, BulkRequest request) {

                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                }
            }).setBulkActions(5000).setFlushInterval(TimeValue.timeValueSeconds(5)).build();
        }
        return bulkProcessor;
    }

    /***
     * 初始化elasticsearch集群业务对象
     */
    public ElasticSearchService() {
        try {
            if (client == null) {
                settings = Settings.builder()
                    .put("cluster.name", Constants.CLUSTER_NAME).build();
                client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress
                        .getByName(Constants.IP), Constants.PORT));
            }
        } catch (UnknownHostException e) {
        }
    }

    /***
     * 创建索引
     * 
     * @param indexName
     */
    public static void createIndex(String indexName) {
        client.admin().indices().create(new CreateIndexRequest(indexName))
            .actionGet();
    }

    /***
     * 
     *  创建索引
     * @param index
     * @param type
     */
    public static void createIndex(String index, String type) {
        client.prepareIndex(index, type).setSource().get();
        try {
            String s = getEsFormatMappingProperties().getProperty(index);
            Map<String, Object> map = JSON.parseObject(s, HashMap.class);
            client.admin().indices().preparePutMapping(index).setType(type)
                .setSource(map).execute().actionGet();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /****
     * 获取创建索引配置信息
     * @author liaolian.ll
     * 
     * @return
     * @throws IOException
     */
    public static Properties getEsFormatMappingProperties() throws IOException {
        Properties p = new Properties();
        p.load(ElasticSearchService.class.getClassLoader().getResourceAsStream(
            "esFormatMapping.properties"));
        return p;
    }

    /**
     * 删除索引
     * 
     * @param index
     */
    public static void deleteIndex(String index) {
        if (indexExist(index)) {
            DeleteIndexResponse dResponse = client.admin().indices()
                .prepareDelete(index).execute().actionGet();
            if (!dResponse.isAcknowledged()) {
                //                logger.error("failed to delete index.");  
            }
        } else {
            //            logger.error("index name not exists");  
        }
    }

    /***
     * 判断索引是否存在
     * 
     * @param index
     * @return
     */
    public static boolean indexExist(String index) {
        IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(index);
        IndicesExistsResponse inExistsResponse = client.admin().indices()
            .exists(inExistsRequest).actionGet();
        return inExistsResponse.isExists();
    }

    /***
     * 
     * 插入数据
     * @param index
     * @param type
     * @param json
     */
    public static void insertData(String index, String type, Map<String, Object> json) {
        IndexRequestBuilder builder = client.prepareIndex(index, type);
        IndexResponse response = builder.setSource(json).get();
    }

    /***
     * 
     * 插入数据
     * @param index
     * @param type
     * @param _id
     * @param json
     */
    public static void insertData(String index, String type, String _id, Map<String, Object> json) {
        IndexResponse response = client.prepareIndex(index, type)
            .setId(_id).setSource(json).get();
    }

    /***
     * 更新数据
     * 
     * @param index
     * @param type
     * @param _id
     * @param json
     */
    public static void updateData(String index, String type, String _id, Map<String, Object> json) {
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, type, _id).doc(json);
            client.update(updateRequest).get();
        } catch (Exception e) {
            //            logger.error("update data failed.", e);  
        }
    }

    /**
     * 删除数据
     * 
     * @param index
     * @param type
     * @param _id
     */
    public static void deleteData(String index, String type, String _id) {
        DeleteResponse response = client.prepareDelete(index, type, _id)
            .get();
    }

    /***
     * 
     * 
     * @param index
     * @param type
     * @param data
     */
    public static void bulkInsertData(String index, String type, Map<String, Object> data) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        data.forEach((param1, param2) -> {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            bulkRequest.add(client.prepareIndex(index, type, param1)
                .setSource((Map<String, Object>) param2));
        });
        BulkResponse bulkResponse = bulkRequest.get();
    }

    /***
     * 
     * 
     * @param index
     * @param type
     * @param data
     */
    public static void bulkInsertDataProcesser(String index, String type, Map<String, Object> data) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        data.forEach((param1, param2) -> {
            getBulkProcessor().add(
                new IndexRequest(index, type, param1).source((Map<String, Object>) param2));
        });
        getBulkProcessor().flush();
        client.admin().indices().prepareRefresh().get();
    }

    /***
     * 条件查询
     * 
     * @param index
     * @param type
     * @param constructor
     * @param fields
     * @return
     */
    public static Map<String, Object> search(String index, String type,
                                             ESQueryBuilderConstructor constructor, String[] fields) {
        Map<String, Object> resultMap = new HashMap<String, Object>();
        List<Map<String, Object>> result = new ArrayList<>();
        SearchRequestBuilder searchRequestBuilder;
        if (StringUtils.isEmpty(index)) {
            searchRequestBuilder = client.prepareSearch().setTypes(type);
        } else if (StringUtils.isEmpty(type)) {
            searchRequestBuilder = client.prepareSearch(index).setTypes();
        } else {
            searchRequestBuilder = client.prepareSearch(index)
                .setTypes(type);
        }
        //排序查询
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        //添加查询
        searchRequestBuilder.setQuery(constructor.listBuilders());

        //最小值匹配
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        //最大值匹配
        if (size > MAX) {
            size = MAX;
        }
        //设置最大数量
        searchRequestBuilder.setSize(size);
        searchRequestBuilder.setFrom(constructor.getFrom() <= 0 ? 0 : (constructor.getFrom() - 1)
                                                                      * size);

        //分组
        if (constructor.getEsGroupby() != null) {
            Map<Object, Object> groupByMap = new HashMap<Object, Object>();
            searchRequestBuilder.addAggregation(constructor.getEsGroupby().aggregationBuilder());
        }
        //高亮
        if (fields != null && fields.length > 0) {
            HighlightBuilder hiBuilder = new HighlightBuilder();
            //高亮前缀
            hiBuilder.preTags(PRE_TAGS);
            //高亮后缀
            hiBuilder.postTags(POST_TAGS);
            for (String field : fields) {
                hiBuilder.field(field);
            }
            searchRequestBuilder = searchRequestBuilder.highlighter(hiBuilder);
        }
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        if (searchResponse.getAggregations() != null) {
            Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
            resultMap.put("aggregation", aggMap);
        }

        SearchHits hits = searchResponse.getHits();

        SearchHit[] searchHists = hits.getHits();
        resultMap.put("total", hits.getTotalHits());
        for (SearchHit sh : searchHists) {
            Map<String, Object> dataMap = sh.getSourceAsMap();
            dataMap.put("rowKey", sh.getId());
            dataMap.put("tableName", sh.getIndex());
            dataMap.put("type", sh.getType());
            dataMap.put("version", sh.getVersion());
            //        	dataMap.put("highLignt", sh.getHighlightFields().get(arg0));
            Map<String, List<String>> highMap = new HashMap<String, List<String>>();
            if (fields != null) {
                for (String field : fields) {
                    if (sh.getHighlightFields().containsKey(field)) {
                        //	            	System.out.println("�����������ϣ���ӡ����Ƭ��:");
                        Text[] text = sh.getHighlightFields().get(field).getFragments();
                        List<String> highLight = new LinkedList<String>();
                        for (Text str : text) {
                            highLight.add(str.toString());
                        }
                        highMap.put(field, highLight);
                    }
                }
            }
            dataMap.put("highLight", highMap);
            result.add(dataMap);
        }
        resultMap.put("dataList", result);

        if (constructor.getEsGroupby() != null) {
            MultiBucketsAggregation stateAgg = searchResponse.getAggregations().get(
                constructor.getEsGroupby().getKeyName());
            Iterator<Terms.Bucket> iter = (Iterator<Bucket>) stateAgg.getBuckets().iterator();
            JSONArray array = new JSONArray();
            while (iter.hasNext()) {
                JSONObject json = new JSONObject();
                MultiBucketsAggregation.Bucket gradeBucket = iter.next();
                json.put(gradeBucket.getKeyAsString(), gradeBucket.getDocCount());
                array.add(json);
            }
            Map<String, Object> groupByTotalMap = new HashMap<String, Object>();
            groupByTotalMap.put("total", array.size());
            groupByTotalMap.put("data", array.subList((constructor.getEsGroupby().getFrom() - 1)
                                                      * constructor.getEsGroupby().getSize(),
                (array.size() < constructor.getEsGroupby().getSize()
                                * constructor.getEsGroupby().getFrom()) ? array.size()
                    : constructor.getEsGroupby().getSize() * constructor.getEsGroupby().getFrom()));
            resultMap.put("groupByData", groupByTotalMap);
        }
        return resultMap;
    }

    public static Map<String, Object> searchByType(String index, String type,
                                                   ESQueryBuilderConstructor constructor,
                                                   String[] fields) {
        Map<String, Object> resultMap = new HashMap<String, Object>();
        List<Map<String, Object>> result = new ArrayList<>();
        SearchRequestBuilder searchRequestBuilder;
        if (StringUtils.isEmpty(index)) {
            searchRequestBuilder = client.prepareSearch().setTypes(type);
        } else if (StringUtils.isEmpty(type)) {
            searchRequestBuilder = client.prepareSearch(index).setTypes();
        } else {
            searchRequestBuilder = client.prepareSearch(index)
                .setTypes(type);
        }
        // 判断排序
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        //        searchRequestBuilder.setQuery(constructor.listBuilders());
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        searchRequestBuilder.setSize(size);
        searchRequestBuilder.setFrom(constructor.getFrom() <= 0 ? 0 : (constructor.getFrom() - 1)
                                                                      * size);

        if (constructor.getEsGroupby() != null) {
            Map<Object, Object> groupByMap = new HashMap<Object, Object>();
            searchRequestBuilder.addAggregation(constructor.getEsGroupby().aggregationBuilder());
        }
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        if (searchResponse.getAggregations() != null) {
            Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
            resultMap.put("aggregation", aggMap);
        }
        if (constructor.getEsGroupby() != null) {
            Terms stateAgg = searchResponse.getAggregations().get(
                constructor.getEsGroupby().getKeyName());
            Iterator<Terms.Bucket> iter = (Iterator<Bucket>) stateAgg.getBuckets().iterator();
            JSONArray array = new JSONArray();
            while (iter.hasNext()) {
                JSONObject json = new JSONObject();
                Terms.Bucket gradeBucket = iter.next();
                json.put(gradeBucket.getKeyAsString(), gradeBucket.getDocCount());
                array.add(json);
            }
            Map<String, Object> groupByTotalMap = new HashMap<String, Object>();
            groupByTotalMap.put("total", array.size());
            groupByTotalMap.put("data", array.subList((constructor.getEsGroupby().getFrom() - 1)
                                                      * constructor.getEsGroupby().getSize(),
                (array.size() < constructor.getEsGroupby().getSize()
                                * constructor.getEsGroupby().getFrom()) ? array.size()
                    : constructor.getEsGroupby().getSize() * constructor.getEsGroupby().getFrom()));
            resultMap.put("groupByData", groupByTotalMap);
        }
        return resultMap;
    }

    /***
     * 查询单条数据
     * 
     * @param index
     * @param type
     * @param id
     * @return
     */
    public static Map<String, Object> search(String index, String type, String id) {
        GetResponse getResponse = client.prepareGet().setIndex(index)
            .setType(type).setId(id).get();
        return getResponse.getSourceAsMap();
    }

    
    public static Map<Object, Object> statSearch(String index, String type,
                                                 ESQueryBuilderConstructor constructor,
                                                 String groupBy) {
        Map<Object, Object> map = new HashMap();
        SearchRequestBuilder searchRequestBuilder = client
            .prepareSearch(index).setTypes(type);
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        if (null != constructor) {
            searchRequestBuilder.setQuery(constructor.listBuilders());
        } else {
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        searchRequestBuilder.setSize(size);

        searchRequestBuilder.setFrom(constructor.getFrom() < 0 ? 0 : constructor.getFrom());
        SearchResponse sr = searchRequestBuilder.addAggregation(
            AggregationBuilders.terms("agg").field(groupBy)).get();

        Terms stateAgg = sr.getAggregations().get("agg");
        Iterator<Terms.Bucket> iter = (Iterator<Bucket>) stateAgg.getBuckets().iterator();

        while (iter.hasNext()) {
            Terms.Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey(), gradeBucket.getDocCount());
        }

        return map;
    }

    public static Map<Object, Object> statSearch(String index, String type,
                                                 ESQueryBuilderConstructor constructor,
                                                 AggregationBuilder agg) {
        if (agg == null) {
            return null;
        }
        Map<Object, Object> map = new HashMap();
        SearchRequestBuilder searchRequestBuilder = client
            .prepareSearch(index).setTypes(type);
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        if (null != constructor) {
            searchRequestBuilder.setQuery(constructor.listBuilders());
        } else {
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        searchRequestBuilder.setSize(size);

        searchRequestBuilder.setFrom(constructor.getFrom() < 0 ? 0 : constructor.getFrom());
        SearchResponse sr = searchRequestBuilder.addAggregation(agg).get();

        Terms stateAgg = sr.getAggregations().get("agg");
        Iterator<Terms.Bucket> iter = (Iterator<Bucket>) stateAgg.getBuckets().iterator();

        while (iter.hasNext()) {
            Terms.Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey(), gradeBucket.getDocCount());
        }

        return map;
    }

    /**
     * 索引数据迁移
     * @param indexFrom 源索引
     * @param indexTo 目标索引
     * @param type 源和目标的共同type名
     */
    private void getSearchDataByScrolls(String indexFrom, String indexTo, String type) {

        int timeMillis = 60000;

        SearchResponse scrollResp = client.prepareSearch(indexFrom)
            .setTypes(type)

            .setScroll(new TimeValue(timeMillis)).setSize(10).execute().actionGet();

        while (true) {

            //    	        BulkRequestBuilder bulkRequest = client.prepareBulk();

            SearchHit[] hits = scrollResp.getHits().getHits();

            System.out.println(hits.length);

            if (hits.length > 0) {

                for (SearchHit searchHit : hits) {
                    Map<String, Object> map = searchHit.getSourceAsMap();
                    getBulkProcessor().add(
                        new IndexRequest(indexTo, type, searchHit.getId()).source(map));
                }

                getBulkProcessor().flush();
                client.admin().indices().prepareRefresh().get();

            }

            scrollResp = client
                .prepareSearchScroll(scrollResp.getScrollId())

                .setScroll(new TimeValue(timeMillis)).execute().actionGet();

            if (scrollResp.getHits().getHits().length == 0) {

                break;

            }

        }

    }
    
    /**
     * 查询删除
     * @param index 索引
     * @param map 条件符合集
     * @param notMap 条件过滤集
     */
    public void delByQuery(String index, Map<String, Object> map, Map<String, Object> notMap) {

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if (map != null) {
            for (String key : map.keySet()) {
                queryBuilder.must(QueryBuilders.termQuery(key, map.get(key)));
            }
        }

        if (notMap != null) {
            for (String key : notMap.keySet()) {
                queryBuilder.mustNot(QueryBuilders.termQuery(key, notMap.get(key)));
            }
        }

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
            .newRequestBuilder(client).filter(queryBuilder).source(index)
            .get();

        long deleted = response.getDeleted();
        System.out.println(deleted);
    }
    
    public static void close() {
        client.close();
    }

    public static void setBulkProcessor(BulkProcessor bulkProcessor) {
        ElasticSearchService.bulkProcessor = bulkProcessor;
    }

    public static void main(String[] args) throws ParseException, IOException {
    	// 添加es
    	String index = "test";
    	String type = "testtype";
        ElasticSearchService ess = new ElasticSearchService();
//        Map<String,Object> map = new HashMap<String,Object>();
//        ess.createIndex(inedx, type);
        
        // 加入数据
//        Map<String,Object> mainMap = new HashMap<String,Object>();
//        Map<String,Object> param = new HashMap<String,Object>();
//        param.put("name", "张三");
//        param.put("num", 1);
//        param.put("begin_date", "2019-02-11 10:10:10");
        
//        Map<String,Object> param1 = new HashMap<String,Object>();
//        param1.put("name", "张三四");
//        param1.put("num", 2);
//        param1.put("begin_date", "2019-02-10 06:06:06");
//        mainMap.put(DigestUtils.md5Hex(JSON.toJSONString(param)), param);
//        mainMap.put(DigestUtils.md5Hex(JSON.toJSONString(param1)), param1);
//        ess.bulkInsertData(index, type, mainMap);
        
        
        // 查询
        ESQueryBuilderConstructor esbc = new ESQueryBuilderConstructor();
        ESQueryBuilders esb = new ESQueryBuilders();
        
     // 分组
//        ESGroupby esgb = new ESGroupby("num");
//        esgb.desc(2);
//        esgb.aggregationBuilder();
//        esbc.setEsGroupby(esgb);
//        esb.term("name", "张三");
//        esbc.must(esb);
//        Map<String,Object> data = ess.search(index, type, esbc, new String[]{"name"});
//        data.forEach((key,value)->{
//        	System.out.println(key);
//        	System.out.println(value);
//        });
    }
}