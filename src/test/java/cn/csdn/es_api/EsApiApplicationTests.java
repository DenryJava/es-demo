package cn.csdn.es_api;

import cn.csdn.es_api.entity.User;
import cn.csdn.es_api.utils.ESUtils;
import cn.hutool.json.JSONUtil;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class EsApiApplicationTests {
    @Autowired
    private RestHighLevelClient restHighLevelClient;
    //索引创建
    @Test
    void testCreateIndex() throws IOException {
        //创建索引请求
        CreateIndexRequest request = new CreateIndexRequest(ESUtils.ES_INDEX);
        //执行创建请求 indicesClient
        CreateIndexResponse createIndexResponse =
                restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }
    //获取索引
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest(ESUtils.ES_INDEX);
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    //删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(ESUtils.ES_INDEX);
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
    //添加文档
    @Test
    void testAddDocument() throws IOException {
        User user = new User("sui", 18);
        //创建请求
        IndexRequest request = new IndexRequest(ESUtils.ES_INDEX);
        //规则 PUT /deng_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //数据放入请求
        request.source(JSONUtil.toJsonStr(user), XContentType.JSON);
        //客户端发送请求,获取响应结果
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());
    }
    //获取文档是否存在
    @Test
    void testIsExistDocument() throws IOException {
        GetRequest request = new GetRequest(ESUtils.ES_INDEX,"1");
        // 不获取返回的 _source 上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    //获取文档信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest(ESUtils.ES_INDEX,"1");
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
        System.out.println(response);
    }
    //更新文档信息
    @Test
    void testUplDocument() throws IOException {
        UpdateRequest request = new UpdateRequest(ESUtils.ES_INDEX, "1");
        request.timeout("1s");
        User user = new User("dengliangcai",99);
        request.doc(JSONUtil.toJsonStr(user),XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }
    //删除文档信息
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest(ESUtils.ES_INDEX, "1");
        request.timeout("1s");
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }
    //批量添加文档信息
    @Test
    void testBulkDocument() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("10s");
        List<User> list = new ArrayList<>();
        list.add(new User("deng",11));
        list.add(new User("sui",22));
        list.add(new User("zhouhao",55));
        for (int i = 0; i < list.size(); i++) {
            // 批量更新，删除修改add中不同request请求即可
            request.add(new IndexRequest(ESUtils.ES_INDEX)
                    .id(""+(i+1))
                    .source(JSONUtil.toJsonStr(list.get(i)),XContentType.JSON));
        }
        BulkResponse responses = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(responses.hasFailures());
    }
    //查询
    @Test
    void testSearch() throws IOException {
        SearchRequest request = new SearchRequest(ESUtils.ES_INDEX);
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //构建查询构造器
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "sui");
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(TimeValue.timeValueSeconds(60));
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        System.out.println(JSONUtil.toJsonStr(response.getHits()));
        System.out.println("=======================");
        for (SearchHit documentFields : response.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }

    }
}
