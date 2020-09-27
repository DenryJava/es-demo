package cn.csdn.es_api.service.impl;

import cn.csdn.es_api.entity.Content;
import cn.csdn.es_api.service.ContentService;
import cn.csdn.es_api.utils.HtmlParseUtil;
import cn.hutool.json.JSONUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ContentServiceImpl implements ContentService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;
    @Autowired
    private HtmlParseUtil htmlParseUtil;

    // 解析数据放入es索引中
    @Override
    public Boolean parseContent(String keyword) throws Exception {
        List<Content> list = htmlParseUtil.parseJD(keyword);
        // 查询数据放入es中
        //保证已创建了索引库
        //批量插入数据
        BulkRequest request = new BulkRequest();
        request.timeout("2m");
        list.forEach(content -> {
            request.add(new IndexRequest("jd_goods")
                    .source(JSONUtil.toJsonStr(content), XContentType.JSON));
        });
        BulkResponse responses = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        return !responses.hasFailures();
    }

    // 根据名字获取es中的数据 分页
    @Override
    public List<Map<String, Object>> searchPage(String keyword, int pageNo, int pageSize) throws IOException {
        if (pageNo < 1) {
            pageNo = 1;
        }
        //条件搜索
        SearchRequest request = new SearchRequest("jd_goods");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 分页
        sourceBuilder.from(pageNo);
        sourceBuilder.size(pageSize);
        // 中文分词模糊匹配关键字
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", keyword);
        if(!"all".equals(keyword)){
            sourceBuilder.query(matchQueryBuilder);
        }
        sourceBuilder.timeout(TimeValue.timeValueSeconds(60));
        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(true);//多个高亮显示
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color: red'>");
        highlightBuilder.postTags("</span>");
        sourceBuilder.highlighter(highlightBuilder);
        //执行搜索
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 解析结果
        List<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit documentFields : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            // 解析高亮的字段
            Map<String, HighlightField> fieldMap = documentFields.getHighlightFields();
            HighlightField title = fieldMap.get("title");
            if(title!=null){
                Text[] fragments = title.getFragments();
                String name = "";
                for (Text fragment : fragments) {
                    name += fragment;
                }
                sourceAsMap.put("title",name); // 替换高亮字段
            }
            list.add(sourceAsMap);
        }
        return list;
    }
}
