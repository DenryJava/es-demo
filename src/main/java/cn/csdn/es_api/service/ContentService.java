package cn.csdn.es_api.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ContentService {

    Boolean parseContent(String keyword) throws Exception;

    List<Map<String,Object>> searchPage(String keyword,int pageNo,int pageSize) throws IOException;
}
