package cn.csdn.es_api.utils;

import cn.csdn.es_api.entity.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Component
public class HtmlParseUtil {

    public List<Content> parseJD(String keyword) throws Exception {
        // 获取请求 https://search.jd.com/Search?keyword=java;
        String url = "https://search.jd.com/Search?keyword=" + keyword;
        //解析网页
        Document document = Jsoup.parse(new URL(url), 30000);
        Element element = document.getElementById("J_goodsList");
        Elements elements = element.getElementsByTag("li");
        List<Content> list = new ArrayList<>();
        for (Element liElement : elements) {
            String img = liElement.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = liElement.getElementsByClass("p-price").eq(0).text();
            String title = liElement.getElementsByClass("p-name").eq(0).text();
            list.add(new Content(img,price,title));
        }
        return list;
    }
}
