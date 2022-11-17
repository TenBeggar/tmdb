package com.example.tmdb;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@SpringBootTest
class TmdbApplicationTests {

    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Test
    void importData() {
        ClassPathResource classPathResource = new ClassPathResource("tmdb_5000_movies.csv");
        try (InputStream inputStream = classPathResource.getInputStream();
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             CSVReader csvReader = new CSVReader(inputStreamReader)) {
            Iterator<String[]> iterator = csvReader.iterator();
            int id = 0;
            List<IndexQuery> queries = new ArrayList<>();
            IndexQueryBuilder indexQueryBuilder = new IndexQueryBuilder();
            while (iterator.hasNext()) {
                id++;
                if (id == 1) {
                    continue;
                }
                String[] next = iterator.next();
                JSONArray array = JSONArray.parseArray(next[20]);
                JSONObject cast = new JSONObject();
                cast.put("character", array.getJSONObject(0).getString("character"));
                cast.put("name", array.getJSONObject(0).getString("name"));
                JSONObject source = new JSONObject();
                source.put("title", next[17]);
                source.put("tagline", next[16]);
                source.put("release_date", StringUtils.isBlank(next[11]) ? "1970/01/01" : next[11]);
                source.put("popularity", next[8]);
                source.put("cast", cast);
                source.put("overview", next[7]);
                queries.add(indexQueryBuilder.withId(String.valueOf(id - 1)).withSource(source.toJSONString()).build());
            }
            elasticsearchRestTemplate.bulkIndex(queries, IndexCoordinates.of("movie"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
