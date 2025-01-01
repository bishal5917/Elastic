package com.bsal.utils;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ESUtil {

    // connecting to Elastic DB
    private RestHighLevelClient getConnClient(){
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "0DxIRGQZGBhV6qYx-x4L"));
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    // searching a index(database) from Elastic DB
    public void searchElasticDb() {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("my-index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;

        try{
            searchResponse = getConnClient().search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value > 0){
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                for (SearchHit searchHit : searchHits){
                    System.out.println("SEARCH-HIT:" + searchHit);
                }
            }
        } catch (Exception ex){
            System.out.println(ex.getMessage());
        }
    }
}
