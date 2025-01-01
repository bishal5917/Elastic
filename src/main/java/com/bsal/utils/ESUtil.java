package com.bsal.utils;

import com.bsal.pojos.Product;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ESUtil {

    // connecting to Elastic DB
    private RestHighLevelClient getConnClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "0DxIRGQZGBhV6qYx-x4L"));
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    // creating a new index into the elastic search db
    public void createIndex(String indexName) throws IOException {
        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 2));
            CreateIndexResponse createIndexResponse = getConnClient().indices().create(request, RequestOptions.DEFAULT);
            System.out.println("Response id: " + createIndexResponse.index());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    // searching a index(database) from Elastic DB
    public void searchElasticDb() {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("products");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;

        try {
            searchResponse = getConnClient().search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value > 0) {
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                for (SearchHit searchHit : searchHits) {
                    System.out.println("SEARCH-HIT:" + searchHit);
                }
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void insertIntoElasticDb() {
        String indexName = "products";
        String prodId = "pll1";
        // Now time to insert the product pojo
        Product product = Product.builder()
                .id(prodId)
                .name("Lenovo Legion 5")
                .description("High end gaming laptop")
                .price(150)
                .build();
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.id(prodId);
        try {
            indexRequest.source(new ObjectMapper().writeValueAsString(product), XContentType.JSON);
            IndexResponse indexResponse = getConnClient().index(indexRequest, RequestOptions.DEFAULT);
            System.out.println("Response id: " + indexResponse.getId());
            System.out.println("Response name: " + indexResponse.getResult().name());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    // Deleting a particular record with a ID
    public void deleteRecord() {
        String indexName = "products";
        String idToDelete = "pll2";
        DeleteRequest deleteRequest = new DeleteRequest(indexName, idToDelete);
        try {
            DeleteResponse deleteResponse = getConnClient().delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println("response id: " + deleteResponse.getResult().toString());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    // Deleting the whole index (database)
    public void deleteIndex() {
        String indexName = "haha";
        try {
            //Delete the index
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            getConnClient().indices().delete(request, RequestOptions.DEFAULT);
            System.out.println("Deleted Index: " + indexName);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            System.out.println("Delete Index failed: " + indexName);
        }
    }

    public void aggregationInElasticDb() {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("products");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        searchSourceBuilder.aggregation(AggregationBuilders.sum("sum").field("price"));
        searchSourceBuilder.aggregation(AggregationBuilders.avg("avg").field("price"));
        searchSourceBuilder.aggregation(AggregationBuilders.min("min").field("price"));
        searchSourceBuilder.aggregation(AggregationBuilders.max("max").field("price"));
        searchSourceBuilder.aggregation(AggregationBuilders.cardinality("cardinality").field("price"));
        searchSourceBuilder.aggregation(AggregationBuilders.count("count").field("price"));
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = null;
            searchResponse = getConnClient().search(searchRequest, RequestOptions.DEFAULT);
            Sum sum = searchResponse.getAggregations().get("sum");
            double valueSum = sum.getValue();
            System.out.println("Sum: " + valueSum);
            Avg avg = searchResponse.getAggregations().get("avg");
            double valueAvg = avg.getValue();
            System.out.println("Avg: " + valueAvg);
            Min min = searchResponse.getAggregations().get("min");
            double minOutput = min.getValue();
            System.out.println("Min: " + minOutput);
            Max max = searchResponse.getAggregations().get("max");
            double maxOutput = max.getValue();
            System.out.println("Max: " + maxOutput);
            Cardinality cardinality = searchResponse.getAggregations().get("cardinality");
            long valueCardinality = cardinality.getValue();
            System.out.println("Cardinality: " + valueCardinality);
            ValueCount count = searchResponse.getAggregations().get("count");
            long valueCount = count.getValue();
            System.out.println("Count: " + valueCount);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
