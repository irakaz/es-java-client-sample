package com.zenika.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ElasticsearchDocumentNewVersionTest {

    public static final String INDEX_NAME = "java-index";
    public static final String DOC_TYPE = "_doc";
    private static RestHighLevelClient client;

    @BeforeAll
    static void setUpOnce() {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    @AfterAll
    static void tearDownOnce() throws IOException {
        client.close();
    }

    @Test
    @Order(1)
    void testIndexDocument() throws IOException {
        final XContentBuilder document = XContentFactory.jsonBuilder()
                .startObject()
                .field("firstname", "James")
                .field("name", "BOND")
                .endObject();

        IndexRequest request = new IndexRequest(INDEX_NAME)
                .id("1")
                .source(document)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response);

        assertThat(response.getResult()).isIn(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED);
    }

    @Test
    @Order(2)
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest(INDEX_NAME, "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        System.out.println(response);

        assertThat(response.isExists()).isEqualTo(true);
    }

    @Test
    @Order(3)
    void testSearchForDocument() throws IOException {
        QueryBuilder query = QueryBuilders.matchQuery("name", "bond");
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(response);

        assertThat(response.getHits().getTotalHits().value).isEqualTo(1);
    }

    @Test
    @Order(4)
    void testDeleteDocument() throws IOException {

        DeleteRequest request = new DeleteRequest(INDEX_NAME,"1");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);

        System.out.println(response);

        assertThat(response.getResult()).isEqualTo(DocWriteResponse.Result.DELETED);
    }
}
