package com.zenika.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.*;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ElasticsearchAdminNewVersionTest {

    public static final String INDEX_NAME = "java-index";
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
    void testCreateIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
        );
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(String.format("index : %s, acknowledged : %s", response.index(), response.isAcknowledged()));

        assertThat(response.isAcknowledged()).isTrue();
    }

    @Test
    @Order(2)
    void testGetIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest(INDEX_NAME);
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);

        System.out.println(response);
        assertThat(response.getIndices()).contains(INDEX_NAME);
    }

    @Test
    @Order(3)
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(INDEX_NAME);
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);

        System.out.println(String.format("index %s, acknowledged : %s", INDEX_NAME, response.isAcknowledged()));
        assertThat(response.isAcknowledged()).isTrue();
    }
}
