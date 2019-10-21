package com.zenika.es;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ElasticsearchDocumentTest {

    public static final String INDEX_NAME = "java-index";
    public static final String DOC_TYPE = "_doc";
    private static TransportClient client;

    @BeforeAll
    public static void setUpOnce() {
        Settings searchEngineSettings = Settings.builder().put("cluster.name", "elasticsearch").build();
        client = new PreBuiltTransportClient(searchEngineSettings).addTransportAddress(
                new TransportAddress(new InetSocketAddress("127.0.0.1", 9300)));
    }

    @Test
    @Order(1)
    void testIndexDocument() throws IOException {
        final XContentBuilder document = XContentFactory.jsonBuilder()
                .startObject()
                .field("firstname", "James")
                .field("name", "BOND")
                .endObject();
        IndexResponse response = client.prepareIndex(INDEX_NAME, DOC_TYPE)
                .setId("1")
                .setSource(document)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        System.out.println(response);

        assertThat(response.getResult()).isIn(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED);
    }

    @Test
    @Order(2)
    void testGetDocument() {
        GetResponse response = client.prepareGet(INDEX_NAME, DOC_TYPE, "1").get();
        System.out.println(response);

        assertThat(response.isExists()).isEqualTo(true);
    }

    @Test
    @Order(3)
    void testSearchForDocument() {
        QueryBuilder query = QueryBuilders.matchQuery("name", "bond");
        SearchResponse response = client.prepareSearch(INDEX_NAME).setQuery(query).get();
        System.out.println(response);

        assertThat(response.getHits().getTotalHits().value).isEqualTo(1);
    }

    @Test
    @Order(4)
    void testDeleteDocument() {
        DeleteResponse response = client.prepareDelete(INDEX_NAME, DOC_TYPE, "1").get();
        System.out.println(response);

        assertThat(response.getResult()).isEqualTo(DocWriteResponse.Result.DELETED);
    }
}
