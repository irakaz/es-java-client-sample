package com.zenika.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ElasticsearchAdminTest {

    public static final String INDEX_NAME = "java-index";
    private static TransportClient client;

    @BeforeAll
    static void setUpOnce() {
        Settings searchEngineSettings = Settings.builder().put("cluster.name", "elasticsearch").build();
        client = new PreBuiltTransportClient(searchEngineSettings).addTransportAddress(
                new TransportAddress(new InetSocketAddress("127.0.0.1", 9300)));
    }

    @Test
    @Order(1)
    void testCreateIndex() {
        final IndicesAdminClient indicesAdmin = client.admin().indices();
        CreateIndexResponse response = indicesAdmin.prepareCreate(INDEX_NAME)
                .setSettings(Settings.builder()
                        .put("number_of_shards", 1)
                        .put("number_of_replicas", 0)
                        .build())
                .get();
        System.out.println(String.format("index : %s, acknowledged : %s", response.index(), response.isAcknowledged()));

        assertThat(response.isAcknowledged()).isTrue();
    }

    @Test
    @Order(2)
    void testGetIndex() {
        final IndicesAdminClient indicesAdmin = client.admin().indices();
        GetIndexResponse response = indicesAdmin.prepareGetIndex().addIndices(INDEX_NAME).get();
        System.out.println(response);
        assertThat(response.indices()).contains(INDEX_NAME);
    }

    @Test
    @Order(3)
    public void testDeleteIndex() {
        final IndicesAdminClient indicesAdmin = client.admin().indices();
        AcknowledgedResponse response = indicesAdmin.prepareDelete(INDEX_NAME).get();
        System.out.println(String.format("index %s, acknowledged : %s", INDEX_NAME, response.isAcknowledged()));
        assertThat(response.isAcknowledged()).isTrue();
    }
}
