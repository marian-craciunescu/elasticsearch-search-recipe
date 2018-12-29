package com.elastic.recipe;

import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 */
public class IndexRecipesApp {
    private static final Logger logger = getLogger(IndexRecipesApp.class);

    public static void main(String[] args) {

        BasicConfigurator.configure();

        //System.out.println("IndexData app");
        File jsonDir = new File("data");
        File[] files = jsonDir.listFiles(
                (dir, name) -> {
                    return name.toLowerCase().endsWith(".json");
                }
        );

        // return if nothing to do
        if (files.length == 0) {
            return;
        }

        try {
            String host = "es161";

            Settings settings = Settings.builder()
                    .put("client.transport.sniff", false)
                    .put("cluster.name", "mycluster-lab-cluster")
                    .put("xpack.security.user", "elastic:changeme")
                    .build();

            TransportClient client = new PreBuiltXPackTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(host), 9300));
            System.out.println("aici");

            for (DiscoveryNode node : client.connectedNodes()) {
                System.out.println(node.toString());
            }

            // iterate through json files, indexing each
            for (int n = 0; n < files.length; n++) {
                String json = new String(Files.readAllBytes(files[n].toPath()));
                IndexResponse response = client.prepareIndex("recipes2", "doc").setSource(json, XContentType.JSON).get();
                String _index = response.getIndex();
                String _type = response.getType();
                String _id = response.getId();
                long _version = response.getVersion();
            }

            // close es client
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
