package kafka;

import kafka.consumer.ConsumerToElasticSearch;
import kafka.elasticSearchConnection.ElasticSearchClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws IOException {

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            LOG.info("Consumer is shutting down...");
            ConsumerToElasticSearch.shutdown();
        }));

        ConsumerToElasticSearch.run();

    }

}
