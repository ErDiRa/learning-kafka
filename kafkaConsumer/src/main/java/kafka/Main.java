package kafka;

import kafka.consumer.ConsumerToElasticSearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws IOException {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Consumer is shutting down...");
            ConsumerToElasticSearch.shutdown();
        }));

        ConsumerToElasticSearch.run();

    }

}
