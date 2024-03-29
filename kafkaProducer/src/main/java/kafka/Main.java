package kafka;


import kafka.producer.TwitterProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down application...");
            TwitterProducer.shutdown();
        }));

        TwitterProducer.run();

    }
}
