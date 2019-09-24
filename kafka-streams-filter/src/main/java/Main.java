import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String TOPIC = "twitter_tweets";
    private static final String FILTERED_TOPIC = "important_tweets";

    private static final String URL = "localhost";
    private static final String PORT = "9092";
    private static final String APP_ID = "kafka-demo-streams";


    public static void main(String[] args) {

        KafkaStreams kafkaStreams = KafkaHelper.initFilterStream(URL, PORT, APP_ID, TOPIC, FILTERED_TOPIC);

        kafkaStreams.start();

    }


}
