import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaHelper {

    public static KafkaStreams initFilterStream(String url, String port, String appId, String topic, String filteredTopic) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, url + ":" + port);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId); //similar to group id
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputStream = streamsBuilder.stream(topic);
        KStream<String, String> filteredStream = inputStream.filter(
                (key, jsonTweet) -> {
                    return extractUserFollowersInTweet(jsonTweet) > 10000;
                }
        );
        filteredStream.to(filteredTopic); //Make sure this topics exist on kafka

        //Build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        return kafkaStreams;

    }

    private static int extractUserFollowersInTweet(String jsonTweet) {

        JsonParser jsonParser = new JsonParser();


        try {
            int followers = jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
            jsonParser = null;
            return followers;
        } catch (NullPointerException e) {
            jsonParser = null;
            return 0;
        }

    }
}
