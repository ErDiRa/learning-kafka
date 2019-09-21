package kafka.consumer;

import com.google.gson.JsonParser;
import kafka.KafkaHelper;
import kafka.elasticSearchConnection.ElasticSearchClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class ConsumerToElasticSearch {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerToElasticSearch.class);

    private static final String TOPIC = "twitter_tweets";

    private static final String URL = "localhost";
    private static final String PORT = "9092";
    private static final String GROUP_ID = "kafka-demo-elasticsearch";
    private static final Consumer<String,String> consumer = KafkaHelper.initConsumer(URL, PORT,GROUP_ID,TOPIC);

    public static void run() throws IOException {
        RestHighLevelClient client = ElasticSearchClient.createClient();

        try{
            while(true){
                ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String,String> consumerRecord : consumerRecords){

                    processData(client,consumerRecord);

                }
            }
        }catch(WakeupException e){
            LOG.info("Received Shutdown signal");
        } finally {
            consumer.close();
            client.close();
        }
    }

    public static void shutdown(){
        consumer.wakeup();
    }

    /**
     *Insert data into elastic search hosted on bonsai ("Processing the data")
     */
    private static void processData(RestHighLevelClient client, ConsumerRecord<String, String> consumerRecord) throws IOException {

        // Two strategies to make IDs unique and you consumer idempotent:
        // 1. kafka generic ID (use when do not have a specific id for you message)
        //String id = consumerRecord.topic() + "_"+  consumerRecord.partition() + "_" + consumerRecord.offset(); //will identify a message in kafka

        // 2. use the twitter message id
        String id = extractIdfromTweet(consumerRecord.value());


        IndexRequest indexRequest = new IndexRequest(
                "twitter",//make sure the index has been created upfront on bonsai using the console and a rest put via /twitter
                "tweets",
                id // use the id to request one message
        ).source(consumerRecord.value(), XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String responseId = indexResponse.getId();

        LOG.info("ID: " + responseId);
    }

    private static String extractIdfromTweet(String jsonMessage) {
        JsonParser jsonParser = new JsonParser();
        String id = jsonParser.parse(jsonMessage)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

        jsonParser = null;

        return id;
    }


}
