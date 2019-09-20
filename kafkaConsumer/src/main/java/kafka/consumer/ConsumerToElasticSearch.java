package kafka.consumer;

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
    private static final String PROT = "9092";
    private static final String GROUP_ID = "kafka-demo-elasticsearch";
    private static final Consumer<String,String> consumer = KafkaHelper.initConsumer(URL,PROT,GROUP_ID,TOPIC);

    public static void run() throws IOException {
        RestHighLevelClient client = ElasticSearchClient.createClient();

        try{
            while(true){
                ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String,String> consumerRecord : consumerRecords){
                    LOG.info("Key:" + consumerRecord.key() + " Value: " + consumerRecord.value());
                    LOG.info( "Topic: "+ consumerRecord.topic());
                    LOG.info("Partition: " +  consumerRecord.partition());
                    LOG.info("Offset: " + consumerRecord.offset());

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",//make sure the index has been created upfront on bonsai using the console and a rest put via /twitter
                            "tweets"
                    ).source(consumerRecord.value(), XContentType.JSON);

                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();

                    LOG.info("ID: " + id);

                    Thread.sleep(1000);

                }
            }
        }catch(WakeupException e){
            LOG.info("Received Shutdown signal");

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            client.close();
        }
    }

    public static void shutdown(){
        consumer.wakeup();
    }



}
