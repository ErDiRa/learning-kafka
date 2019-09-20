package ch.github.erdira.kafka.producer;

import ch.github.erdira.kafka.KafkaHelper;
import ch.github.erdira.kafka.twitterClient.TwitterClient;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * used as twitter client
 */
public class TwitterProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterProducer.class);

    private String url = "localhost";
    private String port = "9092";
    private Producer<String, String> producer = KafkaHelper.initProducer(url, port);

    private TwitterProducer(){}

    public static void run(){
        // create twitter client
        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client =  TwitterClient.create(msgQueue);
        client.connect();
        // create kafka producer

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null){
                LOG.info(msg);
                //TODO: do not forget to create the topic before using:
                /*kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor*/
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null){
                            LOG.error("onCompletion for producer failed", exception);
                        }
                    }
                });
            }

        }
    }

    public Producer<String, String> getProducer(){
        return this.producer;
    }

}
