package ch.github.erdira.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class MainProducer {

    public static void main(String[] args) {


        new TwitterProducer().run();


    }
}
