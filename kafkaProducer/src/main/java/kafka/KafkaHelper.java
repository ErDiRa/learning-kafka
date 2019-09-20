package kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.omg.PortableInterceptor.INACTIVE;

import java.util.Properties;

public class KafkaHelper {

    public static Producer<String,String> initProducer(String url, String port){
        // Set the properties (https://kafka.apache.org/documentation/#producerconfigs)
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url + ":" + port);
        props.put(ProducerConfig.ACKS_CONFIG, "all"); //wait for all replicas to acknowledge data
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Add safety:
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put("min.insync.replicas","2");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //Just use 5 if kafka version >2.0 otherwise use 1

        // increase throughput (at the expense of a bit latency(linger) and CPU usage(compression)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20"); //delay before sending a batch
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //about 32kBs

        return new KafkaProducer<String, String>(props);
    }

}
