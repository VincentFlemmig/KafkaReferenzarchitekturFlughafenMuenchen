import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class Main {

	public static void main(final String[] args) {
		
		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		
		for(int i = 0; i< 5; i++) {
			final ProducerRecord<String, String> record = new ProducerRecord<>("FD","Hallo","Welt"+ i);
			
			try {
				producer.send(record).get();
				Thread.sleep(1);
				
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
		producer.close();
		
	}

}
