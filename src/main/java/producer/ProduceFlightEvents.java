package producer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

import fmg.esb.lf05.Flight;
import fmg.esb.lf05.Flight.Builder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class ProduceFlightEvents {
	public static void main(final String[] args){

		final String fluegeTopic = "Fluege";
		System.out.println(LSK.values().length);

		try {
			final Properties config = readConfig("src/main/resources/properties.txt");
			config.put(ProducerConfig.CLIENT_ID_CONFIG, "ProduceFlightEventsService");
			config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
			config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			final KafkaProducer<Long, Flight> producer = new KafkaProducer<>(config);

			for (long i = 1; i < 11; i++) {
				final Long knr = (long) i;
				final Builder flight_builder = Flight.newBuilder();
				setRandomFieldValues(flight_builder, knr);
				final Flight flight = flight_builder.build();
				final ProducerRecord<Long, Flight> record = new ProducerRecord<>(fluegeTopic, knr, flight);
				producer.send(record, (metadata, exception) -> {
				    if (exception != null) {
				        System.out.printf("Failed to produce record key={} to topic={}", record.key(), metadata.topic(), exception);
				    } else {
				    	System.out.println("Sent");
				    }
				});
			}

			producer.close();
		} catch (final Exception e) {
			e.printStackTrace();
		}

	}

	public static Properties readConfig(final String configFile) throws IOException {
		if (!Files.exists(Paths.get(configFile))) {
			throw new IOException(configFile + " not found.");
		}

		final Properties config = new Properties();
		try (InputStream inputStream = new FileInputStream(configFile)) {
			config.load(inputStream);
		}

		return config;
	}

	public static void setRandomFieldValues(final Builder flightBuilder, final long knr) {
		flightBuilder.setFLA(generateString(3));
		flightBuilder.setKNR(knr);
		flightBuilder.setSTT(new Date().toInstant());
		flightBuilder.setSAA(generateString(3));
		flightBuilder.setLSK(LSK.values()[new Random().nextInt(LSK.values().length)].name());
		flightBuilder.setKKN((long)new Random().nextInt(2));
	}

	public static String generateString(final int length) {

		return new Random().ints(97, 123).limit(length)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
	}

}

enum LSK {
	S, L
}

