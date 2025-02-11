package streams;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema.Field;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import fmg.esb.lf05.Flight;
import fmg.esb.lf05.FlightUpdateOrder;
import fmg.esb.lf05.Flight.Builder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class UpdateFlight {
	public static void main(final String[] args) {
		final String validierterUpdateFlugAuftragTopic = "ValidierterUpdateFlugAuftrag";
		final String fluegeTopic = "Fluege";
		final KafkaStreams streamTableInnerJoin;

		Properties stream_config = null;
		Properties table_config = null;
		try {
			stream_config = readConfig("src/main/resources/properties.txt");
			stream_config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ProduceUpdatedFlightsService");
			stream_config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
			stream_config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
			stream_config.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

			table_config = readConfig("src/main/resources/properties.txt");
			table_config.put(StreamsConfig.APPLICATION_ID_CONFIG, "UpdateFlightsService");
			table_config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
			table_config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
			table_config.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

			System.out.println("initialised config");

			// Avro-Serde konfigurieren
			final Map<String, String> serdeConfig = new HashMap<>();
			serdeConfig.put("schema.registry.url", <SchemaRegistryURL>);
			serdeConfig.put("basic.auth.credentials.source", "USER_INFO");
			serdeConfig.put("basic.auth.user.info",	<SchemaRegistryKey> + ":" + <SchemaRegistrySecret>);

			final SpecificAvroSerde<Flight> flightSerde = new SpecificAvroSerde<>();
			flightSerde.configure(serdeConfig, false); // false = für Value konfigurieren

			final StreamsBuilder builder = new StreamsBuilder();

			final KTable<Long, Flight> flights = builder.table(fluegeTopic);

			final KStream<Long, FlightUpdateOrder> flightUpdatesOrders = builder
					.<Long, FlightUpdateOrder>stream(validierterUpdateFlugAuftragTopic);

			final KStream<Long, Flight> updatedFlights = flightUpdatesOrders.join(flights, (update, flight) -> joinOperation(update, flight) );
			updatedFlights.to(fluegeTopic, Produced.with(Serdes.Long(), flightSerde));
			
			final Topology topology = builder.build();
			
			streamTableInnerJoin = new KafkaStreams(topology, stream_config);
			

			Runtime.getRuntime().addShutdownHook(new Thread(streamTableInnerJoin::close));

			streamTableInnerJoin.start();
			System.out.println("Stream gestartet");

		} catch (final Exception e) {
			e.printStackTrace();
		}

	}

	public static Properties readConfig(final String configFile) throws IOException {
		// reads the client configuration from client.properties
		// and returns it as a Properties object
		if (!Files.exists(Paths.get(configFile))) {
			throw new IOException(configFile + " not found.");
		}

		final Properties config = new Properties();
		try (InputStream inputStream = new FileInputStream(configFile)) {
			config.load(inputStream);
		}

		return config;
	}

	public static Builder setMandatoryFields(final Builder flightBuilder, final long knr) {
		flightBuilder.setKNR(knr);
		flightBuilder.setFLA(generateString(3));
		flightBuilder.setSTT(new Date().toInstant());
		flightBuilder.setSAA(generateString(3));
		flightBuilder.setLSK(generateString(3));
		return flightBuilder;
	}

	public static String generateString(final int length) {

		return new Random().ints(97, 123).limit(length)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
	}

	public static Flight joinOperation(final FlightUpdateOrder update, final Flight flight) {
		if (((long) flight.get("KNR")) == 0) {
			return null;
		}
		final Flight flightUpdate = setMandatoryFields(Flight.newBuilder(), (long) flight.get("KNR")).build();
		for (final Field field : flight.getSchema().getFields()) {
			if (field.name().equals("KNR")) {
				continue;
			}
			if (update.get(field.name()) == null) {
				flightUpdate.put(field.name(), flight.get(field.name()));
			} else {
				flightUpdate.put(field.name(), update.get(field.name()));
			}
		}
		return flightUpdate;
	}

}
