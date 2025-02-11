package streams;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import fmg.esb.lf05.FlightUpdateOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class ValidateFlugAuftrag {

	public static void main(final String[] args) {
		final String updateFlugAuftragTopic = "UpdateFlugAuftrag";
		final String validierterUpdateFlugAuftragTopic = "ValidierterUpdateFlugAuftrag";

		final KafkaStreams UpdateFlugAuftragStream;

		Properties stream_config = null;

		try {
			stream_config = readConfig("src/main/resources/properties.txt");
			stream_config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ValidateFlugAuftragService");
			stream_config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
			stream_config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
			stream_config.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

			System.out.println("initialised config");

			// Avro-Serde konfigurieren
			final Map<String, String> serdeConfig = new HashMap<>();
			serdeConfig.put("schema.registry.url", <SchemaRegistryURL>);
			serdeConfig.put("basic.auth.credentials.source", "USER_INFO");
			serdeConfig.put("basic.auth.user.info",	<SchemaRegistryKey> + ":" + <SchemaRegistrySecret>);

			final SpecificAvroSerde<FlightUpdateOrder> flightSerde = new SpecificAvroSerde<>();
			flightSerde.configure(serdeConfig, false); // false = für Value konfigurieren

			final StreamsBuilder builder = new StreamsBuilder();

			final KStream<Long, FlightUpdateOrder> flightUpdatesOrders = builder
					.<Long, FlightUpdateOrder>stream(updateFlugAuftragTopic);
			flightUpdatesOrders
					.filter((key, updateFlightAuftrag) -> {

						if (updateFlightAuftrag.getLSK() != null) {
							return updateFlightAuftrag.getLSK().toString().equals("L")
							    || updateFlightAuftrag.getLSK().toString().equals("S");
						}
						return true;
					}).filter((key, updateFlightAuftrag) -> updateFlightAuftrag.getKNR() == null)
					.to(validierterUpdateFlugAuftragTopic, Produced.with(Serdes.Long(), flightSerde));

			final Topology topology = builder.build();

			UpdateFlugAuftragStream = new KafkaStreams(topology, stream_config);

			Runtime.getRuntime().addShutdownHook(new Thread(UpdateFlugAuftragStream::close));

			UpdateFlugAuftragStream.start();

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

}
