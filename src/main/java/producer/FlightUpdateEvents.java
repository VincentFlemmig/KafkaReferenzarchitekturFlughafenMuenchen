package producer;

import java.lang.System;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.*;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import java.io.IOException;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import fmg.esb.lf05.Flight;
import fmg.esb.lf05.FlightUpdateOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.vertx.core.file.FileSystemOptions;
import streams.SchemaRegistryURL;

public class FlightUpdateEvents {

	public static void main(final String[] args) throws IOException {
		
		if(args[0] == null)
	    {
	        System.err.println("Anzahl an Updates müssen als Parameter angegeben werden");
	        System.exit(0);
	    }
		
		final String updateFlugAuftragTopic = "UpdateFlugAuftrag";
		final String fluegeTopic = "Fluege";
		final String schemaUrl = <SchemaRegistryURL>;
		final String schemaKey = <SchemaRegistryKey>;
		final String schemaSecret = <SchemaRegistrySecret>;
		final String properties = "src/main/resources/properties.txt";
		
		try {

			final Properties producerConfig = getProducerConfig(properties);
			final Properties streamConfig   = getStreamConfig(properties);

			if (producerConfig == null || streamConfig == null) {
				System.err.println("Properties Konnten nicht gesetzt werden");
			}

			final KafkaProducer<Long, FlightUpdateOrder> producer = new KafkaProducer<>(producerConfig);

			final SpecificAvroSerde<Flight> specificFlightSerde = getSpecificFlightSerde(schemaUrl, schemaKey + ":"+schemaSecret);

			// Streams-Topologie definieren
			final StreamsBuilder builder = new StreamsBuilder();

			// KStream aus dem flightsCompacted Topic erstellen
			final KStream<Long, Flight> flightStream = builder.stream(fluegeTopic);

			// aus diesem Stream einen KTable bauen
			flightStream.toTable(Materialized.<Long, Flight, KeyValueStore<Bytes, byte[]>>as("flight-ktable-store-new")
					.withKeySerde(Serdes.Long()).withValueSerde(specificFlightSerde));

			final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamConfig);
		
			kafkaStreams.start();

			Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

			// Warte, bis Kafka Streams vollständig gestartet ist und sich der Table
			// aufgebaut hat.
			waitForKafkaStreamsToBeRunning(kafkaStreams);

			// Iteriere über alle Werte im Store und generiere Zufällige Updates
			final AtomicInteger count = new AtomicInteger(0);
			final ReadOnlyKeyValueStore<Long, Flight> store = kafkaStreams.store(
					StoreQueryParameters.fromNameAndType("flight-ktable-store-new", QueryableStoreTypes.keyValueStore()));
			while (count.get() < Integer.parseInt(args[0])) {
				final KeyValueIterator<Long, Flight> iterator = store.all();
				iterator.forEachRemaining((entry -> {
					if (entry.key != 0) {
						if (ThreadLocalRandom.current().nextInt(100) < 3) {
							final FlightUpdateOrder updatedFlight = 
									generateUpdatedFlight(entry.value);
							final ProducerRecord<Long, FlightUpdateOrder> record = 
									new ProducerRecord<Long, FlightUpdateOrder>(
											updateFlugAuftragTopic, entry.key, updatedFlight);
							producer.send(record);
							count.incrementAndGet();
						}
					}
				}));
			}
			producer.close();
			kafkaStreams.close();
			System.exit(0);

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

	private static void waitForKafkaStreamsToBeRunning(final KafkaStreams kafkaStreams) {
		final Instant start = Instant.now();
		while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
			try {
				System.out.println(kafkaStreams.state());
				Thread.sleep(100); // 100 ms warten, um den Zustand erneut zu prüfen
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException("Interrupted while waiting for Kafka Streams to start", e);
			}
		}

		final Instant end = Instant.now();
		final Duration timeElapsed = Duration.between(start, end);
		System.out.println("Vergangene Zeit, bis Aufbau des Tables vollständig: " + timeElapsed);
	}

	public static FlightUpdateOrder generateUpdatedFlight(final Flight flight) {
		final FlightUpdateOrder fuo = FlightUpdateOrder.newBuilder().build();
		for (final Field field : fuo.getSchema().getFields()) {
			if (field.name().equals("LSK"))  fuo.put(field.name(), LSK.values()[new Random().nextInt(LSK.values().length)].name());
			if (field.name().equals("KNR") || field.name().equals("LSK") || field.name().equals("KKN")) {
				continue;
			}
			if (ThreadLocalRandom.current().nextInt(100) < 10) {
				fuo.put(field.name(), generateUpdateValue(field));
			}
		}
		return fuo;
	}

	public static Object generateUpdateValue(final Field f) {
		final String fieldType = getFieldType(f.schema());
		switch (fieldType) {
		case "STRING":
			return generateString(ThreadLocalRandom.current().nextInt(2, 5));
		case "LONG":
			return ThreadLocalRandom.current().nextLong(1, 10000000);
		case "timestamp-micros":
			return new Date().toInstant();
		default:
			break;
		}
		return null;
	}

	private static String getFieldType(final Schema schema) {
		// Prüfe, ob es sich um einen Union-Typ handelt
		if (schema.getType() == Schema.Type.UNION) {
			// ["null", "string"]: Das Feld an Index 0 ist bei einer UNION immer der default wert. An Index 1 steht der Datentyp des Wertes
			// Deswegen rekursiver aufruf, bis eigentlicher Datentyp gefunden ist
			return getFieldType(schema.getTypes().get(1)); 
		}
		return schema.getLogicalType() == null ? schema.getType().toString() : schema.getLogicalType().getName();
	}

	public static String generateString(final int length) {

		return new Random().ints(97, 123).limit(length)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
	}

	public static Properties getProducerConfig(final String pathToConfigFile) {
		final Properties producer_config;
		try {
			producer_config = readConfig(pathToConfigFile);
			producer_config.put(ProducerConfig.CLIENT_ID_CONFIG, "CreateFlightUpdateEventsService");
			producer_config.put(ProducerConfig.RETRIES_CONFIG, 0);
			producer_config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
			producer_config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
			return producer_config;
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Properties getStreamConfig(final String pathToConfigFile) {

		final Properties stream_config;
		try {
			stream_config = readConfig(pathToConfigFile);
			stream_config.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamFlightsCompactedForUpdatesService");
			stream_config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
			stream_config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
			stream_config.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
			return stream_config;
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static SpecificAvroSerde<Flight> getSpecificFlightSerde(final String schemaUrl, final String  userInfo) {
		// Avro-Serde konfigurieren
		final Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put("schema.registry.url", schemaUrl);
		serdeConfig.put("basic.auth.credentials.source", "USER_INFO");
		serdeConfig.put("basic.auth.user.info", userInfo);

		final SpecificAvroSerde<Flight> flightSerde = new SpecificAvroSerde<>();
		flightSerde.configure(serdeConfig, false); // false = für Value konfigurieren
		return flightSerde;
	}
	
	enum LSK {
		S, L, N, K, P, Q, R, T
	}

}