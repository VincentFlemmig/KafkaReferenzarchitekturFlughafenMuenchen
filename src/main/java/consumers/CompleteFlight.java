package consumers;

import java.time.Instant;
import java.util.List;

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.avro_schemas.KsqlDataSourceSchema;

public class CompleteFlight {
	
	public static void main(final String[] args) {
		final String ksqlServerUrl = <ksqlDBEndpoint>;
		final int ksqlPort = 443;
		final String apiKey = <ksqlDBAPIKey>;;
		final String apiSecret = <ksqlDBAPISecret>;
			
		// KSQL-Client erstellen
		try (final Client client = Client.create(ClientOptions.create()
														.setHost(ksqlServerUrl)
														.setPort(ksqlPort)
														.setUseTls(true)
														.setUseAlpn(true)
														.setBasicAuthCredentials(apiKey, apiSecret))) {
			client.assertSchema(100056, true);
			
			System.out.println("Connected");
			final String query = "SELECT STT_L, STT_S FROM JOIN_KKN_L_S WHERE KKN_L = 1;";
			final BatchedQueryResult batchedQueryResult = client.executeQuery(query);
			
			// Wait for query result
			final List<Row> resultRows = batchedQueryResult.get();
			for (final Row row : resultRows) {
				final KsqlDataSourceSchema fsl = KsqlDataSourceSchema.newBuilder().build();
				for (final String colum : row.columnNames()) {
					fsl.put(colum, row.getValue(colum));
				}
				System.out.println("STT_S: " + Instant.ofEpochSecond(fsl.getSTTS() / (1_000_000L), (fsl.getSTTS() % (1_000_000L)) * 1_000L));
				System.out.println("STT_S: " + Instant.ofEpochSecond(fsl.getSTTL() / (1_000_000L), (fsl.getSTTL() % (1_000_000L)) * 1_000L));
			}

		} catch (final Exception e) {
			e.printStackTrace();
		}
	}
}
