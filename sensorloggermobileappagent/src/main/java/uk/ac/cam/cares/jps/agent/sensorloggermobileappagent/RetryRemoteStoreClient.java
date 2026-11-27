package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;

final class RetryRemoteStoreClient extends RemoteStoreClient {
    private static final Logger LOGGER = LogManager.getLogger(RetryRemoteStoreClient.class);
    private static final int MAX_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MILLIS = 10_000L;

    RetryRemoteStoreClient(String queryEndpoint) {
        super(queryEndpoint);
    }

    RetryRemoteStoreClient(String queryEndpoint, String updateEndpoint) {
        super(queryEndpoint, updateEndpoint);
    }

    @Override
    public JSONArray executeQuery(String query) {
        for (int attempt = 1; ; attempt++) {
            try {
                return super.executeQuery(query);
            } catch (RuntimeException failure) {
                if (attempt == MAX_ATTEMPTS || Thread.currentThread().isInterrupted()) {
                    throw failure;
                }
                LOGGER.warn("SPARQL query attempt {} failed. Retrying in {} ms.", attempt, RETRY_DELAY_MILLIS);
                try {
                    Thread.sleep(RETRY_DELAY_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure.addSuppressed(interrupted);
                    throw failure;
                }
            }
        }
    }
}
