package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import java.nio.file.Path;

/** Fixed queue settings with an environment override for the worker count. */
final class UploadQueueConfig {
    final Path directory = Path.of("/var/lib/sensorlogger/queue");
    final long maxBytes = 1073741824L;
    final long reserveBytes = 1073741824L;
    final int requestBytes = 10485760;
    final int decompressedBytes = 52428800;
    final int workers = Math.toIntExact(number("UPLOAD_QUEUE_WORKERS", 4));
    final int maxAttempts = 10;
    final long retryBaseMillis = 5000L;
    final long retryMaxMillis = 300000L;

    private static String value(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
    private static long number(String key, long fallback) {
        long result = Long.parseLong(value(key, Long.toString(fallback)));
        if (result <= 0) throw new IllegalArgumentException(key + " must be positive");
        return result;
    }
}
