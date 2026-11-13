package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import java.nio.file.Path;

/** Environment configuration kept separate from sensor downsampling settings. */
final class UploadQueueConfig {
    final Path directory = Path.of(value("UPLOAD_QUEUE_DIR", "/var/lib/sensorlogger/queue"));
    final long maxBytes = number("UPLOAD_QUEUE_MAX_BYTES", 1073741824L);
    final long reserveBytes = number("UPLOAD_QUEUE_RESERVE_BYTES", 1073741824L);
    final int requestBytes = byteLimit("UPLOAD_MAX_REQUEST_BYTES", 10485760);
    final int decompressedBytes = byteLimit("UPLOAD_MAX_DECOMPRESSED_BYTES", 52428800);
    final int workers = Math.toIntExact(number("UPLOAD_QUEUE_WORKERS", 5));
    final int maxAttempts = Math.toIntExact(number("UPLOAD_QUEUE_MAX_ATTEMPTS", 10));
    final long retryBaseMillis = Math.multiplyExact(number("UPLOAD_QUEUE_RETRY_SECONDS", 5), 1000L);
    final long retryMaxMillis = Math.multiplyExact(number("UPLOAD_QUEUE_MAX_RETRY_SECONDS", 300), 1000L);

    private static int byteLimit(String key, long fallback) {
        int limit = Math.toIntExact(number(key, fallback));
        if (limit == Integer.MAX_VALUE) throw new IllegalArgumentException(key + " must be below Integer.MAX_VALUE");
        return limit;
    }

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
