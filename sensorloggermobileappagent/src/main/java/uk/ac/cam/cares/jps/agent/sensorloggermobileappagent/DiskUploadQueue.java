package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.json.JSONObject;

/** Single-process durable spool. Files stay pending until the database commit succeeds. */
final class DiskUploadQueue implements AutoCloseable {
    static class FullException extends IOException {
        FullException() { super("Upload queue capacity or free disk reserve exhausted"); }
    }

    static final class Entry {
        final Path path;
        final JSONObject record;
        Entry(Path path, JSONObject record) { this.path = path; this.record = record; }
        String deviceId() { return record.getJSONObject("request").getString("deviceId"); }
    }

    private final Path root;
    private final Path pending;
    private final Path failed;
    private final long maxBytes;
    private final long reserveBytes;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Set<String> activeDevices = new HashSet<>();
    private static final class Metadata {
        final String device;
        long nextAttempt;
        Metadata(JSONObject record) {
            device = record.getJSONObject("request").getString("deviceId");
            nextAttempt = record.optLong("nextAttempt");
        }
    }
    // Only small routing metadata stays in memory; payloads are read when claimed.
    private final NavigableMap<Path, Metadata> index = new TreeMap<>();
    private long bytes;
    private long sequence = System.currentTimeMillis();
    private boolean closed;

    DiskUploadQueue(Path directory, long maxBytes, long reserveBytes) throws IOException {
        root = directory.toAbsolutePath();
        pending = Files.createDirectories(root.resolve("pending"));
        failed = Files.createDirectories(root.resolve("failed"));
        this.maxBytes = maxBytes;
        this.reserveBytes = reserveBytes;
        lockChannel = FileChannel.open(root.resolve("queue.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock acquired;
        try { acquired = lockChannel.tryLock(); }
        catch (RuntimeException | IOException e) { lockChannel.close(); throw e; }
        if (acquired == null) { lockChannel.close(); throw new IOException("Queue directory is already in use"); }
        lock = acquired;
        try (Stream<Path> files = Files.walk(root)) {
            for (Path path : files.filter(Files::isRegularFile).toList()) {
                if (path.getFileName().toString().endsWith(".tmp")) Files.delete(path);
                else if (!path.getFileName().toString().equals("queue.lock")) {
                    bytes += Files.size(path);
                    String name = path.getFileName().toString();
                    if (name.matches("[0-9]{19}-.*")) sequence = Math.max(sequence, Long.parseLong(name.substring(0, 19)));
                    if (path.getParent().equals(pending) && name.endsWith(".json")) {
                        try { index.put(path, new Metadata(new JSONObject(Files.readString(path)))); }
                        catch (RuntimeException e) { quarantine(path); }
                    }
                }
            }
            syncDirectory(root);
        } catch (IOException | RuntimeException e) {
            lock.release(); lockChannel.close(); throw e;
        }
    }

    synchronized void enqueue(JSONObject request, String userId) throws IOException {
        if (closed) throw new IOException("Queue is shutting down");
        JSONObject record = new JSONObject().put("request", request).put("attempts", 0)
                .put("nextAttempt", 0).put("acceptedAt", System.currentTimeMillis());
        if (userId != null) record.put("userId", userId);
        byte[] data = encode(record);
        if (data.length > maxBytes - bytes || Files.getFileStore(root).getUsableSpace() - data.length < reserveBytes)
            throw new FullException();
        sequence = Math.max(sequence + 1, System.currentTimeMillis());
        Path target = pending.resolve(String.format("%019d-%s.json", sequence, UUID.randomUUID()));
        bytes += data.length;
        try {
            writeAtomic(target, data);
            index.put(target, new Metadata(record));
        }
        catch (IOException e) {
            if (!Files.exists(target)) bytes -= data.length;
            else index.put(target, new Metadata(record));
            throw e;
        }
    }

    synchronized Entry claim() throws IOException {
        if (closed) return null;
        Set<String> seen = new HashSet<>();
        var iterator = index.entrySet().iterator();
        while (iterator.hasNext()) {
            var item = iterator.next();
            Path path = item.getKey();
            Metadata metadata = item.getValue();
            // A retrying message holds its place ahead of later uploads for the same device.
            if (!seen.add(metadata.device) || activeDevices.contains(metadata.device)
                    || metadata.nextAttempt > System.currentTimeMillis()) continue;
            JSONObject record;
            try { record = new JSONObject(Files.readString(path)); }
            catch (RuntimeException e) {
                quarantine(path); iterator.remove(); continue;
            }
            activeDevices.add(metadata.device);
            return new Entry(path, record);
        }
        return null;
    }

    private void quarantine(Path path) throws IOException {
        Files.move(path, failed.resolve(path.getFileName()), StandardCopyOption.ATOMIC_MOVE);
        syncDirectory(failed); syncDirectory(pending);
    }

    synchronized void complete(Entry entry) throws IOException {
        long size = Files.size(entry.path);
        Files.delete(entry.path);
        index.remove(entry.path);
        bytes -= size;
        syncDirectory(pending);
    }

    synchronized void fail(Entry entry, Exception error, boolean permanent, int maxAttempts,
            long baseDelayMillis, long maxDelayMillis) throws IOException {
        int attempts = entry.record.optInt("attempts") + 1;
        long factor = 1L << Math.min(attempts - 1, 20);
        long delay = baseDelayMillis > maxDelayMillis / factor ? maxDelayMillis : baseDelayMillis * factor;
        entry.record.put("attempts", attempts).put("lastError", error.toString().substring(0,
                Math.min(error.toString().length(), 1000))).put("lastFailedAt", System.currentTimeMillis())
                .put("nextAttempt", System.currentTimeMillis() + delay + new Random().nextInt((int) Math.min(delay / 4 + 1, 10000)));
        long oldSize = Files.size(entry.path);
        byte[] data = encode(entry.record);
        writeAtomic(entry.path, data);
        bytes += data.length - oldSize;
        index.get(entry.path).nextAttempt = entry.record.getLong("nextAttempt");
        if (permanent || attempts >= maxAttempts) {
            try { quarantine(entry.path); }
            finally { if (!Files.exists(entry.path)) index.remove(entry.path); }
        }
    }

    synchronized void release(Entry entry) { activeDevices.remove(entry.deviceId()); }
    synchronized long sizeBytes() { return bytes; }

    private static byte[] encode(JSONObject record) {
        return record.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private void writeAtomic(Path target, byte[] data) throws IOException {
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            syncDirectory(target.getParent());
        } finally { Files.deleteIfExists(temporary); }
    }

    private static void syncDirectory(Path path) throws IOException {
        // Windows does not expose directory fsync through FileChannel. Production runs on Linux.
        if (System.getProperty("os.name").startsWith("Windows")) return;
        try (FileChannel directory = FileChannel.open(path, StandardOpenOption.READ)) { directory.force(true); }
    }

    @Override public synchronized void close() throws IOException {
        closed = true;
        lock.release();
        lockChannel.close();
    }
}
