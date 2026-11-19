package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** Initialized sensor metadata for one agent and its configured Blazegraph namespace. */
final class DeviceMetadataCache {
    private final int capacity;
    private final Map<String, Map<String, List<String>>> entries = new LinkedHashMap<>(16, 0.75f, true);
    private final Object[] locks = IntStream.range(0, 64).mapToObj(index -> new Object()).toArray();

    DeviceMetadataCache(int capacity) {
        this.capacity = capacity;
    }

    Object lockFor(String deviceId) {
        return locks[Math.floorMod(deviceId.hashCode(), locks.length)];
    }

    synchronized Map<String, List<String>> get(String deviceId) {
        return entries.get(deviceId);
    }

    synchronized void put(String deviceId, Map<String, List<String>> metadata) {
        entries.put(deviceId, Map.copyOf(metadata));
        if (entries.size() > capacity) {
            entries.remove(entries.keySet().iterator().next());
        }
    }

    synchronized void invalidate(String deviceId) {
        entries.remove(deviceId);
    }
}
