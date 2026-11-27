package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.jena.query.QueryFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.postgis.Point;
import uk.ac.cam.cares.jps.base.query.RemoteRDBStoreClient;
import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SmartphoneRecordingTaskTest {
    SmartphoneRecordingTask task;

    @Before
    public void setup() throws Exception {
        RemoteStoreClient storeClientMock = mock(RemoteStoreClient.class);
        when(storeClientMock.executeQuery(ArgumentMatchers.anyString())).thenReturn(new JSONArray());

        RemoteRDBStoreClient rdbStoreClientMock = mock(RemoteRDBStoreClient.class);
        when(rdbStoreClientMock.getRdbURL()).thenReturn("rdb_url");
        when(rdbStoreClientMock.getUser()).thenReturn("rdb_user");
        when(rdbStoreClientMock.getPassword()).thenReturn("rdb_password");

        AgentConfig agentConfigMock = mock(AgentConfig.class);
        when(agentConfigMock.getTimerFrequency()).thenReturn(2);
        when(agentConfigMock.getTaskInactiveTime()).thenReturn(2);

        String deviceId = "123";
        task = new SmartphoneRecordingTask(storeClientMock, rdbStoreClientMock, agentConfigMock, deviceId,
                storeClientMock);
        // Timing assertions start after potentially slow class/KG initialization.
        for (String name : List.of("lastProcessedTime", "lastActiveTime")) {
            java.lang.reflect.Field field = SmartphoneRecordingTask.class.getDeclaredField(name);
            field.setAccessible(true);
            field.setLong(task, System.currentTimeMillis());
        }
    }

    @Test
    public void batchesSensorMetadataLookupsAndAppliesPartialResults() {
        RemoteStoreClient ontop = mock(RemoteStoreClient.class);
        RemoteStoreClient blazegraph = mock(RemoteStoreClient.class);
        when(ontop.executeQuery(ArgumentMatchers.anyString())).thenAnswer(invocation -> {
            String query = invocation.getArgument(0);
            QueryFactory.create(query);
            assertTrue(query.contains("UNION"));
            assertTrue(query.contains("Accelerometer"));
            assertTrue(query.contains("GPSDevice"));
            assertTrue(query.contains("Activity"));
            return new JSONArray()
                    .put(new JSONObject().put("sensor", "Accelerometer")
                            .put("x", "urn:accel:x").put("y", "urn:accel:y").put("z", "urn:accel:z"))
                    .put(new JSONObject().put("sensor", "Activity")
                            .put("confidence", "urn:activity:confidence").put("activityType", "urn:activity:type"));
        });
        when(blazegraph.executeQuery(ArgumentMatchers.anyString())).thenAnswer(invocation -> {
            String query = invocation.getArgument(0);
            QueryFactory.create(query);
            assertTrue(query.contains("<urn:accel:x>"));
            assertTrue(query.contains("<urn:activity:type>"));
            return new JSONArray()
                    .put(new JSONObject().put("dataIRI", "urn:accel:x").put("tsIRI", "urn:series:accel"))
                    .put(new JSONObject().put("dataIRI", "urn:activity:type").put("tsIRI", "urn:series:activity"));
        });
        RemoteRDBStoreClient rdb = mock(RemoteRDBStoreClient.class);
        when(rdb.getRdbURL()).thenReturn("jdbc:postgresql://unused/test");

        SmartphoneRecordingTask recording = new SmartphoneRecordingTask(blazegraph, rdb,
                mock(AgentConfig.class), "123", ontop, mock(SensorLoggerPostgresClient.class));

        verify(ontop).executeQuery(ArgumentMatchers.anyString());
        verify(blazegraph).executeQuery(ArgumentMatchers.anyString());
        assertEquals("urn:series:accel", recording.accelerometerProcessor.getTsIri());
        assertEquals("urn:series:activity", recording.activityProcessor.getTsIri());
        assertFalse(recording.accelerometerProcessor.getSensorData().get(0).isNeedToInitTimeSeries());
        assertTrue(recording.accelerometerProcessor.getSensorData().get(1).isNeedToInitTimeSeries());
        assertTrue(recording.locationDataProcessor.isNeedToInstantiateDevice());
    }

    @Test
    public void repeatUploadReusesVerifiedMetadataUntilInvalidated() {
        RemoteStoreClient ontop = mock(RemoteStoreClient.class);
        RemoteStoreClient blazegraph = mock(RemoteStoreClient.class);
        RemoteRDBStoreClient rdb = mock(RemoteRDBStoreClient.class);
        when(rdb.getRdbURL()).thenReturn("jdbc:postgresql://unused/test");
        AgentConfig config = mock(AgentConfig.class);
        SensorLoggerPostgresClient postgres = mock(SensorLoggerPostgresClient.class);
        DeviceMetadataCache cache = new DeviceMetadataCache(1);

        JSONArray sensorRows = new JSONArray();
        JSONArray seriesRows = new JSONArray();
        Map.of(
                "Accelerometer", List.of("x", "y", "z"),
                "DBFS", List.of("o"),
                "GravitySensor", List.of("x", "y", "z"),
                "Camera", List.of("o"),
                "GPSDevice", List.of("bearing", "altitude", "speed", "point", "session"),
                "Magnetometer", List.of("x", "y", "z"),
                "RelativeBrightness", List.of("o"),
                "Activity", List.of("confidence", "activityType")
        ).forEach((sensor, fields) -> {
            JSONObject row = new JSONObject().put("sensor", sensor);
            for (String field : fields) {
                String iri = "urn:data:" + sensor + ":" + field;
                row.put(field, iri);
                seriesRows.put(new JSONObject().put("dataIRI", iri).put("tsIRI", "urn:series:" + sensor));
            }
            sensorRows.put(row);
        });
        when(ontop.executeQuery(ArgumentMatchers.anyString())).thenReturn(sensorRows);
        when(blazegraph.executeQuery(ArgumentMatchers.anyString())).thenReturn(seriesRows);

        SmartphoneRecordingTask first = new SmartphoneRecordingTask(blazegraph, rdb,
                config, "123", ontop, postgres, cache);
        first.processAndSendData();
        SmartphoneRecordingTask repeat = new SmartphoneRecordingTask(blazegraph, rdb,
                config, "123", ontop, postgres, cache);
        repeat.processAndSendData();

        verify(ontop).executeQuery(ArgumentMatchers.anyString());
        verify(blazegraph).executeQuery(ArgumentMatchers.anyString());
        assertEquals(first.locationDataProcessor.getDataIRIs(), repeat.locationDataProcessor.getDataIRIs());
        assertEquals(first.activityProcessor.getDataIRIs(), repeat.activityProcessor.getDataIRIs());

        cache.invalidate("123");
        new SmartphoneRecordingTask(blazegraph, rdb, config, "123", ontop, postgres, cache)
                .processAndSendData();
        verify(ontop, times(2)).executeQuery(ArgumentMatchers.anyString());
        verify(blazegraph, times(2)).executeQuery(ArgumentMatchers.anyString());
    }

    @Test
    public void shouldProcessData() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertTrue(task.shouldProcessData());
    }

    @Test
    public void shouldNotProcessData() {
        assertFalse(task.shouldProcessData());
    }

    @Test
    public void shouldTerminateTask() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertTrue(task.shouldTerminateTask());
    }

    @Test
    public void shouldNotTerminateTask() {
        assertFalse(task.shouldTerminateTask());
    }
    @Test
    public void activityProcessorIsRegistered() {
        assertNotNull(task.activityProcessor);
        assertTrue(task.sensorDataProcessors.stream().anyMatch(p -> p instanceof
                uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor.ActivityProcessor));
    }

    @Test
    public void initializationFailurePropagatesAndAlwaysReleasesProcessingFlag() throws Exception {
        var processor = mock(uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor.SensorDataProcessor.class);
        when(processor.isNeedToInstantiateDevice()).thenThrow(new IllegalStateException("initialization unavailable"));
        task.sensorDataProcessors = List.of(processor);
        try { task.processAndSendData(); fail("Failure must reach durable queue worker"); }
        catch (IllegalStateException expected) { }
        var field = SmartphoneRecordingTask.class.getDeclaredField("isProcessing");
        field.setAccessible(true);
        assertFalse(field.getBoolean(task));
    }

    @Test
    public void shutdownInterruptionStopsDeviceInitialization() throws Exception {
        var field = SmartphoneRecordingTask.class.getDeclaredField("sensorLoggerPostgresClient");
        field.setAccessible(true);
        field.set(task, mock(SensorLoggerPostgresClient.class));
        var initialize = SmartphoneRecordingTask.class.getDeclaredMethod("initDeviceKgUsingOntop", List.class);
        initialize.setAccessible(true);
        Thread.currentThread().interrupt();
        try {
            initialize.invoke(task, List.of());
            fail("Interrupted worker must leave the upload on disk");
        } catch (java.lang.reflect.InvocationTargetException e) {
            assertTrue(e.getCause().getCause() instanceof InterruptedException);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
