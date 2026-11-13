package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import org.json.JSONArray;
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
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
