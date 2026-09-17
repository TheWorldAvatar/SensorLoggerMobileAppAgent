package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.Payload;
import uk.ac.cam.cares.jps.base.query.RemoteRDBStoreClient;
import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

@WebServlet(urlPatterns = { SensorLoggerMobileAppAgent.UPDATE_ROUTE,
        SensorLoggerMobileAppAgent.TIMELINE_UPDATE_ROUTE, SensorLoggerMobileAppAgent.INSTANTIATE_ROUTE })
public class SensorLoggerMobileAppAgent extends HttpServlet {
    static final String UPDATE_ROUTE = "/update";
    static final String TIMELINE_UPDATE_ROUTE = "/update_for_timeline";
    static final String INSTANTIATE_ROUTE = "/instantiate";

    private static final Logger LOGGER = LogManager.getLogger(SensorLoggerMobileAppAgent.class);
    private RemoteStoreClient storeClient;
    private RemoteStoreClient ontopClient;
    private RemoteRDBStoreClient rdbStoreClient;
    private SensorLoggerPostgresClient postgresClient;
    private TimelineAuthentication timelineAuthentication;
    private AgentConfig agentConfig;
    private ExecutorService addDataExecutor;
    private ExecutorService sendDataExecutor;
    private static final HashMap<String, SmartphoneRecordingTask> smartphoneHashmap = new HashMap<>();

    @Override
    public void init() throws ServletException {
        loggerTest();

        agentConfig = new AgentConfig();
        EndpointConfig endpointConfig = new EndpointConfig();
        rdbStoreClient = new RemoteRDBStoreClient(endpointConfig.getDburl(), endpointConfig.getDbuser(),
                endpointConfig.getDbpassword());
        postgresClient = new SensorLoggerPostgresClient(endpointConfig.getDburl(), endpointConfig.getDbuser(),
                endpointConfig.getDbpassword());
        timelineAuthentication = new TimelineAuthentication();
        storeClient = new RemoteStoreClient(endpointConfig.getKgurl(), endpointConfig.getKgurl());
        ontopClient = new RemoteStoreClient(endpointConfig.getOntopUrl());

        addDataExecutor = Executors.newFixedThreadPool(5);
        sendDataExecutor = Executors.newFixedThreadPool(5);

        Timer taskScanningTimer = new Timer();
        taskScanningTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                synchronized (smartphoneHashmap) {
                    List<String> inactiveTask = new ArrayList<>();
                    for (Map.Entry<String, SmartphoneRecordingTask> entry : smartphoneHashmap.entrySet()) {
                        SmartphoneRecordingTask task = entry.getValue();
                        String deviceId = entry.getKey();

                        if (task.shouldTerminateTask()) {
                            LOGGER.info(deviceId + ": flush all data out");
                            sendDataExecutor.submit(task::processAndSendData);
                            inactiveTask.add(deviceId);
                        } else if (task.shouldProcessData()) {
                            sendDataExecutor.submit(task::processAndSendData);
                        }
                    }

                    if (!inactiveTask.isEmpty()) {
                        LOGGER.info(String.format("tasks: %s are inactive and has been removed from the hashmap",
                                String.join(",", inactiveTask)));
                        inactiveTask.forEach(smartphoneHashmap::remove);
                    }
                }
            }
        }, agentConfig.getTimerDelay() * 1000L, agentConfig.getTimerFrequency() * 1000L);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());

        JSONObject requestParams;
        try {
            requestParams = readJsonBody(request);
        } catch (JSONException e) {
            writeResponse(response, HttpServletResponse.SC_BAD_REQUEST, "Malformed JSON request body");
            return;
        }

        String path = request.getServletPath();
        if (INSTANTIATE_ROUTE.equals(path)) {
            handleInstantiation(requestParams, response);
        } else if (UPDATE_ROUTE.equals(path) || TIMELINE_UPDATE_ROUTE.equals(path)) {
            handleUpdate(requestParams, request, response, TIMELINE_UPDATE_ROUTE.equals(path));
        } else {
            writeResponse(response, HttpServletResponse.SC_NOT_FOUND, "Unknown route");
        }
    }

    private void handleInstantiation(JSONObject requestParams, HttpServletResponse response) throws IOException {
        if (!requestParams.has("deviceId") || requestParams.isNull("deviceId")) {
            writeResponse(response, HttpServletResponse.SC_BAD_REQUEST, "deviceId is missing");
            return;
        }

        String deviceId;
        try {
            deviceId = requestParams.getString("deviceId");
        } catch (JSONException e) {
            writeResponse(response, HttpServletResponse.SC_BAD_REQUEST, "deviceId must be a string");
            return;
        }

        SmartphoneRecordingTask task = new SmartphoneRecordingTask(storeClient, rdbStoreClient, agentConfig,
                deviceId, ontopClient);
        task.instantiate();
        writeResponse(response, HttpServletResponse.SC_OK, "instantiated deviceId = " + deviceId);
    }

    private void handleUpdate(JSONObject requestParams, HttpServletRequest request, HttpServletResponse response,
            boolean authenticate) throws IOException {
        if (!validateInput(requestParams)) {
            writeResponse(response, HttpServletResponse.SC_BAD_REQUEST,
                    "one or more of the request params is missing or invalid");
            return;
        }

        String deviceId = requestParams.getString("deviceId");
        if (authenticate && !authorizeTimelineRequest(request, response, deviceId)) {
            return;
        }

        String sessionId = requestParams.getString("sessionId");
        LOGGER.info("{}: receive request", deviceId);
        SmartphoneRecordingTask task = getSmartphoneRecordingTask(deviceId);
        addDataExecutor.submit(() -> addData(requestParams, deviceId, sessionId, task));

        LOGGER.info("{} data being added", deviceId);
        writeResponse(response, HttpServletResponse.SC_OK, "data being processed");
    }

    private boolean authorizeTimelineRequest(HttpServletRequest request, HttpServletResponse response,
            String deviceId) throws IOException {
        final String userId;
        try {
            userId = timelineAuthentication.authenticate(request.getHeader("Authorization"));
        } catch (TimelineAuthentication.AuthenticationException e) {
            LOGGER.warn(e.getMessage());
            response.setHeader("WWW-Authenticate", "Bearer");
            writeResponse(response, HttpServletResponse.SC_UNAUTHORIZED, e.getMessage());
            return false;
        }

        try {
            if (!postgresClient.isPhoneOwnedByUser(deviceId, userId)) {
                writeResponse(response, HttpServletResponse.SC_FORBIDDEN,
                        "Device is not registered to the authenticated user");
                return false;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to verify ownership for device {}", deviceId, e);
            writeResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Unable to verify device ownership");
            return false;
        }
        return true;
    }

    private void addData(JSONObject requestParams, String deviceId, String sessionId, SmartphoneRecordingTask task) {
        try {
            final JSONArray payload;
            if (requestParams.has("compressedData")) {
                LOGGER.info("{} is fetched and start to add data", deviceId);
                byte[] compressedData = Base64.getDecoder().decode(requestParams.getString("compressedData"));
                payload = new JSONArray(decompressGzip(compressedData));
            } else {
                payload = requestParams.getJSONArray("payload");
                LOGGER.info("{} is fetched and start to add data", deviceId);
            }
            task.addData(new Payload(payload, sessionId));
        } catch (IOException | JSONException | IllegalArgumentException e) {
            LOGGER.error("Failed to add data for device {}", deviceId, e);
        }
    }

    boolean validateInput(JSONObject requestParams) {
        if (!requestParams.has("messageId") || !requestParams.has("sessionId") || !requestParams.has("deviceId")
                || (!requestParams.has("payload") && !requestParams.has("compressedData"))) {
            return false;
        }
        try {
            requestParams.getString("sessionId");
            requestParams.getString("deviceId");
            if (requestParams.has("compressedData")) {
                requestParams.getString("compressedData");
            } else {
                requestParams.getJSONArray("payload");
            }
            return true;
        } catch (JSONException e) {
            return false;
        }
    }

    private JSONObject readJsonBody(HttpServletRequest request) throws IOException {
        StringBuilder body = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line);
            }
        }
        return new JSONObject(body.toString());
    }

    private void writeResponse(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.getWriter().write(new JSONObject().put(status >= 400 ? "error" : "message", message).toString());
    }

    private String decompressGzip(byte[] compressedData) throws IOException {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressedData));
                InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            StringBuilder outStr = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                outStr.append(line);
            }
            return outStr.toString();
        }
    }

    private SmartphoneRecordingTask getSmartphoneRecordingTask(String deviceId) {
        synchronized (smartphoneHashmap) {
            if (smartphoneHashmap.containsKey(deviceId)) {
                return smartphoneHashmap.get(deviceId);
            }

            LOGGER.info("{}: creating new task", deviceId);
            SmartphoneRecordingTask task = new SmartphoneRecordingTask(storeClient, rdbStoreClient, agentConfig,
                    deviceId, ontopClient);
            smartphoneHashmap.put(deviceId, task);
            return task;
        }
    }

    private void loggerTest() {
        LOGGER.debug("This is a debug message.");
        LOGGER.info("This is an info message.");
        LOGGER.warn("This is a warn message.");
        LOGGER.error("This is an error message.");
        LOGGER.fatal("This is a fatal message.");
    }
}
