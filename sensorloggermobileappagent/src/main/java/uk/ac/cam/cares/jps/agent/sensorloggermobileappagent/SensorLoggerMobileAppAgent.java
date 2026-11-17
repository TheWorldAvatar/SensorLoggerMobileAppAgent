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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
    private com.zaxxer.hikari.HikariDataSource connectionPool;
    private TimelineAuthentication timelineAuthentication;
    private AgentConfig agentConfig;
    private UploadQueueConfig queueConfig;
    private DiskUploadQueue uploadQueue;
    private volatile boolean stopping;
    private ExecutorService sendDataExecutor;


    @Override
    public void init() throws ServletException {
        loggerTest();

        agentConfig = new AgentConfig();
        EndpointConfig endpointConfig = new EndpointConfig();
        rdbStoreClient = new RemoteRDBStoreClient(endpointConfig.getDburl(), endpointConfig.getDbuser(),
                endpointConfig.getDbpassword());

        timelineAuthentication = new TimelineAuthentication();
        storeClient = new RemoteStoreClient(endpointConfig.getKgurl(), endpointConfig.getKgurl());
        ontopClient = new RemoteStoreClient(endpointConfig.getOntopUrl());

        queueConfig = new UploadQueueConfig();
        connectionPool = UploadConnectionPool.create(endpointConfig.getDburl(), endpointConfig.getDbuser(),
                endpointConfig.getDbpassword(), queueConfig.workers);
        postgresClient = new SensorLoggerPostgresClient(connectionPool);
        try {
            uploadQueue = new DiskUploadQueue(queueConfig.directory, queueConfig.maxBytes, queueConfig.reserveBytes);
        } catch (IOException | RuntimeException e) {
            connectionPool.close();
            throw new ServletException("Unable to open persistent upload queue", e);
        }
        sendDataExecutor = Executors.newFixedThreadPool(queueConfig.workers);
        for (int i = 0; i < queueConfig.workers; i++) sendDataExecutor.submit(this::consumeUploads);
    }
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());

        JSONObject requestParams;
        try {
            requestParams = readJsonBody(request);
        } catch (RequestTooLargeException e) {
            writeResponse(response, 413, "Request exceeds upload size limit");
            return;
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
                deviceId, ontopClient, postgresClient);
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

        String userId = null;
        long started = System.nanoTime();
        if (authenticate) {
            try {
                userId = timelineAuthentication.authenticate(request.getHeader("Authorization"));
            } catch (TimelineAuthentication.AuthenticationException e) {
                response.setHeader("WWW-Authenticate", "Bearer");
                writeResponse(response, HttpServletResponse.SC_UNAUTHORIZED, e.getMessage());
                return;
            }
        }
        long authenticated = System.nanoTime();
        try {
            uploadQueue.enqueue(requestParams, userId);
        } catch (IOException e) {
            LOGGER.warn("Unable to durably enqueue upload", e);
            response.setHeader("Retry-After", "5");
            writeResponse(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Upload queue unavailable; retry later");
            return;
        }
        LOGGER.info("Upload accepted device={} session={} message={} authenticationMs={} enqueueMs={} queueBytes={}",
                requestParams.getString("deviceId"), requestParams.getString("sessionId"), requestParams.get("messageId"),
                (authenticated - started) / 1000000, (System.nanoTime() - authenticated) / 1000000, uploadQueue.sizeBytes());
        writeResponse(response, HttpServletResponse.SC_ACCEPTED, "Upload durably accepted for processing");
    }

    private static class RejectedUpload extends RuntimeException {
        RejectedUpload(String message) { super(message); }
    }

    private void consumeUploads() {
        while (!stopping && !Thread.currentThread().isInterrupted()) {
            DiskUploadQueue.Entry entry = null;
            try {
                entry = uploadQueue.claim();
                if (entry == null) {
                    Thread.sleep(250);
                    continue;
                }
                processUpload(entry);
                uploadQueue.complete(entry);
                LOGGER.info("Upload committed: {}", entry.path.getFileName());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOGGER.error("Upload processing failed: {}", entry == null ? "queue scan" : entry.path.getFileName(), e);
                if (entry != null) {
                    try {
                        uploadQueue.fail(entry, e, e instanceof RejectedUpload, queueConfig.maxAttempts,
                                queueConfig.retryBaseMillis, queueConfig.retryMaxMillis);
                    } catch (IOException failure) {
                        LOGGER.error("Unable to record retry; original upload remains on disk", failure);
                        // Do not hot-loop on an unavailable spool.
                        try { Thread.sleep(1000); } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt(); return;
                        }
                    }
                } else {
                    try { Thread.sleep(1000); } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt(); return;
                    }
                }
            } finally {
                if (entry != null) uploadQueue.release(entry);
            }
        }
    }

    private void processUpload(DiskUploadQueue.Entry entry) throws Exception {
        JSONObject request = entry.record.getJSONObject("request");
        String deviceId = entry.deviceId();
        if (entry.record.has("userId")
                && !postgresClient.isPhoneOwnedByUser(deviceId, entry.record.getString("userId"))) {
            throw new RejectedUpload("Device is not registered to the authenticated user");
        }
        final JSONArray payload;
        try {
            payload = request.has("compressedData")
                    ? new JSONArray(decompressGzip(Base64.getDecoder().decode(request.getString("compressedData"))))
                    : request.getJSONArray("payload");
        } catch (IOException | JSONException | IllegalArgumentException e) {
            throw new RejectedUpload("Invalid compressed sensor payload: " + e.getMessage());
        }
        SmartphoneRecordingTask task = new SmartphoneRecordingTask(storeClient, rdbStoreClient, agentConfig,
                deviceId, ontopClient, postgresClient);
        try {
            task.addData(new Payload(payload, request.getString("sessionId")));
        } catch (JSONException | IllegalArgumentException e) {
            throw new RejectedUpload("Invalid sensor readings: " + e.getMessage());
        }
        task.processAndSendData();
    }

    @Override
    public void destroy() {
        stopping = true;
        try {
            if (sendDataExecutor != null) {
                sendDataExecutor.shutdown();
                try {
                    if (!sendDataExecutor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                        sendDataExecutor.shutdownNow();
                        if (!sendDataExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                            LOGGER.error("Upload workers still running; retaining queue lock until process exits");
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    sendDataExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if (uploadQueue != null) {
                try { uploadQueue.close(); } catch (IOException e) { LOGGER.error("Unable to close upload queue", e); }
            }
        } finally {
            if (connectionPool != null) connectionPool.close();
            super.destroy();
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
        long started = System.nanoTime();
        if (request.getContentLengthLong() > queueConfig.requestBytes) throw new RequestTooLargeException();
        try (java.io.InputStream input = request.getInputStream()) {
            byte[] body = input.readNBytes(queueConfig.requestBytes + 1);
            if (body.length > queueConfig.requestBytes) throw new RequestTooLargeException();
            JSONObject result = new JSONObject(new String(body, StandardCharsets.UTF_8));
            LOGGER.info("Upload request read/JSON parse: bytes={} elapsedMs={}", body.length,
                    (System.nanoTime() - started) / 1000000);
            return result;
        }
    }

    private static class RequestTooLargeException extends IOException { }

    private void writeResponse(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.getWriter().write(new JSONObject().put(status >= 400 ? "error" : "message", message).toString());
    }

    private String decompressGzip(byte[] compressedData) throws IOException {
        try (GZIPInputStream input = new GZIPInputStream(new ByteArrayInputStream(compressedData))) {
            byte[] data = input.readNBytes(queueConfig.decompressedBytes + 1);
            if (data.length > queueConfig.decompressedBytes) throw new IOException("Decompressed payload exceeds limit");
            return new String(data, StandardCharsets.UTF_8);
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
