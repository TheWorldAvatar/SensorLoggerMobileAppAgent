package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.syntax.ElementBind;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementUnion;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.ClassPathResource;

import com.cmclinnovations.stack.clients.ontop.OntopClient;

import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.Payload;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.SensorData;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor.*;
import uk.ac.cam.cares.jps.base.query.RemoteRDBStoreClient;
import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeries;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeriesClient;

import com.cmclinnovations.stack.clients.timeseries.TimeSeriesRDBClient;

import java.awt.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;

public class SmartphoneRecordingTask {
    Logger logger;

    AccelerometerProcessor accelerometerProcessor;
    DBFSDataProcessor dbfsDataProcessor;
    GravityDataProcessor gravityDataProcessor;
    IlluminationProcessor illuminationProcessor;
    LocationDataProcessor locationDataProcessor;
    MagnetometerDataProcessor magnetometerDataProcessor;
    RelativeBrightnessProcessor relativeBrightnessProcessor;
    ActivityProcessor activityProcessor;
    List<SensorDataProcessor> sensorDataProcessors;

    private final Node smartphoneIRI;
    private final String deviceId;
    private final DeviceMetadataCache metadataCache;

    private final RemoteStoreClient ontopRemoteStoreClient;
    private final RemoteStoreClient blazegraphStoreClient;
    private final TimeSeriesClient<Instant> tsClient;
    private final AgentConfig config;

    // Prepared batches survive a failed write; newly arriving data stays buffered.
    private List<TimeSeries<Instant>> pendingTimeSeries;
    private final TrajectoryTableWriter trajectoryTableWriter = new TrajectoryTableWriter();
    private long lastProcessedTime;
    private long lastActiveTime;
    private boolean isProcessing;

    private SensorLoggerPostgresClient sensorLoggerPostgresClient;

    public SmartphoneRecordingTask(RemoteStoreClient storeClient, RemoteRDBStoreClient rdbStoreClient,
            AgentConfig config, String deviceId, RemoteStoreClient ontopRemoteStoreClient) {
        this(storeClient, rdbStoreClient, config, deviceId, ontopRemoteStoreClient,
                new SensorLoggerPostgresClient(rdbStoreClient.getRdbURL(),
                        rdbStoreClient.getUser(), rdbStoreClient.getPassword()));
    }

    public SmartphoneRecordingTask(RemoteStoreClient storeClient, RemoteRDBStoreClient rdbStoreClient,
            AgentConfig config, String deviceId, RemoteStoreClient ontopRemoteStoreClient,
            SensorLoggerPostgresClient postgresClient) {
        this(storeClient, rdbStoreClient, config, deviceId, ontopRemoteStoreClient, postgresClient, null);
    }

    SmartphoneRecordingTask(RemoteStoreClient storeClient, RemoteRDBStoreClient rdbStoreClient,
            AgentConfig config, String deviceId, RemoteStoreClient ontopRemoteStoreClient,
            SensorLoggerPostgresClient postgresClient, DeviceMetadataCache metadataCache) {
        logger = LogManager.getLogger("SmartphoneRecordingTask_" + deviceId);
        this.metadataCache = metadataCache;
        this.sensorLoggerPostgresClient = java.util.Objects.requireNonNull(postgresClient);

        this.ontopRemoteStoreClient = ontopRemoteStoreClient;
        TimeSeriesRDBClient<Instant> tsRdbClient = new TimeSeriesRDBClient<>(Instant.class) {
            @Override public java.sql.Connection getConnection() throws java.sql.SQLException {
                return postgresClient.getConnection();
            }
        };
        tsRdbClient.setRdbURL(rdbStoreClient.getRdbURL());
        tsRdbClient.setRdbUser(rdbStoreClient.getUser());
        tsRdbClient.setRdbPassword(rdbStoreClient.getPassword());

        this.blazegraphStoreClient = storeClient;
        this.tsClient = new TimeSeriesClient<>(blazegraphStoreClient, tsRdbClient);
        this.config = config;
        lastActiveTime = System.currentTimeMillis();
        lastProcessedTime = System.currentTimeMillis();

        String smartphoneString = "https://www.theworldavatar.com/kg/sensorloggerapp/smartphone_" + deviceId;
        smartphoneIRI = NodeFactory.createURI(smartphoneString);
        this.deviceId = deviceId;

        initSensorProcessors();
        if (metadataCache == null) {
            loadMetadata();
        }

    }

    public synchronized void addData(Payload data) {
        logger.info("adding data...");
        sensorDataProcessors.forEach(p -> p.addData(data));
        lastActiveTime = System.currentTimeMillis();
        logger.info("finish adding data and the last active time is updated to " + lastActiveTime);
    }

    public synchronized boolean shouldProcessData() {
        if (System.currentTimeMillis() - lastProcessedTime < config.getTimerFrequency() * 1000L) {
            logger.debug(String.format("Current time: %d, last processed time: %d; No need to process data.",
                    System.currentTimeMillis(),
                    lastProcessedTime));
            return false;
        } else if (isProcessing) {
            logger.debug("Another thread is processing the data, current thread should skip");
            return false;
        } else {
            return true;
        }
    }

    public synchronized void processAndSendData() {
        isProcessing = true;
        try {
            logger.info("Processing and sending data");

            ensureMetadata();
            bulkAddTimeSeriesData();
        } finally {
            lastProcessedTime = System.currentTimeMillis();
            isProcessing = false;
        }
    }

    private void loadMetadata() {
        resolveSensorDataIris(sensorDataProcessors, true);
        resolveTimeSeriesIris(sensorDataProcessors);
    }

    private void ensureMetadata() {
        if (metadataCache == null) {
            initializeMissingMetadata();
            return;
        }

        synchronized (metadataCache.lockFor(deviceId)) {
            Map<String, List<String>> cached = metadataCache.get(deviceId);
            if (cached != null) {
                for (SensorDataProcessor processor : sensorDataProcessors) {
                    List<String> iris = cached.get(processor.getSensorName());
                    List<SensorData<?>> fields = processor.getSensorData();
                    for (int i = 0; i < fields.size(); i++) {
                        fields.get(i).setDataIri(iris.get(i));
                    }
                }
                return;
            }

            loadMetadata();
            initializeMissingMetadata();
            Map<String, List<String>> metadata = new HashMap<>();
            for (SensorDataProcessor processor : sensorDataProcessors) {
                for (SensorData<?> field : processor.getSensorData()) {
                    if (field.getDataIri() == null || field.getDataIri().isBlank()
                            || field.isNeedToInitTimeSeries()) {
                        throw new IllegalStateException("Sensor metadata is incomplete for " + deviceId);
                    }
                }
                metadata.put(processor.getSensorName(), List.copyOf(processor.getDataIRIs()));
            }
            metadataCache.put(deviceId, metadata);
        }
    }


    private void initializeMissingMetadata() {
        List<SensorDataProcessor> processorsToInstantiate = sensorDataProcessors.stream()
                .filter(SensorDataProcessor::isNeedToInstantiateDevice).toList();
        if (!processorsToInstantiate.isEmpty()) {
            logger.info("Need to init device: " + processorsToInstantiate.stream()
                    .map(SensorDataProcessor::getOntodeviceLabel).collect(Collectors.joining(",")));
            initDeviceKgUsingOntop(processorsToInstantiate);
        }

        List<SensorDataProcessor> processorsToInitTimeSeries = sensorDataProcessors.stream()
                .filter(SensorDataProcessor::isNeedToInitTimeSeries).toList();
        if (!processorsToInitTimeSeries.isEmpty()) {
            logger.info("Need to init rdb for " + processorsToInitTimeSeries.stream()
                    .map(SensorDataProcessor::getOntodeviceLabel).collect(Collectors.joining(",")));
            initDevicesInRDB();
        }

        fixUninitializedSensorData();
    }

    public synchronized boolean shouldTerminateTask() {
        return pendingTimeSeries == null
                && sensorDataProcessors.stream().allMatch(p -> p.getTimeSeriesLength() == 0)
                && System.currentTimeMillis() - lastActiveTime > config.getTaskInactiveTime() * 1000L;
    }

    private void initSensorProcessors() {
        accelerometerProcessor = new AccelerometerProcessor(config, smartphoneIRI);
        dbfsDataProcessor = new DBFSDataProcessor(config, smartphoneIRI);
        gravityDataProcessor = new GravityDataProcessor(config, smartphoneIRI);
        illuminationProcessor = new IlluminationProcessor(config, smartphoneIRI);
        locationDataProcessor = new LocationDataProcessor(config, smartphoneIRI);
        magnetometerDataProcessor = new MagnetometerDataProcessor(config, smartphoneIRI);
        relativeBrightnessProcessor = new RelativeBrightnessProcessor(config, smartphoneIRI);
        // Always preserve raw activity observations in the source time series.
        activityProcessor = new ActivityProcessor(config, smartphoneIRI);

        sensorDataProcessors = new ArrayList<>(Arrays.asList(accelerometerProcessor,
                dbfsDataProcessor,
                gravityDataProcessor,
                illuminationProcessor,
                locationDataProcessor,
                magnetometerDataProcessor,
                relativeBrightnessProcessor,
                activityProcessor));
    }

    private void resolveSensorDataIris(List<SensorDataProcessor> processors, boolean allowUnmappedOntop) {
        if (processors.isEmpty()) {
            return;
        }

        Var sensor = Var.alloc("sensor");
        Query query = new Query();
        query.setQuerySelectType();
        query.addResultVar(sensor);
        ElementUnion union = new ElementUnion();
        Map<String, SensorDataProcessor> pending = new HashMap<>();
        Set<Var> resultVars = new LinkedHashSet<>();
        for (SensorDataProcessor processor : processors) {
            String sensorName = processor.getSensorName();
            pending.put(sensorName, processor);
            Query sensorQuery = processor.buildDataIriQuery().build();
            resultVars.addAll(sensorQuery.getProjectVars());
            query.getPrefixMapping().setNsPrefixes(sensorQuery.getPrefixMapping());
            ElementGroup branch = new ElementGroup();
            branch.addElement(sensorQuery.getQueryPattern());
            branch.addElement(new ElementBind(sensor, NodeValue.makeString(sensorName)));
            union.addElement(branch);
        }
        resultVars.forEach(query::addResultVar);
        query.setQueryPattern(union);

        JSONArray rows;
        try {
            rows = ontopRemoteStoreClient.executeQuery(query.toString());
        } catch (RuntimeException e) {
            if (!allowUnmappedOntop) {
                throw e;
            }
            // Ontop can reject queries before its first mapping is installed.
            logger.warn("Initial Ontop lookup failed for {}", deviceId, e);
            return;
        }
        for (Object result : rows) {
            JSONObject row = (JSONObject) result;
            SensorDataProcessor processor = pending.remove(row.getString("sensor"));
            if (processor != null) {
                processor.applyDataIris(row);
            }
        }
    }

    private void resolveTimeSeriesIris(List<SensorDataProcessor> processors) {
        List<String> dataIris = processors.stream()
                .flatMap(processor -> processor.getDataIRIs().stream())
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        Map<String, String> timeSeriesByDataIri = new HashMap<>();
        if (!dataIris.isEmpty()) {
            Var dataIri = Var.alloc("dataIRI");
            Var tsIri = Var.alloc("tsIRI");
            SelectBuilder query = new SelectBuilder()
                    .addPrefix("ontots", OntoConstants.ONTOTS)
                    .setDistinct(true)
                    .addVar(dataIri)
                    .addVar(tsIri)
                    .addWhere(dataIri, "ontots:hasTimeSeries", tsIri)
                    .addValueVar(dataIri);
            dataIris.forEach(iri -> query.addValueRow(NodeFactory.createURI(iri)));
            for (Object result : blazegraphStoreClient.executeQuery(query.buildString())) {
                JSONObject row = (JSONObject) result;
                timeSeriesByDataIri.putIfAbsent(row.getString("dataIRI"), row.getString("tsIRI"));
            }
        }
        processors.forEach(processor -> processor.applyTimeSeriesIris(timeSeriesByDataIri));
    }

    private void initDeviceKgUsingOntop(List<SensorDataProcessor> processors) {
        logger.info("Instantiating kg in ontop");

        List<String> sensorClasses = processors.stream().map(SensorDataProcessor::getOntodeviceLabel)
                .collect(Collectors.toList());

        boolean firstTime = sensorLoggerPostgresClient.populateTable(deviceId, sensorClasses);

        if (firstTime) {
            Path obdaFile = null;
            try {
                obdaFile = new ClassPathResource("ontop.obda").getFile().toPath();
            } catch (IOException e) {
                logger.error("Could not retrieve ontop.obda file.");
                throw new RuntimeException(e);
            }
            OntopClient.getInstance("ontop").updateOBDA(obdaFile);
        }

        logger.info(
                "finish instantiating kg, exception above is probably from updating the obda file and is harmless.");
        // wait 30 seconds for ontop to initialise before allow queries execution
        try {
            logger.info("Waiting for 30 seconds to allow ontop to intialise");
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during device initialization", e);
        }

        List<SensorDataProcessor> unresolved = sensorDataProcessors.stream()
                .filter(processor -> processor.getDataIRIs().contains(null))
                .toList();
        resolveSensorDataIris(unresolved, false);
        if (unresolved.stream().anyMatch(processor -> processor.getDataIRIs().contains(null))) {
            throw new IllegalStateException("Ontop sensor mappings are incomplete for " + deviceId);
        }
        resolveTimeSeriesIris(unresolved);
    }

    private void initDevicesInRDB() {
        List<SensorDataProcessor> processors = sensorDataProcessors.stream()
                .filter(SensorDataProcessor::isNeedToInitTimeSeries)
                .toList();
        if (processors.isEmpty()) {
            return;
        }
        List<List<String>> dataIris = new ArrayList<>();
        List<List<Class<?>>> dataClasses = new ArrayList<>();
        for (SensorDataProcessor p : processors) {
            dataIris.add(p.getDataIRIs());
            dataClasses.add(p.getDataClass());
        }

        logger.info("bulk init iris in rdb");
        tsClient.bulkInitTimeSeries(dataIris, dataClasses, null, 4326);

        resolveTimeSeriesIris(processors);
        if (processors.stream().anyMatch(SensorDataProcessor::isNeedToInitTimeSeries)) {
            throw new IllegalStateException("Time-series links are missing after initialization for " + deviceId);
        }
        logger.info("finish init postgres dbTable and the corresponding table");
    }

    /**
     * Updates blazegraph and time_series_quantities table for new sensor data when
     * device already exists.
     * This handles the case where the device and some of its sensor data exists,
     * but having new sensor data not yet linked with a time series in blazegraph
     * and RDB tables.
     * Only updates entries for sensor data that need to be initialized.
     * 
     * NOTICE: This function assumes all sensor data by a device has the same time
     * series.
     */
    private void fixUninitializedSensorData() {
        for (SensorDataProcessor sensorDataProcessor : sensorDataProcessors) {
            List<SensorData<?>> sensorData = sensorDataProcessor.getSensorData().stream()
                    .filter(sd -> sd.isNeedToInitTimeSeries()).collect(Collectors.toList());
            if (sensorData.isEmpty()) {
                continue;
            }

            List<String> dataIris = new ArrayList<>();
            List<Class<?>> classes = new ArrayList<>();
            for (SensorData sd : sensorData) {
                dataIris.add(sd.getDataIri());
                classes.add(sd.getType());
            }

            logger.debug(sensorDataProcessor.getSensorName() + ": " + String.join(",", dataIris)
                    + " have no tsIRI, fixing is needed.");

            tsClient.addColumnsToExistingTimeSeries(sensorDataProcessor.getTsIri(), dataIris, classes,
                    classes.contains(Point.class) ? 4326 : null);
            sensorData.forEach(sd -> sd.setNeedToInitTimeSeries(false));
        }
    }

    private void bulkAddTimeSeriesData() throws RuntimeException {
        if (pendingTimeSeries == null) {
            pendingTimeSeries = new ArrayList<>();
            for (SensorDataProcessor processor : sensorDataProcessors) {
                if (processor.getTimeSeriesLength() > 0) {
                    try {
                        pendingTimeSeries.add(processor.getProcessedTimeSeries());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        List<TimeSeries<Instant>> tsList = pendingTimeSeries;
        if (tsList.isEmpty() || tsList.stream().mapToInt(ts -> ts.getTimes().size()).sum() == 0) {
            pendingTimeSeries = null;
            logger.info("No new data, skip bulk add time series data");
            return;
        }

        // downsampling algorithm applied in getProcessedTimeSeries might make a time
        // series empty
        tsList.removeIf(longTimeSeries -> longTimeSeries.getTimes().isEmpty());

        logger.info(String.format("bulk adding %d time series", tsList.size()));
        try (java.sql.Connection connection = sensorLoggerPostgresClient.getConnection()) {
            TrajectoryTableWriter.initialize(connection);
            connection.setAutoCommit(false);
            try {
                logger.info("Waiting for device database lock: {}", deviceId);
                trajectoryTableWriter.lockDevice(connection, deviceId);
                logger.info("Writing source time series: {}", deviceId);
                tsClient.bulkaddTimeSeriesData(tsList, connection);
                logger.info("Source write finished; writing shared trajectory tables: {}", deviceId);
                trajectoryTableWriter.write(connection, deviceId, tsList,
                        locationDataProcessor, activityProcessor);
                logger.info("Shared writes finished; committing batch: {}", deviceId);
                connection.commit();
                logger.info("Batch committed: {}", deviceId);
                pendingTimeSeries = null;
            } catch (Exception e) {
                logger.error("Rolling back sensor/trajectory batch for {}", deviceId, e);
                connection.rollback();
                throw new RuntimeException("Failed to persist sensor and trajectory batch", e);
            }
        } catch (java.sql.SQLException e) {
            throw new RuntimeException(e);
        }
        logger.info("finish adding data to postgres tables");
    }

    String getSmartphoneIRI() {
        String smartphone = null;
        String query = """
                SELECT ?device
                WHERE {
                  ?device <https://www.theworldavatar.com/kg/ontodevice/hasDeviceID> "%s"
                }
                """.formatted(deviceId);
        JSONArray queryResult = ontopRemoteStoreClient.executeQuery(query);

        if (queryResult.length() == 1) {
            smartphone = queryResult.getJSONObject(0).getString("device");
        } else if (queryResult.length() > 1) {
            throw new RuntimeException("More than one smartphone associated with " + deviceId);
        }

        return smartphone;
    }

    public void instantiate() {
        try (java.sql.Connection connection = sensorLoggerPostgresClient.getConnection()) {
            TrajectoryTableWriter.initialize(connection);
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Failed to initialize shared trajectory tables", e);
        }
        initDeviceKgUsingOntop(sensorDataProcessors);
        initDevicesInRDB();
    }
}
