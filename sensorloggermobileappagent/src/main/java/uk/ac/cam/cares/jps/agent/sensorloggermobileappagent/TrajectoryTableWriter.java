package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

import org.postgis.Point;
import org.springframework.core.io.ClassPathResource;

import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor.ActivityProcessor;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor.LocationDataProcessor;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeries;

/** Maintains the shared query projection using the sensor-write transaction. */
public class TrajectoryTableWriter {
    private static boolean initialized;

    public static synchronized void initialize(Connection connection) throws SQLException {
        if (initialized) {
            return;
        }
        try (InputStream input = new ClassPathResource("trajectory_tables.sql").getInputStream();
                Statement statement = connection.createStatement()) {
            statement.execute(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            try (InputStream cache = new ClassPathResource("date_cache.sql").getInputStream()) {
                statement.execute(new String(cache.readAllBytes(), StandardCharsets.UTF_8));
            }
            initialized = true;
        } catch (IOException e) {
            throw new SQLException("Cannot read trajectory table schema", e);
        }
    }

    public void lockDevice(Connection connection, String deviceId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
            statement.setString(1, deviceId);
            statement.execute();
        }
    }

    static double sourceTime(Instant instant) {
        // Match TimeSeriesRDBClientOntop's time_as_number conversion exactly.
        return instant.toEpochMilli() / 1000.0;
    }

    public void write(Connection connection, String deviceId, List<TimeSeries<Instant>> batches,
            LocationDataProcessor location, ActivityProcessor activity) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TEMP TABLE trajectory_dirty_points (source_time DOUBLE PRECISION PRIMARY KEY) ON COMMIT DROP");
            statement.execute("CREATE TEMP TABLE trajectory_dirty_activities (source_time DOUBLE PRECISION PRIMARY KEY) ON COMMIT DROP");
        }
        for (TimeSeries<Instant> batch : batches) {
            if (batch.getDataIRIs().contains(location.getPointIri())) {
                writePoints(connection, deviceId, batch, location);
            }
            if (batch.getDataIRIs().contains(activity.getActivityTypeIri())) {
                writeActivities(connection, deviceId, batch, activity);
            }
        }
        // Only convert this batch's GPS points, for timezones requested previously.
        // The caller holds the same device lock used by lazy cache backfills.
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO public.device_available_dates(device_id, timezone, local_date)
                SELECT DISTINCT p.device_id, c.timezone,
                    (to_timestamp(p.time / 1000.0) AT TIME ZONE c.timezone)::date
                FROM trajectory_dirty_points d
                JOIN public.trajectory_points p ON p.source_time = d.source_time
                JOIN public.device_date_cache c ON c.device_id = p.device_id
                WHERE p.device_id = ?
                ON CONFLICT DO NOTHING
                """)) {
            statement.setString(1, deviceId);
            statement.executeUpdate();
        }

        // Both arrival orders, including corrections, use the same nearest rule.
        // Indexed ranges restrict work before sorting by absolute distance.
        try (PreparedStatement statement = connection.prepareStatement("""
                WITH affected AS (
                    SELECT p.device_id, p.source_time FROM public.trajectory_points p
                    JOIN trajectory_dirty_points d USING (source_time)
                    WHERE p.device_id = ?
                    UNION
                    SELECT p.device_id, p.source_time FROM trajectory_dirty_activities d
                    JOIN public.trajectory_points p
                      ON p.device_id = ?
                     AND p.source_time BETWEEN d.source_time - 5.001 AND d.source_time + 5.001
                ), matched AS (
                    SELECT p.device_id, p.source_time, a.activity_type,
                           a.source_time AS activity_source_time
                    FROM affected p
                    LEFT JOIN LATERAL (
                        SELECT a.activity_type, a.source_time
                        FROM public.trajectory_activities a
                        WHERE a.device_id = p.device_id
                          AND a.source_time BETWEEN p.source_time - 5.001 AND p.source_time + 5.001
                          AND ABS(ROUND(p.source_time * 1000)::BIGINT
                                  - ROUND(a.source_time * 1000)::BIGINT) <= 5000
                        ORDER BY ABS(ROUND(p.source_time * 1000)::BIGINT
                                     - ROUND(a.source_time * 1000)::BIGINT), a.source_time
                        LIMIT 1
                    ) a ON TRUE
                )
                UPDATE public.trajectory_points p
                SET activity_type = m.activity_type, activity_source_time = m.activity_source_time
                FROM matched m
                WHERE p.device_id = m.device_id AND p.source_time = m.source_time
                """)) {
            statement.setString(1, deviceId);
            statement.setString(2, deviceId);
            statement.executeUpdate();
        }
    }

    private void writePoints(Connection connection, String deviceId, TimeSeries<Instant> batch,
            LocationDataProcessor location) throws SQLException {
        List<Point> points = batch.getValuesAsPoint(location.getPointIri());
        List<?> speeds = batch.getValues(location.getSpeedIri());
        List<?> altitudes = batch.getValues(location.getAltitudeIri());
        List<?> bearings = batch.getValues(location.getBearingIri());
        List<?> sessions = batch.getValues(location.getSessionIri());
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO public.trajectory_points
                    (device_id, source_time, geom, speed, altitude, bearing, session_id)
                VALUES (?, ?, ST_SetSRID(ST_MakePoint(?, ?), 4326), ?, ?, ?, ?)
                ON CONFLICT (device_id, source_time) DO UPDATE SET
                    geom = EXCLUDED.geom,
                    speed = EXCLUDED.speed, altitude = EXCLUDED.altitude,
                    bearing = EXCLUDED.bearing, session_id = EXCLUDED.session_id
                """);
                PreparedStatement dirty = connection.prepareStatement(
                        "INSERT INTO trajectory_dirty_points VALUES (?) ON CONFLICT DO NOTHING")) {
            for (int i = 0; i < batch.getTimes().size(); i++) {
                double time = sourceTime(batch.getTimes().get(i));
                statement.setString(1, deviceId);
                statement.setDouble(2, time);
                statement.setDouble(3, points.get(i).getX());
                statement.setDouble(4, points.get(i).getY());
                statement.setObject(5, speeds.get(i));
                statement.setObject(6, altitudes.get(i));
                statement.setObject(7, bearings.get(i));
                statement.setObject(8, sessions.get(i));
                statement.addBatch();
                dirty.setDouble(1, time);
                dirty.addBatch();
            }
            statement.executeBatch();
            dirty.executeBatch();
        }
    }

    private void writeActivities(Connection connection, String deviceId, TimeSeries<Instant> batch,
            ActivityProcessor activity) throws SQLException {
        List<?> types = batch.getValues(activity.getActivityTypeIri());
        List<?> confidences = batch.getValues(activity.getConfidenceIri());
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO public.trajectory_activities (device_id, source_time, activity_type, confidence)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (device_id, source_time) DO UPDATE SET
                    activity_type = EXCLUDED.activity_type, confidence = EXCLUDED.confidence
                """);
                PreparedStatement dirty = connection.prepareStatement(
                        "INSERT INTO trajectory_dirty_activities VALUES (?) ON CONFLICT DO NOTHING")) {
            for (int i = 0; i < batch.getTimes().size(); i++) {
                double time = sourceTime(batch.getTimes().get(i));
                statement.setString(1, deviceId);
                statement.setDouble(2, time);
                statement.setObject(3, types.get(i));
                statement.setObject(4, confidences.get(i));
                statement.addBatch();
                dirty.setDouble(1, time);
                dirty.addBatch();
            }
            statement.executeBatch();
            dirty.executeBatch();
        }
    }
}
