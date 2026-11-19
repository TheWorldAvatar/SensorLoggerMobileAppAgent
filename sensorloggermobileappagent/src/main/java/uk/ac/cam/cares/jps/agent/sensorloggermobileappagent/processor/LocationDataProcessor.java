package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.arq.querybuilder.WhereBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.json.JSONObject;
import org.postgis.Point;

import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.AgentConfig;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.Payload;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.SensorData;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeries;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.OntoConstants.*;

public class LocationDataProcessor extends SensorDataProcessor {

    private SensorData<Double> bearing;
    private SensorData<Double> speed;
    private SensorData<Double> altitude;
    private SensorData<Point> geomLocation;
    private SensorData<String> session;

    public LocationDataProcessor(AgentConfig config, Node smartphoneIRINode) {
        super("GPSDevice", config, smartphoneIRINode);
    }

    @Override
    public void addData(Payload data) {
        timeList.addAll(data.getLocationTs());
        bearing.addData(data.getBearings());
        speed.addData(data.getSpeeds());
        altitude.addData(data.getAltitudes());
        geomLocation.addData(data.getGeomLocations());
        session.addData(data.getSessionIds());
    }

    @Override
    public TimeSeries<Instant> getProcessedTimeSeries() {
        // todo: do processing of location data
        List<String> dataIRIList = getDataIRIs();
        List<List<?>> valueList = getValues().stream()
                .map(ArrayList::new).collect(Collectors.toList());

        List<Instant> instantList = timeList.stream().map(t -> t.toInstant()).toList();
        TimeSeries<Instant> ts = new TimeSeries<>(new ArrayList<>(instantList), dataIRIList, valueList);

        clearData();
        return ts;
    }

    @Override
    void initSensorData() {
        bearing = new SensorData<>(Double.class);
        speed = new SensorData<>(Double.class);
        altitude = new SensorData<>(Double.class);
        geomLocation = new SensorData<>(Point.class);
        session = new SensorData<>(String.class);
        sensorData = List.of(bearing, speed, altitude, geomLocation, session);
    }

    @Override
    public SelectBuilder buildDataIriQuery() {
        Var bearing = Var.alloc("bearing");
        Var altitude = Var.alloc("altitude");
        Var speed = Var.alloc("speed");
        Var point = Var.alloc("point");
        Var session = Var.alloc("session");

        WhereBuilder wb = new WhereBuilder()
                .addPrefix("saref", SAREF)
                .addPrefix("ontodevice", ONTODEVICE)
                .addPrefix("rdf", RDF)
                .addPrefix("om", OM)
                .addPrefix("sf", SF)
                .addWhere(smartphoneIRINode, "saref:consistsOf", "?GPSDevice")
                .addWhere(smartphoneIRINode, "ontodevice:hasSessionID", "?session")
                .addWhere("?GPSDevice", "rdf:type", "ontodevice:GPSDevice")
                .addWhere("?GPSDevice", "ontodevice:measures", "?Bearing")
                .addWhere("?Bearing", "rdf:type", "ontodevice:Bearing")
                .addWhere("?Bearing", "om:hasValue", bearing)
                .addWhere("?GPSDevice", "ontodevice:measures", "?Altitude")
                .addWhere("?Altitude", "rdf:type", "ontodevice:Altitude")
                .addWhere("?Altitude", "om:hasValue", altitude)
                .addWhere("?GPSDevice", "ontodevice:measures", "?Speed")
                .addWhere("?Speed", "rdf:type", "om:Speed")
                .addWhere("?Speed", "om:hasValue", speed)
                .addWhere("?GPSDevice", "ontodevice:hasGeoLocation", point)
                .addWhere(point, "rdf:type", "sf:Point");

        return new SelectBuilder()
                .addVar(bearing).addVar(altitude).addVar(speed).addVar(point).addVar(session).addWhere(wb);
    }

    @Override
    public void applyDataIris(JSONObject row) {
        this.bearing.setDataIri(row.getString("bearing"));
        this.altitude.setDataIri(row.getString("altitude"));
        this.speed.setDataIri(row.getString("speed"));
        this.geomLocation.setDataIri(row.getString("point"));
        this.session.setDataIri(row.getString("session"));
    }

    public String getPointIri() { return geomLocation.getDataIri(); }
    public String getSpeedIri() { return speed.getDataIri(); }
    public String getAltitudeIri() { return altitude.getDataIri(); }
    public String getBearingIri() { return bearing.getDataIri(); }
    public String getSessionIri() { return session.getDataIri(); }

    @Override
    public String getOntodeviceLabel() {
        return "GPSDevice";
    }
}
