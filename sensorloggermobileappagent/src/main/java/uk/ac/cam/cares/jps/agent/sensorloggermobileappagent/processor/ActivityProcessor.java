package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.arq.querybuilder.WhereBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.logging.log4j.LogManager;
import org.json.JSONObject;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.AgentConfig;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.Payload;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.SensorData;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeries;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.OntoConstants.*;

public class ActivityProcessor extends SensorDataProcessor {

    private SensorData<Integer> confidence;
    private SensorData<String> activityType;

    public ActivityProcessor(AgentConfig config, Node smartphoneIRINode) {
        super("Activity", config, smartphoneIRINode);
        logger = LogManager.getLogger(ActivityProcessor.class);
    }

    @Override
    public void addData(Payload data) {
        timeList.addAll(data.getActivityTs());
        confidence.addData(data.getConfidences());
        activityType.addData(data.getActivityTypes());
    }

    @Override
    public TimeSeries<Instant> getProcessedTimeSeries() throws Exception {
        List<String> iriList = getDataIRIs();
        List<List<?>> valueList = getValues().stream()
                .map(ArrayList::new)
                .collect(Collectors.toList());

        List<Instant> instantList = timeList.stream().map(t -> t.toInstant()).collect(Collectors.toList());

        clearData();
        return new TimeSeries<>(instantList, iriList, valueList);
    }

    @Override
    void initSensorData() {
        confidence = new SensorData<>(Integer.class);
        activityType = new SensorData<>(String.class);
        sensorData = List.of(confidence, activityType);
    }

    @Override
    public SelectBuilder buildDataIriQuery() {
        Var confidence = Var.alloc("confidence");
        Var activityType = Var.alloc("activityType");

        WhereBuilder wb = new WhereBuilder()
                .addPrefix("saref", SAREF)
                .addPrefix("ontodevice", ONTODEVICE)
                .addPrefix("rdf", RDF)
                .addPrefix("om", OM)
                .addWhere(smartphoneIRINode, "ontodevice:detectedByDevice", "?activity")
                .addWhere("?activity", "rdf:type", "ontodevice:Activity")
                .addWhere("?activity", "ontodevice:hasConfidenceLevel", confidence)
                .addWhere("?activity", "ontodevice:hasActivityType", activityType);

        return new SelectBuilder()
                .addVar(confidence).addVar(activityType).addWhere(wb);
    }

    @Override
    public void applyDataIris(JSONObject row) {
        this.confidence.setDataIri(row.getString("confidence"));
        this.activityType.setDataIri(row.getString("activityType"));
    }

    public String getActivityTypeIri() { return activityType.getDataIri(); }
    public String getConfidenceIri() { return confidence.getDataIri(); }

    @Override
    public String getOntodeviceLabel() {
        return "Activity";
    }
}
