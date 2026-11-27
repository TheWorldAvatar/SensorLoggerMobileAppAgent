package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.arq.querybuilder.WhereBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.json.JSONObject;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.AgentConfig;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.Payload;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.SensorData;

import java.util.List;

import static uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.OntoConstants.*;

public class RelativeBrightnessProcessor extends SensorDataDownsampledProcessor {

    private SensorData<Double> brightness;

    public RelativeBrightnessProcessor(AgentConfig config, Node smartphoneIRINode) {
        super("RelativeBrightness", config, smartphoneIRINode,
                config.getRbDSResolution(),
                config.getRbDSType());
    }

    @Override
    public void addData(Payload data) {
        timeList.addAll(data.getBrightnessTs());
        brightness.addData(data.getBrightness());
    }

    @Override
    void initSensorData() {
        brightness = new SensorData<>(Double.class);
        sensorData = List.of(brightness);
    }

    @Override
    public SelectBuilder buildDataIriQuery() {
        Var varO = Var.alloc("o");

        WhereBuilder wb = new WhereBuilder()
                .addPrefix("slma", SLA)
                .addPrefix("saref", SAREF)
                .addPrefix("ontodevice", ONTODEVICE)
                .addPrefix("rdf", RDF)
                .addPrefix("om", OM)
                .addWhere(smartphoneIRINode, "ontodevice:hasScreenBrightness", "?relativeBrightness")
                .addWhere("?relativeBrightness", "rdf:type", "ontodevice:RelativeBrightness")
                .addWhere("?relativeBrightness", "om:hasValue", varO);

        return new SelectBuilder()
                .addVar(varO).addWhere(wb);
    }

    @Override
    public void applyDataIris(JSONObject row) {
        brightness.setDataIri(row.getString("o"));
    }

    @Override
    public String getOntodeviceLabel() {
        return "RelativeBrightness";
    }
}
