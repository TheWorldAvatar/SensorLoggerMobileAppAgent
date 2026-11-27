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

import java.util.List;

import static uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.OntoConstants.*;

public class GravityDataProcessor extends SensorDataDownsampledProcessor {

    public GravityDataProcessor(AgentConfig config, Node smartphoneIRINode) {
        super("GravitySensor", config, smartphoneIRINode,
                config.getGravityDSResolution(),
                config.getGravityDSType());
        logger = LogManager.getLogger(GravityDataProcessor.class);
    }

    @Override
    void initSensorData() {
        x = new SensorData<>(Double.class);
        y = new SensorData<>(Double.class);
        z = new SensorData<>(Double.class);
        sensorData = List.of(x, y, z);
    }

    @Override
    public void addData(Payload data) {
        timeList.addAll(data.getGravityTs());
        x.addData(data.getGravityXs());
        y.addData(data.getGravityYs());
        z.addData(data.getGravityZs());
    }

    @Override
    public SelectBuilder buildDataIriQuery() {
        Var x = Var.alloc("x");
        Var y = Var.alloc("y");
        Var z = Var.alloc("z");

        WhereBuilder wb = new WhereBuilder()
                .addPrefix("slma", SLA)
                .addPrefix("saref", SAREF)
                .addPrefix("ontodevice", ONTODEVICE)
                .addPrefix("rdf", RDF)
                .addPrefix("om", OM)
                .addWhere(smartphoneIRINode, "saref:consistsOf", "?gravitySensor")
                .addWhere("?gravitySensor", "rdf:type", "ontodevice:GravitySensor")
                .addWhere("?gravitySensor", "ontodevice:measures", "?vector")
                .addWhere("?vector", "rdf:type", "ontodevice:GravityVector")
                .addWhere("?vector", "ontodevice:hasXComponent", "?quantityX")
                .addWhere("?quantityX", "om:hasValue", x)
                .addWhere("?vector", "ontodevice:hasYComponent", "?quantityY")
                .addWhere("?quantityY", "om:hasValue", y)
                .addWhere("?vector", "ontodevice:hasZComponent", "?quantityZ")
                .addWhere("?quantityZ", "om:hasValue", z);
        return new SelectBuilder()
                .addVar(x).addVar(y).addVar(z).addWhere(wb);
    }

    @Override
    public void applyDataIris(JSONObject row) {
        this.x.setDataIri(row.getString("x"));
        this.y.setDataIri(row.getString("y"));
        this.z.setDataIri(row.getString("z"));
    }

    @Override
    public String getOntodeviceLabel() {
        return "GravitySensor";
    }
}
