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

public class AccelerometerProcessor extends SensorDataDownsampledProcessor {

    public AccelerometerProcessor(AgentConfig config, Node smartphoneNode) {
        super("Accelerometer", config, smartphoneNode,
                config.getAccelDSResolution(),
                config.getAccelDSType()
                );
        logger = LogManager.getLogger(AccelerometerProcessor.class);
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
        timeList.addAll(data.getAccelTs());
        x.addData(data.getAccelXs());
        y.addData(data.getAccelYs());
        z.addData(data.getAccelZs());
    }

    @Override
    public SelectBuilder buildDataIriQuery() {
        Var x = Var.alloc("x");
        Var y = Var.alloc("y");
        Var z = Var.alloc("z");

        WhereBuilder wb = new WhereBuilder()
                .addPrefix("saref", SAREF)
                .addPrefix("ontodevice", ONTODEVICE)
                .addPrefix("rdf", RDF)
                .addPrefix("om", OM)
                .addWhere(smartphoneIRINode, "saref:consistsOf", "?accelerometer")
                .addWhere("?accelerometer", "rdf:type", "ontodevice:Accelerometer")
                .addWhere("?accelerometer", "ontodevice:measures", "?vector")
                .addWhere("?vector", "rdf:type", "ontodevice:AccelerationVector")
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
        return "Accelerometer";
    }
}
