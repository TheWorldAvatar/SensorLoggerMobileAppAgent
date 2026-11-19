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

public class DBFSDataProcessor extends SensorDataDownsampledProcessor {

    private SensorData<Double> dBFS;

    public DBFSDataProcessor(AgentConfig config, Node smartphoneNode) {
        super("DBFS", config, smartphoneNode, config.getDbfsDSResolution(), config.getDbfsDSType());
        logger = LogManager.getLogger(DBFSDataProcessor.class);
    }

    @Override
    public void addData(Payload data) {
        timeList.addAll(data.getdBFSTs());
        dBFS.addData(data.getdBFSs());
    }

    @Override
    void initSensorData() {
        dBFS = new SensorData<>(Double.class);
        sensorData = List.of(dBFS);
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
                .addWhere(smartphoneIRINode, "saref:consistsOf", "?microphone")
                .addWhere("?microphone", "rdf:type", "ontodevice:Microphone")
                .addWhere("?microphone", "ontodevice:measures", "?om_soundPressureLevel")
                .addWhere("?om_soundPressureLevel", "om:hasValue", varO);

        return new SelectBuilder().addVar(varO).addWhere(wb);
    }

    @Override
    public void applyDataIris(JSONObject row) {
        dBFS.setDataIri(row.getString("o"));
    }

    @Override
    public String getOntodeviceLabel() {
        return "Microphone";
    }
}
