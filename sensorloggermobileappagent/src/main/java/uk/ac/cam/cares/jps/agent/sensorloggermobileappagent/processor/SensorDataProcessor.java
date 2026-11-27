package uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.processor;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.Node;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.AgentConfig;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.Payload;
import uk.ac.cam.cares.jps.agent.sensorloggermobileappagent.model.SensorData;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeries;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

public abstract class SensorDataProcessor {
    AgentConfig config;
    Node smartphoneIRINode;
    final List<OffsetDateTime> timeList = new ArrayList<>();
    List<SensorData<?>> sensorData;
    String sensorName;
    String tsIri;
    Logger logger;

    public SensorDataProcessor(String sensorName, AgentConfig config, Node smartphoneIRINode) {
        this.sensorName = sensorName;
        this.config = config;
        this.smartphoneIRINode = smartphoneIRINode;

        initSensorData();
    }

    public abstract void addData(Payload data);

    public abstract TimeSeries<Instant> getProcessedTimeSeries() throws Exception;

    abstract void initSensorData();

    public List<SensorData<?>> getSensorData() {
        return sensorData;
    };

    public List<Class<?>> getDataClass() {
        return getSensorData().stream().map(SensorData::getType).collect(Collectors.toList());
    };

    public List<String> getDataIRIs() {
        return getSensorData().stream().map(SensorData::getDataIri).toList();
    };

    public List<List<?>> getValues() {
        return getSensorData().stream().map(SensorData::getValues).collect(Collectors.toList());
    }

    void clearData() {
        timeList.clear();
        getSensorData().forEach(SensorData::clearData);
    };

    public abstract SelectBuilder buildDataIriQuery();

    public abstract void applyDataIris(JSONObject row);

    public void applyTimeSeriesIris(Map<String, String> timeSeriesByDataIri) {
        tsIri = null;
        for (SensorData<?> field : sensorData) {
            String dataIri = field.getDataIri();
            String linkedSeries = dataIri == null ? null : timeSeriesByDataIri.get(dataIri);
            field.setNeedToInitTimeSeries(linkedSeries == null);
            if (tsIri == null) {
                tsIri = linkedSeries;
            }
        }
    }

    public String getTsIri() {
        return tsIri;
    }

    public boolean isNeedToInstantiateDevice() {
        return getDataIRIs().stream().allMatch(iri -> iri == null);
    }

    public boolean isNeedToInitTimeSeries() {
        return tsIri == null;
    }

    public int getTimeSeriesLength() {
        return timeList.size();
    }

    public String getSensorName() {
        return sensorName;
    }

    public abstract String getOntodeviceLabel();
}
