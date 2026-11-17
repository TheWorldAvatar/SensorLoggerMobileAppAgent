# SensorLoggerMobileAppAgent

## 1. Description

The SensorLoggerMobileAppAgent is an agent which receives HTTP POST requests containing JSON payload sent from the [SensorLogger](https://github.com/tszheichoi/awesome-sensor-logger) mobile application, subsequently instantiate it as time series following the [OntoDevice](https://github.com/cambridge-cares/TheWorldAvatar/tree/main/JPS_Ontology/ontology/ontodevice) ontology. The information instantiated from SensorLogger includes: Smartphone device, Acceleration vector, Gravity vector, Magnetic flux density vector, Sound pressure level, Illuminance, Relative brightness, Location.

The agent functions as below:

1) The agent receives JSON payload from the SensorLogger and parse the received JSON Array.
2) It downsamples the received timeseries data via the [Downsampling](https://github.com/cambridge-cares/TheWorldAvatar/tree/main/core/downsampling) library, and instantiates the data using the [TimeSeriesClient](https://github.com/cambridge-cares/TheWorldAvatar/tree/main/JPS_BASE_LIB/src/main/java/uk/ac/cam/cares/jps/base/timeseries).
3) The [OntoDevice](https://github.com/cambridge-cares/TheWorldAvatar/tree/main/JPS_Ontology/ontology/ontodevice) triples are instantiated in Ontop.

### 1.1 Concurrency Design

The agent manages a phone ID to recording task map, where each phone ID will have a corresponding recording task. The recording task is responsible for sensor data processing, knowldge graph instantiation and postgres table initiation and data upload. Each recording task has different types of sensor processors, which are responsible for the sensor IRI query and generation, downsampling and data formulation for individual types. The following class diagram highlight the relations between class and omit some details of some classes for simlicity.

```mermaid
classDiagram
class SensorLoggerMobileAppAgent
SensorLoggerMobileAppAgent: -addDataExecutor
SensorLoggerMobileAppAgent: -sendDataExecutor
SensorLoggerMobileAppAgent: -processRequestQueue(payload)

class SmartphoneRecordingTask
SmartphoneRecordingTask: -tsClient
SmartphoneRecordingTask: -agentConfig
SmartphoneRecordingTask: -lastProcessedTime
SmartphoneRecordingTask: -lastActiveTime
SmartphoneRecordingTask: -isProcessing
SmartphoneRecordingTask: +addData(data)
SmartphoneRecordingTask: +shouldProcessData() bool
SmartphoneRecordingTask: +shouldTerminateTask() bool
SmartphoneRecordingTask: +processAndSendData()
SmartphoneRecordingTask: -initSensorProcessors()
SmartphoneRecordingTask: -bulkInitKg()
SmartphoneRecordingTask: -bulkInitRdb()
SmartphoneRecordingTask: -bulkAddTimeSeriesData()

class AgentConfig
class EndpointConfig
class OntoConstants
class StaticInstantiation

class SensorDataProcessor
SensorDataProcessor: -storeClient
SensorDataProcessor: -smartphoneIRINode
SensorDataProcessor: -timeList
SensorDataProcessor: -isIriInstantiationNeeded
SensorDataProcessor: -isIriInstantiationNeeded
SensorDataProcessor: +addData(data)
SensorDataProcessor: +getProcessedTimeSeries() TimeSeries<OffsetDateTime>
SensorDataProcessor: +initIRIs()
SensorDataProcessor: +getDataClass() List<Class>
SensorDataProcessor: +getDataIRIMap() Map<String, String>
SensorDataProcessor: +getTimeSeriesLength() int
SensorDataProcessor: clearData() 
SensorDataProcessor: getIrisFromKg()

class AccelerometerProcessor
AccelerometerProcessor: -xIri
AccelerometerProcessor: -yIri
AccelerometerProcessor: -zIri
AccelerometerProcessor: -xList
AccelerometerProcessor: -yList
AccelerometerProcessor: -zList

class DBFSDataProcessor
DBFSDataProcessor: -dbfsIRI
DBFSDataProcessor: -dBFSList

class GravityDataProcessor
GravityDataProcessor: -xIri
GravityDataProcessor: -yIri
GravityDataProcessor: -zIri
GravityDataProcessor: -xList
GravityDataProcessor: -yList
GravityDataProcessor: -zList

class IlluminationProcessor
IlluminationProcessor: -illuminationIri
IlluminationProcessor: -illuminationList

class RelativeBrightnessProcessor
RelativeBrightnessProcessor: -relativeBrightnessIRI
RelativeBrightnessProcessor: -brightnessList

class MagnetometerDataProcessor
MagnetometerDataProcessor: -xIri
MagnetometerDataProcessor: -yIri
MagnetometerDataProcessor: -zIri
MagnetometerDataProcessor: -xList
MagnetometerDataProcessor: -yList
MagnetometerDataProcessor: -zList


class LocationDataProcessor
LocationDataProcessor: -bearingIRI
LocationDataProcessor: -speedIRI
LocationDataProcessor: -altitudeIRI
LocationDataProcessor: -pointIRI
LocationDataProcessor: -bearingList
LocationDataProcessor: -speedList
LocationDataProcessor: -altitudeList
LocationDataProcessor: -geomLocationList

SensorLoggerMobileAppAgent *-- SmartphoneRecordingTask: 0..*
SensorLoggerMobileAppAgent -- AgentConfig: 1
SensorLoggerMobileAppAgent -- EndpointConfig: 1

SmartphoneRecordingTask *-- AccelerometerProcessor: 1
SmartphoneRecordingTask *-- DBFSDataProcessor: 1
SmartphoneRecordingTask *-- GravityDataProcessor: 1
SmartphoneRecordingTask *-- LocationDataProcessor: 1
SmartphoneRecordingTask *-- IlluminationProcessor: 1
SmartphoneRecordingTask *-- MagnetometerDataProcessor: 1
SmartphoneRecordingTask *-- RelativeBrightnessProcessor: 1
SmartphoneRecordingTask -- AgentConfig: 1

SensorDataProcessor <|-- AccelerometerProcessor
SensorDataProcessor <|-- DBFSDataProcessor
SensorDataProcessor <|-- GravityDataProcessor
SensorDataProcessor <|-- LocationDataProcessor
SensorDataProcessor <|-- IlluminationProcessor
SensorDataProcessor <|-- MagnetometerDataProcessor
SensorDataProcessor <|-- RelativeBrightnessProcessor
SensorDataProcessor -- AgentConfig: 1
```

Uploads use a durable disk queue and four background workers by default. The HTTP thread validates the JSON and, for `/update_for_timeline`, authenticates the bearer token. It writes the request and authenticated user ID to disk, forces the file to storage, and atomically publishes it before returning **202 Accepted**. Access tokens are not stored.

Workers check device ownership before decoding or adding sensor data. Each upload is processed as one bounded batch with a fresh recording task; only one worker processes a given device at a time. Different devices can proceed concurrently. The queue file is deleted only after the existing source/trajectory database transaction commits. A crash between commit and deletion can replay the upload; duplicates are accepted. Downsampling runs per uploaded batch rather than combining requests on a timer.

```mermaid
flowchart LR
  phone[Phone] --> validation[JSON validation and authentication]
  validation --> disk[Durable disk queue]
  disk --> response[202 Accepted]
  disk --> worker[Ownership check and processing]
  worker --> commit[Database transaction commit]
  commit --> remove[Delete queued file]
  worker --> retry[Retry or retain failure]
```

## 2. Pre-requisites

### 2.1 SensorLogger mobile application setup

The SensorLogger mobile application can downloaded either from [GooglePlay](https://play.google.com/store/apps/details?id=com.kelvin.sensorapp&hl=en&gl=US) or [IOS](https://apps.apple.com/us/app/sensor-logger/id1531582925), depending on the operating mobile OS. The steps to configure the endpoints in SensorLogger mobile app can be found [here](https://github.com/tszheichoi/awesome-sensor-logger#Live-Data-Streaming), which is summarized as below:

1) Enable HTTP PUSH under settings
2) Specify PUSH URL following `http://<LOCAL-URL>:3838/sensorloggermobileappagent/update`
3) Replace `<LOCAL-URL>` with the same network connected from both your local environment and your phone. `<LOCAL-URL>` can be obtained from the IPv4 Address under Wireless LAN adapter Wi-Fi of your server by running `ipconfig` on command prompt.

## 3. Agent Configuration

### Timeline authentication

The authenticated timeline endpoint requires these environment variables:

- `KEYCLOAK_SERVER`: base URL of the Keycloak server, including any context path.
- `KEYCLOAK_REALM`: realm that issues timeline access tokens.

`POST /sensorloggermobileappagent/update_for_timeline` accepts the same JSON body as
`/update` and additionally requires an access token:

```http
Authorization: Bearer <ACCESS_TOKEN>
```

Example:

```bash
curl -X POST \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data @payload.json \
  http://localhost:3838/sensorloggermobileappagent/update_for_timeline
```

#### Downsampling frequency

The downsampling method and frequency for the different measurements can be configured in [config.properties](sensorloggermobileappagent/src/main/resources/config.properties).

- `DSResolution` sets the time interval in seconds of the timeseries data to be downsampled.
- `DStype` refers to the downsampling method used which the enum types can be found [here](https://github.com/cambridge-cares/TheWorldAvatar/tree/main/core/downsampling#downsampling-type).

#### Persistent upload queue

`UPLOAD_QUEUE_WORKERS` sets the number of concurrent upload workers and the maximum database connection pool size. It defaults to `4` and can be overridden through an environment variable.

## 4. Deploy

### 4.1 Retrieving SensorLoggerMobileAppAgent's image

The SensorLoggerMobileAppAgent should be pulled automatically with the stack-manager, if not you can pull the latest version from [cambridge_cares package](https://github.com/orgs/cambridge-cares/packages/container/package/sensorloggermobileappagent) using `docker pull ghcr.io/cambridge-cares/sensorloggermobileappagent:<LATEST-VERSION>`

### 4.2 Starting with the stack-manager

The agent has been implemented to work in the stack, which requires the SensorLoggerMobileAppAgent Docker container to be deployed in the stack. To do so, place [sensorloggermobileappagent.json](stack-manager-config/inputs/config/services/sensorloggermobileappagent.json) in the [stack-manager config directory].

Then, run `./stack.sh start <STACK NAME>` in the [stack-manager] main folder. This will spin up the agent in the stack.

### 4.3 Configuring AccessAgent endpoints

On this same directory run, replace `STACK-NAME` with your stack-manager name.

```
./copy.sh start <STACK-NAME>
```

### 4.4 Start recording

Once all the configurations and server has been set, press the Start Recording button inside the SensorLogger mobile app to begin session.

## 5. Build and debug

## 5.1 Credentials

The docker image uses TheWorldAvatar maven repository (`https://maven.pkg.github.com/cambridge-cares/TheWorldAvatar/`).
You will need to provide your credentials (GitHub username/personal access token) in single-word text files as follows:

```
./credentials/
        repo_username.txt
        repo_password.txt
```

### 5.2 Building Docker Image

In the same directory as this README, run `./stack.sh build`. This will build the SensorLoggerMobileAppAgent local Docker Image.

### 5.2 Spinning up with stack-manager

To debug the agent, replace [`sensorloggermobileappagent-debug.json`](stack-manager-config/inputs/config/services/sensorloggermobileappagent-debug.json) instead of [`sensorloggermobileappagent.json`](stack-manager-config/inputs/config/services/sensorloggermobileappagent.json) in the [stack-manager config directory].

Spin up with `./stack.sh start <STACK NAME>` in the [stack-manager]'s main folder.
The debugger port will be available at 5005.

### 5.3 Testing resources

You may use the [SamplePOST request](sensorloggermobileappagent/src/main/resources/SamplePOST.http) for testing any changes made to the code, this HTTP request contains a sample of the recording for testing purposes.

[stack-manager]: https://github.com/cambridge-cares/TheWorldAvatar/tree/main/Deploy/stacks/dynamic/stack-manager
[stack-manager config directory]: https://github.com/cambridge-cares/TheWorldAvatar/tree/main/Deploy/stacks/dynamic/stack-manager/inputs/config/services

## Shared trajectory projection

The shared trajectory tables, `public.trajectory_points` and `public.trajectory_activities`, improve GeoServer layer efficiency for the Timeline app.
