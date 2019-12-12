# Kafka CSV File Sink Connector

The CSVFile-Sink-Connector is a Kafka-Connector for testing and development (only) which will dump data from kafka topics straight to CSV files, in a way that is (hopefully) compatible with the CSV files that RADAR generates from the "real" HFDS files.
 
Currently, it is a work in progress.

## Installation

This connector can be used inside a Docker stack or installed as a general Kafka Connect plugin.

### Docker installation

Build using the local [Dockerfile](Dockerfile).
The Kafka Connect Docker image requires environment to be set up. In RADAR-Dockeri (non-replicated), the following environment variables are set:

```yaml
environment:
  CONNECT_BOOTSTRAP_SERVERS: PLAINTEXT://kafka-1:9092
  CONNECT_REST_PORT: 8083
  CONNECT_GROUP_ID: "csvfile-sink"
  CONNECT_CONFIG_STORAGE_TOPIC: "csvfile-sink.config"
  CONNECT_OFFSET_STORAGE_TOPIC: "csvfile-sink.offsets"
  CONNECT_STATUS_STORAGE_TOPIC: "csvfile-sink.status"
  CONNECT_KEY_CONVERTER: "io.confluent.connect.avro.AvroConverter"
  CONNECT_VALUE_CONVERTER: "io.confluent.connect.avro.AvroConverter"
  CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry-1:8081"
  CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry-1:8081"
  CONNECT_INTERNAL_KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
  CONNECT_INTERNAL_VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
  CONNECT_OFFSET_STORAGE_FILE_FILENAME: "/tmp/csvfile-sink.offset"
  CONNECT_REST_ADVERTISED_HOST_NAME: "radar-csvfile-connector"
  CONNECT_ZOOKEEPER_CONNECT: zookeeper-1:2181
  CONNECT_LOG4J_LOGGERS: "org.reflections=ERROR"
  KAFKA_BROKERS: 1
```

Before starting the streams, the Docker image waits for `KAFKA_BROKERS` number of brokers to be available as well as the schema registry.

## Usage

Modify [sink.properties](sink.properties) file according your environment. The following properties are supported:

<table class="data-table"><tbody>
<tr>
<th>Name</th>
<th>Description</th>
<th>Type</th>
<th>Default</th>
<th>Valid Values</th>
<th>Importance</th>
</tr>
<tr>
<td>topics</td><td>List of topics to be streamed.</td><td>list</td><td></td><td></td><td>high</td></tr>
</tbody></table>

- A sample configuration may look as below.

    ```ini    
    # Kafka consumer configuration
    name=kafka-connector-csvfile-sink
    
    # Kafka connector configuration
    connector.class=org.radarcns.connect.csvfile.CsvFileSinkConnector
    tasks.max=1
    
    # Topics that will be consumed
    topics=avrotest
    ```
    
- Run the CsvFile-Sink-Connector in 
  - `standalone` mode
  
      ```shell
      connect-standalone /etc/schema-registry/connect-avro-standalone.properties ./sink.properties 
      ```
  - `distributed` mode
  
      ```shell
      connect-distributed /patht/cluster.properties ./sink.properties
      ```
- Stream sample data to configured `topics` in `sink.properties`. You may use, [rest-proxy](http://docs.confluent.io/4.0.0/kafka-rest/docs/intro.html#produce-and-consume-avro-messages) to do this easily.

    ```shell
    curl -X POST -H "Content-Type: application/vnd.kafka.avro.v2+json" \
          -H "Accept: application/vnd.kafka.v2+json" \
          --data '{"value_schema": "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "testUser"}}]}' \
          "http://localhost:8082/topics/avrotest"
          
    
    # You should get the following response:
      {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":21}

    ```  
- Hopefully the data will appear somewhere in the filesystem :-)
- To stop your connector press `CTRL-C`

## Developer guide 

#### Notes

Connectors can be run inside any machine where Kafka has been installed. Therefore, you can fire them also inside a machine that does not host a Kafka broker.

To reset a connector running in `standalone mode` you have to stop it and then modify `name` and `offset.storage.file.filename` respectively inside `sink.properties` and `standalone.properties`

## Contributing

All of the contribution code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).
