# Self-managed Kafka Connect

## Pipeline Components
* **Source:** Oracle Database
* **Ingestion:** Oracle CDC Source Connector *(captures row-level changes)*
* **Streaming Platform:** Confluent Platform (`cp-all-in-one`) *(Kafka + Schema Registry + Customized Connect container)*
* **Egress:** JDBC Sink Connector *(writes topic events to target tables)*
* **Sink:** Microsoft SQL Server

## Configuration

### Oracle Database prequisites
* Turn on ARCHIVELOG mode
* Enable supplemental logging
```
ALTER SESSION SET CONTAINER=cdb$root;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### Connect Service in Docker Compose
```
  connect-1:
    # image: confluentinc/cp-server-connect:7.1.1
    image: confluentinc/cp-server-connect:8.1.1
    restart: always
    hostname: connect-1
    container_name: connect-1
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8083:8083"
    volumes:
      - ./data:/data
    environment:
      CONNECT_BOOTSTRAP_SERVERS: "broker:29092"
      CONNECT_GROUP_ID: "kc101-connect"
      CONNECT_CONFIG_STORAGE_TOPIC: "_kc101-connect-configs"
      CONNECT_OFFSET_STORAGE_TOPIC: "_kc101-connect-offsets"
      CONNECT_STATUS_STORAGE_TOPIC: "_kc101-connect-status"
      CONNECT_REPLICATION_FACTOR: 1
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: "org.apache.kafka.connect.storage.StringConverter"

      # Confluent Schema Registry for Kafka Connect
      CONNECT_VALUE_CONVERTER: "io.confluent.connect.avro.AvroConverter"
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "true"
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"

      CONNECT_REST_ADVERTISED_HOST_NAME: "connect-1"
      CONNECT_LISTENERS: "http://0.0.0.0:8083"
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components"
      CONNECT_LOG4J_APPENDER_STDOUT_LAYOUT_CONVERSIONPATTERN: "[%d] %p %X{connector.context}%m (%c:%L)%n"
      CONNECT_LOG4J_ROOT_LOGLEVEL: INFO
      CONNECT_LOG4J_LOGGERS: 'org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR'

      # Confluent Cloud config
      CONNECT_REQUEST_TIMEOUT_MS: "20000"
      CONNECT_RETRY_BACKOFF_MS: "500"

      # Connect worker
      CONNECT_SECURITY_PROTOCOL: PLAINTEXT

      # Connect producer
      CONNECT_PRODUCER_SECURITY_PROTOCOL: PLAINTEXT

      # Connect consumer
      CONNECT_CONSUMER_SECURITY_PROTOCOL: PLAINTEXT
    
    command:
      - bash
      - -c
      - |
        echo "Installing Connector"

        confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.8.2
        confluent-hub install --no-prompt confluentinc/kafka-connect-oracle-cdc:latest

        echo "Launching Kafka Connect worker"
        /etc/confluent/docker/run &
        #
        sleep infinity
```

### Oracle CDC Source Connector
```
curl -i -X PUT -H  "Content-Type:application/json"     http://localhost:8083/connectors/cdc-source-oracle-pdb/config     -d '{
    "connector.class": "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector",
    "tasks.max": "2",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "oracle.server": "oracle-free",
    "oracle.port": "1521",
    "oracle.sid": "FREE",
    "oracle.pdb.name": "FREEPDB1",
    "oracle.username": "SYSTEM",
    "oracle.password": "mypassword",

    "predicates": "IsTableTopic",
    "predicates.IsTableTopic.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsTableTopic.pattern": "^cdc$",

    "transforms": "filterByAddress",
    "transforms.filterByAddress.type": "io.confluent.connect.transforms.Filter$Value",
    "transforms.filterByAddress.filter.condition": "$[?(@.DELIVERY_ADDRESS =~ /.*plaza.*/i)]",
    "transforms.filterByAddress.filter.type": "include",
    "transforms.filterByAddress.missing.or.null.behavior": "exclude",
    "transforms.filterByAddress.predicate": "IsTableTopic",

    "redo.log.consumer.bootstrap.servers": "broker:29092",
    "redo.log.topic.name": "redo",

    "table.inclusion.regex": "FREEPDB1[.]SYSTEM[.]ORDERS.*",
    "table.topic.name.template": "cdc",

    "numeric.mapping": "precision_only",
    "numeric.default.scale": "0"
  }'
  ```
### JDBC Sink Connector
```
curl -i -X PUT -H  "Content-Type:application/json" \
    http://localhost:8083/connectors/mssql-sink/config \
-d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "cdc",
    "connection.url": "jdbc:sqlserver://sqlserver:1433;databaseName=master;encrypt=true;trustServerCertificate=true",
    "connection.user": "sa",
    "connection.password": "MyStr0ng!Pass123",
    "table.name.format": "ORDERS",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "ID",
    "delete.enabled": "false",
    "auto.create": "true"
  }'

```
