### Publish-metadata

Decrypts metadata (JSON) and pushes the data off to a publish receiver

#### Environment variables
* CONSUME_TOPIC defaults to "uk.gov.ons.dp.web.publish-file"
* PRODUCE_TOPIC defaults to "uk.gov.ons.dp.web.complete-file"
* KAFKA_ADDR defaults to "localhost:9092"
* ZEBEDEE_ROOT defaults to "../test-data/"

#### Running a test environment
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```

#### Typical config changes

To increase the message size allowed for the $PRODUCE_TOPIC:

set env. var. `KAFKA_MAX_BYTES=10000000` for this service, and also run (once):

`kafka-configs --zookeeper $ZOOKEEPER --entity-type topics --entity-name $PRODUCE_TOPIC --alter --add-config max.message.bytes=$KAFKA_MAX_BYTES`