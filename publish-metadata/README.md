### Publish-metadata

Decrypts metadata (JSON) and pushes the data off to a publish receiver

#### Environment variables
* CONSUME_TOPIC defaults to "uk.gov.ons.dp.web.publish-file"
* PRODUCE_TOPIC defaults to "uk.gov.ons.dp.web.complete-file"
* KAFKA_ADDR defaults to "localhost:9092"
* ZEBEDEE_ROOT defaults to "../test-data/"

* `HEALTHCHECK_ADDR` defaults to ':8080'
* `HEALTHCHECK_ENDPOINT` defaults to '/healthcheck'

* see [Publish-data](publish-data/README.md) for the list of S3 env vars

#### Typical config changes

To increase the message size allowed for the $PRODUCE_TOPIC:

set env. var. `KAFKA_MAX_BYTES=10000000` for this service, and also run (once):

`kafka-configs --zookeeper $ZOOKEEPER --entity-type topics --entity-name $PRODUCE_TOPIC --alter --add-config max.message.bytes=$KAFKA_MAX_BYTES`
