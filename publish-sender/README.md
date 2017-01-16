### Publish sender

Decrypts metadata and pushes the data off to a publish receiver

#### Environment variables
* CONSUME_TOPIC defaults to "test"
* PRODUCE_TOPIC defaults to "test"
* KAFKA_ADDR defaults to "localhost:9092"
* ZEBEDEE_ROOT defaults to ../test-data/

#### Running a test environment
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```

#### Typical config change
To increase the message size for the _complete-file_ topic:

`kafka-configs --zookeeper $ZOOKEEPER --entity-type topics --entity-name uk.gov.ons.dp.web.complete-file --alter --add-config max.message.bytes=10000000`
