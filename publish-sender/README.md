### Publish sender

Decrypts collections and pushs the data off to a publish receiver

#### Environment variables
* CONSUME_TOPIC defaults to "test"
* PRODUCE_TOPIC defaults to "test"
* KAFKA_ADDR defaults to "localhost:9092"
* ZEBEDEE_ROOT defaults to ../test-data/

#### Running a test environment
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```