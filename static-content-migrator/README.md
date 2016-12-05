### static-content-migrator

A service which uploads static content from a Zebedee collection.

Messages are sent via a kafka topic. Example of messages:
```
{"collectionId":"test-0001","encryptionKey":"2iyOwMI3YF+fF+SDqMlD8Q=="}
{"collectionId":"test0002","encryptionKey":"6y/+G0ZVPBBjtA5GOWj9Ow=="}
```

### Getting started

#### Environment variables
* S3_URL defaults to localhost:4000
* S3_ACCESS_KEY defaults to 1234
* S3_SECRET_ACCESS_KEY defaults to 1234
* KAFKA_ADDR defaults to localhost:9092
* S3_BUCKET defaults to static_content
* ZEBEDEE_ROOT defaults to ../test-data/

#### Running a test environment
* Install ruby using ```brew install ruby```
* Install fake s3 server ```gem install fakes3```
* Start fake s3 server ```mkdir -p /tmp/s3  && fakes3 -r /tmp/s3 -p 4000```
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```

### Notes
