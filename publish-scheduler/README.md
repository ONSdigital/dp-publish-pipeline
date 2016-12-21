### Publish-scheduler

A service which schedules and initiates Zebedee collections.

Messages are received via a kafka topic. Examples of inbound messages:
```
{"CollectionId":"test0002","EncryptionKey":"6y/+G0ZVPBBjtA5GOWj9Ow==","ScheduleTime":"1234567890"}
```

Example of an output message:
```
{"CollectionId":"test0002","FileLocation":"/peoplepopulationandcommunity/2015-02-26/1c560659.png","s3Location":"s3://content/peoplepopulationandcommunity/2015-02-26/1c560659.png"}
```

### Getting started

#### Environment variables

* KAFKA_ADDR defaults to localhost:9092
* ZEBEDEE_ROOT defaults to ../test-data/
* SCHEDULE_TOPIC defaults to uk.gov.ons.dp.web.schedule
* PUBLISH_COUNT_TOPIC defaults to uk.gov.ons.dp.web.publish-count
* PUBLISH_FILE_TOPIC defaults to uk.gov.ons.dp.web.publish-file

#### Running a test environment

* `make scheduler`
* Feed the scheduler by running a kafka producer as follows:

  ```
  kafka-console-producer --broker-list $KAFKA_ADDR --topic $SCHEDULE_TOPIC
  ```

  and pasting in the inbound message shown above.
