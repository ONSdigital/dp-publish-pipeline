### Publish tracker

A service used to track the release of a collection and to publish the collection
within the zebedee CMS.

Example of messages
```
{"collectionId":"test-0001", "fileCount": 1 }

{"collectionId":"test-0001", "fileLocation":"about/data.json"}
```

#### Environment variables
* `zebedee_root` defaults to "."
* `PUBLISH_COUNT_TOPIC` defaults to "uk.gov.ons.dp.web.publish-count"
* `COMPETE_FILE_TOPIC` defaults to "uk.gov.ons.dp.web.complete-file"
* `KAFKA_ADDR` defaults to "localhost:9092"

#### Running a test environment
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```
