### Publish receiver

This service receives JSON messages containing new published pages for
the ONS website (either metadata or s3Locations).

Test data examples
```
{ "collectionId":"test-0001", "fileLocation": "/about/data.json", "fileContent":"1234353453"}
{ "collectionId":"test-0002", "fileLocation": "/releases/newpage/data.json", "fileContent":"1234353453"}
{ "collectionId":"test-0002", "fileLocation": "/releases/newpage/stats.xls", "s3Location":"s3/path/stats.xls"}
```

#### Environment variables
* `zebedee_root` defaults to "."
* `TOPIC` defaults to "test"
* `KAFKA_ADDR` defaults to "localhost:9092"

#### Running a test environment

Need to run these, only once:
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```
