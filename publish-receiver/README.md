### Publish receiver

This service receives JSON messages containing new published pages for
the ONS website (either metadata or s3Locations).

Test data examples for 'uk.gov.ons.dp.web.complete-file' topic
```
{ "collectionId":"test-0001", "fileLocation": "/about/data.json", "fileContent": "1234353453" }
{ "collectionId":"test-0001", "fileLocation": "/releases/newpage/data.json", "fileContent": "1234353453" }
{ "collectionId":"test-0001", "fileLocation": "/releases/newpage/stats.xls", "s3Location": "s3/path/stats.xls" }


{ "collectionId":"test-0002", "fileLocation": "/about/data.json", "fileContent": "0000000000" }
{ "collectionId":"test-0002", "fileLocation": "/releases/other/data.json", "fileContent": "1234353453" }
{ "collectionId":"test-0002", "fileLocation": "/releases/other/stats.xls", "s3Location": "s3/path/stats.xls" }
```

Test data examples for 'uk.gov.ons.dp.web.complete' topic
```
{ "collectionId":"test-0001" }
{ "collectionId":"test-0002" }
```

#### Environment variables
* `zebedee_root` defaults to "."
* `DB_ACCESS` defaults to "user=dp dbname=dp sslmode=disable"
* `FILE_COMPLETE_TOPIC` defaults to "uk.gov.ons.dp.web.complete-file"
* `KAFKA_ADDR` defaults to "localhost:9092"
* `MAX_CONCURRENT_FILE_COMPLETES` (default: 40) limit concurrent file-complete messages in progress

* `HEALTHCHECK_ADDR` defaults to ':8080'
* `HEALTHCHECK_ENDPOINT` defaults to '/healthcheck'

#### Running a test environment

Need to run these, only once:
* Install kafka server ```brew install kafka```
* install postgres server ```brew install postgres```
* Run the following SQL script at `doc/init.sql`
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```
