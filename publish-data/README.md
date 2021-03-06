### publish-data

A service which uploads static content (aka data, cf metadata) from a Zebedee collection.

Messages are sent via a kafka topic. Example of messages:
```
{"collectionId":"test-0001","encryptionKey":"2iyOwMI3YF+fF+SDqMlD8Q==", "fileLocation":"/about/data.json"}
{"collectionId":"test0002","encryptionKey":"6y/+G0ZVPBBjtA5GOWj9Ow==", "fileLocation":"/peoplepopulationandcommunity/elections/electoralregistration/bulletins/electoralstatisticsforenglandwalesandnorthernireland/2015-02-26/1c560659.png"}
```

Example of a output message:
```
{"collectionId":"test0002","fileLocation":"/peoplepopulationandcommunity/elections/electoralregistration/bulletins/electoralstatisticsforenglandwalesandnorthernireland/2015-02-26/1c560659.png","s3Location":"s3://content/peoplepopulationandcommunity/elections/electoralregistration/bulletins/electoralstatisticsforenglandwalesandnorthernireland/2015-02-26/1c560659.png"}
```
### Getting started

#### Environment variables
* S3_URL defaults to localhost:4000
* S3_SECURE defaults to 1 (true) - use 0 (false) if not using secure connection
* S3_ACCESS_KEY defaults to 1234
* S3_SECRET_ACCESS_KEY defaults to 1234
* S3_BUCKET defaults to static_content

* UPSTREAM_S3_URL defaults to localhost:4000 (UPSTREAM_* items represent where zebedee puts encrypted collections)
* UPSTREAM_S3_SECURE defaults to 1 (true) - use 0 (false) if not using secure connection
* UPSTREAM_S3_ACCESS_KEY defaults to 1234
* UPSTREAM_S3_SECRET_ACCESS_KEY defaults to 1234
* UPSTREAM_S3_BUCKET defaults to `upstream-content`

* KAFKA_ADDR defaults to localhost:9092
* ZEBEDEE_ROOT defaults to ../test-data/
* CONSUME_TOPIC defaults to uk.gov.ons.dp.web.publish-file
* PRODUCE_TOPIC defaults to uk.gov.ons.dp.web.complete-file

* `HEALTHCHECK_ADDR` defaults to ':8080'
* `HEALTHCHECK_ENDPOINT` defaults to '/healthcheck'

#### Running a test environment
* Install ruby using ```brew install ruby```
* Install fake s3 server ```gem install fakes3```
* Start fake s3 server ```mkdir -p /tmp/s3  && fakes3 -r /tmp/s3 -p 4000```

### Notes
