### Publish search indexer

This service receives JSON messages containing new published pages for
the ONS website.

### Configuration

| Environment variable | Default                                        | Description
| -------------------- | ---------------------------------------------- | ----------------------------------------------------
| KAFKA_ADDR           | http://localhost:9092                          | The Kafka broker addresses comma separated
| KAFKA_CONSUMER_GROUP | uk.gov.ons.dp.web.complete-file.search-index   | The Kafka consumer group to consume messages from
| FILE_COMPLETE_TOPIC  | uk.gov.ons.dp.web.complete-file                | The Kafka topic to consume messages from
| ELASTIC_SEARCH_NODES | http://127.0.0.1:9200                          | The Elastic Search node addresses comma separated
| ELASTIC_SEARCH_INDEX | ons                                            | The Elastic Search index to update
| HEALTHCHECK_ADDR     | :8080                                          | The HTTP listen address for the healthcheck endpoint
| HEALTHCHECK_ENDPOINT | /healthcheck                                   | The HTTP endpoint for the healthcheck response

#### Running a test environment

##### Elastic search install via brew
* Install Elastic Search `brew install elasticsearch@5.2`
* Ensure the cluser.name property is set to `cluster.name: elasticsearch`.
The configuration file can be found at `/usr/local/etc/elasticsearch/elasticsearch.yml`. For some reason it appended my username onto the end of the default clustername.
* Start Elastic Search service `brew services start elasticsearch@5.2`
* Run it `elasticsearch`

##### Configure Elastic search
* To create the ONS and department indexes run ```./createIndex.sh```

##### Elastic search via dp-compose
The dp-compose project requires the native docker for mac (not docker toolbox)
```
git clone git@github.com:ONSdigital/dp-compose.git
cd dp-compose
./run.sh
```

#### Create / Delete elastic index
* Run ./createIndex.sh to create the elastic index
* Run ./deleteIndex.sh to create the elastic index

##### Run the search indexer
* Run the search indexer `make search-indexer`


##### Run a test Kafka producer and send a test message

`kafka-console-producer --broker-list localhost:9092 --topic uk.gov.ons.dp.web.complete-file`

`{"fileId":0,"scheduleId":0,"collectionId":"test-0001","Uri":"/about/data.json","fileContent":"{\"uri\":\"/about\",\"type\":\"static_page\"}"}`

##### Check that the page is updated in Elastic Search

curl / browse http://localhost:9200/ons/static_page/_search

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2016, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
