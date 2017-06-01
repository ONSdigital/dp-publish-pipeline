dp-publish-pipeline
================

### Getting started
See the following pages per service
* [Publish-scheduler](publish-scheduler/README.md)
* [Publish-tracker](publish-tracker/README.md)
* [Publish-metadata](publish-metadata/README.md)
* [Publish-data](publish-data/README.md)
* [Publish-receiver](publish-receiver/README.md)
* [Publish-search-indexer](Publish-search-indexer/README.md)

### External APIs (see also)
* [Content-API](../dp-content-api/README.md)
* [Generator-API](../dp-generator-api/README.md)
* [Search-Query API](../dp-search-query/README.md)

### Design
![alt Design](doc/design.png)

### Event messages
See [Event Message](doc/Messages.md) for details on each topic and type of message sent

### Deployment using nomad
Before creating the nomad plans the following env variables need exporting.

* ```KAFKA_ADDR``` A list of kafka brokers (separated by ",")
* ```VAULT_ADDR``` A URL to vault server. Eg. http://127.0.0.1:8200
* ```S3_URL``` For AWS set this to "s3.amazonaws.com" else for a minio cluster "host:port". For storing web content.
* ```S3_BUCKET``` The bucket name for web content
* ```UPSTREAM_S3_URL``` For AWS set this to "s3.amazonaws.com" else for a minio cluster "host:port". For storing collections to be published.
* ```UPSTREAM_S3_BUCKET``` The bucket name to store collections to be Released
* ```PUBLISH_DB_ACCESS``` A postgres database URL used for publishing collections
* ```WEB_DB_ACCESS``` A postgres database URL used for storing released content
* ```SCHEDULER_VAULT_TOKEN``` A token used to access vault for encryption keys
* ```ELASTIC_SEARCH_URL``` A URL to a elastic search cluster
* ```S3_TAR_FILE``` A S3 location containing a tar file with all the publishing binaries built

Once all env variables have been exported run ```make nomad```. This shall generate 7 nomad
plans for the publish pipeline services.

#### Running a test environment (typically on macOS, common to all services)
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```
* Install GNU sed `brew install gnu-sed` (installs `gsed` - used by `Makefile`)

### Latest test results (2017 February)
Machines: AWS M4.large, M3.xlarge, M4.large

#### AWS setup
* M4.large : Kafka + postgres (For publish-scheduler)
* M4.large : 32 publish-data + 32 publish-metadata
* M3.xlarge : publish-receiver + postgres/mongodb

#### Results
| File count | Size   |  Time in Seconds   |
|------------|--------|--------------------|
| 16k         | 780MB  |  21 - 22          |

### Test results for publishing (2016 December results)
Machine: AWS M4.Large instance 2VCPU and 8GB Memory

| File count | Size   |  Time in Seconds   |  IO mbs  | Network mbps |
|------------|--------|--------------------|----------|--------------|
| 8k         | 256MB  |  27                |  23      |  452         |
| 16K        | 512MB  |  64                |  26      |  436         |

#### Notes
* Ran on a single AWS instance, all 5 services plus docker, mongo, kafka and zookeeper were ran.
* The biggest bottle neck during the tests was the mongod process, as it was using 70% / 90% of the CPU.
* Read / Writes During monitoring no disk I/O bottle necks were seen
* Network max usage 436 mbp out of 450 mbps. This only lasted for a second.
* Both CPUs were at 100% utilisation when publishing the collection.
* Ram usage was low 2-3 GB (Including cache)

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2016, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
