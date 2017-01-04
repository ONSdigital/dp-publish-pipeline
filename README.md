dp-publish-pipeline
================

### Getting started
See the following pages per service
* [Publish-scheduler](publish-scheduler/README.md)
* [Publish-tracker](publish-tracker/README.md)
* [Publish-sender](publish-sender/README.md)
* [Static-content-migrator](static-content-migrator/README.md)
* [Publish-receiver](publish-receiver/README.md)

### Design
![alt Design](doc/design.png)

### Event messages
See [Event Message](doc/Messages.md) for details on each topic and type of message sent

### Test results for publishing
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

### Issues
* Encrytion key between the zebedee and publish scheduler is exposed in the kafka topic. In other topics the encryption key is no longer important as its time to publish the collection to the website.
*  Race condition when a collection is added to mongodb. Its possible someone may get a missing page because of missing resources during the publish time.
* Current services do not take advantage of kafka groups.
* Publish tracker and Publish scheduler are stateful and do not currently store it.

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2016, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
