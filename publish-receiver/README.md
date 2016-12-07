### Publish receiver

A service which receivers json messages containing new published pages for  
the ONS website. Each page is stored in a mongodb collection

Test data examples
```
{ "fileLocation": "/about/data.json", "FileContent":"1234353453"}
{ "fileLocation": "/releases/newpage/data.json", "FileContent":"1234353453"}
```
#### Environment variables
* MONGODB defaults to localhost
* TOPIC defaults to "test"
* KAFKA_ADDR defaults to "localhost:9092"

#### Running a test environment
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```
* Install docker
* Run ```docker run --name website-mongo -p 27017:27017 mongo --storageEngine wiredTiger```
* To inspect mongo use ```
```
