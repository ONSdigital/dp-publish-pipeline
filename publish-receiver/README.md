### Publish receiver

A service which receivers json messages containing new published pages for  
the ONS website.

Test data examples
```
{ "fileLocation": "/about/data.json", "FileContent":"1234353453"}
{ "fileLocation": "/releases/newpage/data.json", "FileContent":"1234353453"}
```
#### Environment variables
* ZEBEDEE_ROOT defaults to "."
* TOPIC defaults to "test"
* KAFKA_ADDR defaults to "localhost:9092"

#### Running a test environment
* Install kafka server ```brew install kafka```
* Start zookeeper and kafka ```brew services start zookeeper && brew services start kafka```
