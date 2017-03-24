## Deployment on AWS with existing services

### Subnets

#### Publishing private A
##### Kafka instance
Instance type : M4.Large
Description: Used to run the kafka broker
Notes: ACLs where updated to allow services from different subnets to connect

##### Track + Schedular
Instance type : M4.Large
Description : Used to run publish-track and publish-scheduler
Notes :
      * This instance contained a local copy of the collection being published
      * ACLs where updated to allow services to connect to a RDS

##### Meta + Data
Instance type : C4.2xlarge
Description : Used to decrypt to collection data
Notes :
      * This instance contained a local copy of the collection being published

#### Web private A
##### Receiver + Search Indexer
Instance type : C4.large
Description : Used to consumer decrypted messages and update a RDS and elastic search
Notes :
      * Performance here is reflected base on what type of RDS is setup
      * ACLS where updated for the web RDS

#### AWS RDS
##### Web RDS
Instance type : db.m4.xlarge
Description : A database used to store json content from zebedee
Notes :
      * By default a db.m3.medium does not provide EBS optimisations, so we upgrade to different box.
      * 100 GB of storage was given to the database, as having less causes higher latencies. See http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_Storage.html#Concepts.Storage.GeneralSSD

##### Track + schedule RDS
Instance type : db.m3.medium
Description : A database used to store scheduled collections to publish.

### Additional notes
All new instances had NTP configured. So logs gave the correct times.
