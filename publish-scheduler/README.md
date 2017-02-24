### Publish-scheduler

A service which schedules and initiates release of Zebedee collections.

Schedule messages are received via a kafka topic (see below). Examples of inbound messages:
```
{"CollectionId":"test 0002","CollectionPath":"test0002", "EncryptionKey":"6y/+G0ZVPBBjtA5GOWj9Ow==", "ScheduleTime":"1234567890"}
```

Example of an output 'publish-file' message:
```
{"ScheduleId":33, "FileId":1234, "CollectionId":"test 0002", "CollectionPath":"test0002", "EncryptionKey":"6y/+G0ZVPBBjtA5GOWj9Ow==", "FileLocation":"/peoplepopulationandcommunity/2015-02-26/1c560659.png"}
```

### Getting started

#### Environment variables

* `KAFKA_ADDR` defaults to "localhost:9092"
* `ZEBEDEE_ROOT` defaults to "../test-data/
* `SCHEDULE_TOPIC` defaults to "uk.gov.ons.dp.web.schedule"
* `PUBLISH_COUNT_TOPIC` defaults to "uk.gov.ons.dp.web.publish-count"
* `PUBLISH_FILE_TOPIC` defaults to "uk.gov.ons.dp.web.publish-file"
* `COMPLETE_TOPIC` defaults to "uk.gov.ons.dp.web.complete"
* `DB_ACCESS` defaults to "user=dp dbname=dp sslmode=disable"
* `RESEND_AFTER_QUIET_SECONDS` defaults to 0 (seconds)
  * if no files have been marked as complete in the last RESEND_AFTER_QUIET_SECONDS, a scheduled job will be resumed (i.e. incomplete files resent)
  * resends are disable when the value is 0

#### Installation

Install and setup Postgresql:
```
 brew install postgresql
 createuser --pwprompt dp
 createdb -Odp dp
 # create table in DB...
 psql -U dp dp -f doc/init.sql
```

On Ubuntu, you may have to give the ubuntu user access to this DB:
Add the line `local   dp              dp                                      trust` to the file `/etc/postgresql/9.5/main/pg_hba.conf` and restart.


#### Running a test environment

* `make scheduler`
* Feed the scheduler by running a kafka producer as follows:

  ```
  kafka-console-producer --broker-list $KAFKA_ADDR --topic $SCHEDULE_TOPIC
  ```

  and pasting in the inbound message shown above.
