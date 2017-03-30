### Publish-scheduler

A service which schedules and initiates release of Zebedee collections. Encryption keys for collections
are all stored in vault.

Schedule messages are received via a kafka topic (see below). Example of inbound messages:
```
{"CollectionId":"test 0002","CollectionPath":"test0002", "ScheduleTime":"1234567890",
  "Files":[{"Uri":"/pop/foo.json","Location":"s3://bucket/test0002/pop/foo.json"},...],
  "UrisToDelete":["/pop/bar.json"]
}
```

Example of an output 'publish-file' message:
```
{"ScheduleId":33, "FileId":1234, "CollectionId":"test 0002", "CollectionPath":"test0002", "EncryptionKey":"6y/+G0ZVPBBjtA5GOWj9Ow==", "FileLocation":"s3://bucket/test0002/peoplepopulationandcommunity/2015-02-26/1c560659.png"}
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
* `VAULT_ADDR` defaults to "http://127.0.0.1:8200"
* `VAULT_TOKEN` defaults to ""
* `VAULT_RENEW_TIME` defaults to 5 (Time in minutes)

#### Installation

Install and setup Postgresql:
```
 brew install postgresql
 createuser --pwprompt dp
 createdb -Odp dp
 grant dp to postgres;     # in psql on AWS
 # create table in DB...
 psql -U dp dp -f doc/init.sql
```

On Ubuntu, you may have to give the ubuntu user access to this DB:
Add the line `local   dp              dp                                      trust` to the file `/etc/postgresql/9.5/main/pg_hba.conf` and restart.

Installing and setup Vault:
```
brew install vault
# Run vault in a new console
vault server -dev
# Configure vault with accounts
cd scripts/
./dp-vault -i
# Generate tokens
./dp-vault -t zebedee-cms
./dp-vault -t scheduler
```

#### Running a test environment

* `make scheduler`
* Feed the scheduler by running a kafka producer as follows:

  ```
  kafka-console-producer --broker-list $KAFKA_ADDR --topic $SCHEDULE_TOPIC
  ```

  and pasting in the inbound message shown above.
