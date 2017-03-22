## Publish deleter
A publish service used to remove content from the website.

### Messages

#### Consumers
Consumes the following messages from a kafka topic.
```
{
  "scheduleId"      : <Number> (Schedule id for this collection)
  "deleteId"        : <Number> (Delete Id for this item)
  "collectionId"    : <String> (Collection id)
  "deleteLocation"  : <String> (Path to delete)
}
```

#### Producers
Produces the following message when a file has been successfully removed.
```
{
  "scheduleId"      : <Number> (Schedule id for this collection)
  "deleteId"        : <Number> (Delete Id for this item)
  "collectionId"    : <String> (Collection id)
  "deleteLocation"  : <String> (Path to delete)
}
```

### Environment variables

* `KAFKA_ADDR` defaults to "localhost:9092"
* `DELETE_TOPIC` defaults to "uk.gov.ons.dp.web.delete-file"
* `PUBLISH_DELETE_TOPIC` defaults to "uk.gov.ons.dp.web.complete-file-flag""
* `DB_ACCESS` defaults to "user=dp dbname=dp sslmode=disable"

### Race conditions
As ordering is not guaranteed, it is possible insert then delete or deci versa to happen.
This can lead to lost content. But the mechanising in place for Zebedee stops this race
condition on happening. Once content has been edited, it is impossible to create
another collection which modifies the same file. Deleting content is treated in
the same as edited content. So its impossible for a insert and delete to happen on
multiple collections being published.

But there are still one scenarios this can happen when a number of collections are
published directory into pipeline without Zebedee.

To keep the implementation simple and that Zebedee should only be the interface into
pipeline. The publish deleter and publish receiver are going to uses the Zebedee file
lock to guarantee content is correctly deleted and inserted.
