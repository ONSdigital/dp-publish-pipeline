## Publishing

### Zebedee

**Publish** to topic:
 - "uk.gov.ons.dp.web.schedule"
  ```
  action: "publish|cancel",
  collectionId: "<string>",
  publishTime: <epoch>,
  encryptionKey: "<string>",
  ```
  - `publishTime` may be optional if `action` is `cancel` (i.e. do not publish)
  - `action:"cancel"` is not yet supported

### Publish-scheduler

**Consume** topic "uk.gov.ons.dp.web.schedule"

Store schedule in DB for publication. (Or mark collection as cancelled, if `action` is `cancel`.)

Then, at appropriate time...

**Publish** to
 - topic "uk.gov.ons.dp.web.publish-file":
  ```
  collectionId: "<string>",
  encryptionKey: "<string>",
  fileLocation: "<string>",
  ```
 - topic "uk.gov.ons.dp.web.publish-count"
 ```
 collectionId: "<string>",
 fileCount: count,
 ```

**Consume** topic "uk.gov.ons.dp.web.complete"
 - Update schedule as complete when this message is received

### Publish-sender

**Consume** topic "uk.gov.ons.dp.web.publish-file"

**Publish**
 - to topic "uk.gov.ons.dp.web.complete-file":
  ```
  collectionId: "<string>",
  fileLocation: "<string>",
  fileContent: "<data.json>",
  ```
 - to topic "uk.gov.ons.dp.web.complete-file-flag"
 ```
 collectionId: "<string>",
 fileLocation: "<string>",
 ```

### Static-content-migrator

**Consume** topic "uk.gov.ons.dp.web.publish-file"

**Output**
 - static content is decrypted and placed in an S3 bucket
 - to topic "uk.gov.ons.dp.web.complete-file":
  ```
  collectionId: "<string>",
  fileLocation: "<string>",
  s3Location: "<data.json>",
  ```
 - publish to topic "uk.gov.ons.dp.web.complete-file-flag"
   (cf _Publish-sender_ above):

### Publish-tracker

**Consume** topics:
- "uk.gov.ons.dp.web.publish-count"
- "uk.gov.ons.dp.web.complete-file-flag"

**Output**
- "uk.gov.ons.dp.web.complete"
```
collectionId: "<string>",
```

---

## Web

### publish-receiver

**Consume** topic "uk.gov.ons.dp.web.complete-file"

**Output** Content is written to database (metadata or s3URL)
