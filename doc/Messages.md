## Publishing

### Zebedee

**Publish** to topic:
 - "uk.gov.ons.dp.web.schedule"
  ```
  collectionId: "<string>",
  encryptionKey: "<string>",
  action: "publish|cancel",
  scheduleTime: <epoch>,
  files:[{uri:"<string>", location:"<string>"}, ...],
  urisToDelete:["<string>", ...],
  ```
  - `scheduleTime` may be optional if `action` is `cancel` (i.e. do not publish)
  - `action:"cancel"` is not yet supported

### Publish-scheduler

**Consume** topic "uk.gov.ons.dp.web.schedule"

Store schedule in DB for publication. (Or mark collection as cancelled, if `action` is `cancel`.)

Then, at appropriate time...

**Publish** to
 - topic "uk.gov.ons.dp.web.publish-file":
  ```
  fileId: <integer>,
  scheduleId: <integer>,
  collectionId: "<string>",
  encryptionKey: "<string>",
  fileLocation: "<string>",
  ```

**Consume** topic "uk.gov.ons.dp.web.complete"
 - Update schedule as complete when this message is received

### Publish-metadata

**Consume** topic "uk.gov.ons.dp.web.publish-file"

**Publish**
 - to topic "uk.gov.ons.dp.web.complete-file":
  ```
  fileId: <integer>,
  scheduleId: <integer>,
  collectionId: "<string>",
  fileLocation: "<string>",
  fileContent: "<data.json>",
  ```
 - to topic "uk.gov.ons.dp.web.complete-file-flag"
 ```
 fileId: <integer>,
 scheduleId: <integer>,
 collectionId: "<string>",
 ```

### Publish-data

**Consume** topic "uk.gov.ons.dp.web.publish-file"

**Output**
 - static content is decrypted and placed in an S3 bucket
 - to topic "uk.gov.ons.dp.web.complete-file":
  ```
  fileId: <integer>,
  scheduleId: <integer>,
  collectionId: "<string>",
  fileLocation: "<string>",
  s3Location: "<data.json>",
  ```
 - publish to topic "uk.gov.ons.dp.web.complete-file-flag"
   (cf _Publish-sender_ above):

### Publish-tracker

**Consume** topic:
- "uk.gov.ons.dp.web.complete-file-flag"

**Output**
- "uk.gov.ons.dp.web.complete"
```
scheduleId: <integer>,
collectionId: "<string>",
```

---

## Web

### publish-receiver

**Consume** topic "uk.gov.ons.dp.web.complete-file"

**Output** Content is written to database (metadata or s3URL)
