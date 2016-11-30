## Publishing

### Publish-scheduler
Input:
```
collecionId: "<string>",
publishTime: "epoch",
encryptionKey: "Blob",
```
Ouput:
```
collecionId: "<string>",
encryptionKey: "Blob",
```
### Static-content-migrator
Input:
```
collecionId: "<string>",
encryptionKey: "Blob",
```
Output:
Content is placed on a S3 bucket

### publish-sender
Input:
```
collecionId: "<string>",
encryptionKey: "Blob",
```
Output:
```
fileLocation: "<string>",
fileContent: "<data.json>"
```
## Web

### publish-receiver
Input:
```
fileLocation: "<string>",
fileContent: "<data.json>"
```
Output:
Content is write to disk
