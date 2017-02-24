package kafka

/* PublishMessage is not used externally yet - message is within publish-scheduler
type PublishMessage struct {
	CollectionId  string
	EncryptionKey string
	Files         []string
}
*/

type ScheduleMessage struct {
	CollectionId   string
	CollectionPath string
	EncryptionKey  string
	ScheduleTime   string
}

type PublishFileMessage struct {
	ScheduleId     int64
	FileId         int64
	CollectionId   string
	CollectionPath string
	EncryptionKey  string
	FileLocation   string
}

// S3Location and FileContent are mutually exclusive
type FileCompleteMessage struct {
	ScheduleId   int64
	FileId       int64
	CollectionId string
	FileLocation string
	S3Location   string
	FileContent  string
}

type FileCompleteFlagMessage struct {
	ScheduleId   int64
	FileId       int64
	CollectionId string
	FileLocation string
}

type CollectionCompleteMessage struct {
	ScheduleId   int64
	CollectionId string
}
