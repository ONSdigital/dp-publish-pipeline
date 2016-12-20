package kafka

type PublishMessage struct {
	CollectionId  string
	EncryptionKey string
}

type ScheduleMessage struct {
	CollectionId  string
	EncryptionKey string
	ScheduleTime  string
}

type PublishFileMessage struct {
	CollectionId  string
	EncryptionKey string
	FileLocation  string
}

type PublishTotalMessage struct {
	CollectionId string
	FileCount    int
}

// S3Location and FileContent are mutually exclusive
type FileCompleteMessage struct {
	CollectionId string
	FileLocation string
	S3Location   string
	FileContent  string
}

type CollectionCompleteMessage struct {
	CollectionId string
}