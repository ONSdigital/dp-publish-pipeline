package mongo

type MetaDocument struct {
	CollectionId string `json:"collectionId" bson:"collectionId"`
	Language     string `json:"fileContent" bson:"language"`
	FileLocation string `json:"fileLocation" bson:"fileLocation"`
	FileContent  string `json:"fileContent" bson:"fileContent"`
}

type S3Document struct {
	CollectionId string `bson:"collectionId"`
	FileLocation string `bson:"fileLocation"`
	S3Location   string `bson:"s3Location"`
}
