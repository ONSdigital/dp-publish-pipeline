package mongo

import (
	"log"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const dataBase = "onswebsite"
const metaCollection = "meta"
const s3Collection = "s3"

const mongodbHost = "MONGODB"

var rmFileContent, rmS3Location bson.M

type MongoClient struct {
	session *mgo.Session
	db      *mgo.Database
	metaC   *mgo.Collection
	S3C     *mgo.Collection
}

func CreateClient() (MongoClient, error) {
	dbSession, err := mgo.Dial(utils.GetEnvironmentVariable(mongodbHost, "localhost"))
	//dbSession.SetMode(0, true)  // 0 == Eventual consistency mode
	rmFileContent = bson.M{"fileContent": 1}
	rmS3Location = bson.M{"s3Location": 1}
	return MongoClient{dbSession, dbSession.DB(dataBase), dbSession.DB(dataBase).C(metaCollection), dbSession.DB(dataBase).C(s3Collection)}, err
}

func (c *MongoClient) Close() {
	c.session.Close()
}

func (c *MongoClient) FindPage(uri string) (MetaDocument, error) {
	var document MetaDocument
	notFoundErr := c.metaC.Find(bson.M{"fileLocation": uri}).One(&document)
	return document, notFoundErr
}

func (c *MongoClient) AddPage(document MetaDocument) error {
	query := bson.M{"fileLocation": document.FileLocation}
	setter := bson.M{"fileContent": document.FileContent, "collectionId": document.CollectionId}
	updateRes, err := c.metaC.Upsert(query, bson.M{"$set": setter, "$unset": rmS3Location})
	if err != nil {
		log.Panicf("Failed to upsert meta %s", err.Error())
	}
	if updateRes.Updated != 1 && updateRes.Matched != 1 {
		log.Printf("meta update %v", updateRes)
	}
	return err
}

func (c *MongoClient) AddS3Data(document S3Document) error {
	query := bson.M{"fileLocation": document.FileLocation}
	setter := bson.M{"s3Location": document.S3Location, "collectionId": document.CollectionId}
	change := bson.M{"$set": setter, "$unset": rmFileContent}
	updateRes, err := c.S3C.Upsert(query, change)
	if err != nil {
		log.Panicf("Failed to upsert s3 %s", err.Error())
	}
	if updateRes.Updated != 1 && updateRes.Matched != 1 {
		log.Printf("s3 update %v", updateRes)
	}
	return err
}

func QueueS3Data(b *mgo.Bulk, document S3Document) {
	query := bson.M{"fileLocation": document.FileLocation}
	setter := bson.M{"s3Location": document.S3Location, "collectionId": document.CollectionId}
	change := bson.M{"$set": setter, "$unset": rmFileContent}
	b.Upsert(query, change)
}
