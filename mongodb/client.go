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

type MongoClient struct {
	session *mgo.Session
	db      *mgo.Database
}

func CreateClient() (MongoClient, error) {
	dbSession, err := mgo.Dial(utils.GetEnvironmentVariable(mongodbHost, "localhost"))
	return MongoClient{dbSession, dbSession.DB(dataBase)}, err
}

func (c *MongoClient) Close() {
	c.session.Close()
}

func (c *MongoClient) FindPage(uri string, lang string) (MetaDocument, error) {
	var document MetaDocument
	if lang == "" {
		lang = "en"
	}
	log.Printf("lang : %s", lang)
	notFoundErr := c.db.C(metaCollection).Find(bson.M{"fileLocation": uri, "language": lang}).One(&document)
	return document, notFoundErr
}

func (c *MongoClient) AddPage(document MetaDocument) error {
	collection := c.db.C(metaCollection)
	query := bson.M{"fileLocation": document.FileLocation, "language": document.Language}
	change := bson.M{"$set": bson.M{"fileContent": document.FileContent, "collectionId": document.CollectionId}}
	updateErr := collection.Update(query, change)
	if updateErr != nil {
		// No document existed so this must be a new page
		insertError := collection.Insert(document)
		return insertError
	}
	return nil
}

func (c *MongoClient) AddS3Data(document S3Document) error {
	collection := c.db.C(s3Collection)
	query := bson.M{"fileLocation": document.FileLocation}
	change := bson.M{"$set": bson.M{"s3Location": document.S3Location, "collectionId": document.CollectionId}}
	updateErr := collection.Update(query, change)
	if updateErr != nil {
		// No document existed so this must be a new page
		insertError := collection.Insert(document)
		return insertError
	}
	return nil
}
