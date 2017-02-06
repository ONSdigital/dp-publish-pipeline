package mongo

import mongo "github.com/ONSdigital/dp-publish-pipeline/mongodb"

const batchSize = 1

func QueueS3DataTest() {
	batchNext := 0
	dbSession, err := mongo.CreateClient()
	if err != nil {
		panic(err)
	}
	bulk := dbSession.S3C.Bulk()
	for i := 0; i < 1000000; i++ {
		if batchNext == batchSize {
			_, err := bulk.Run()
			if err != nil {
				panic(err)
			}
			batchNext = 0
		}
		QueueS3Data(bulk, S3Document{"foo", "bar", "ook"})
	}
	if batchNext != 0 {
		_, err := bulk.Run()
		if err != nil {
			panic(err)
		}
	}
}
