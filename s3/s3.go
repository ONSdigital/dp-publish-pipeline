package s3

import (
	"io/ioutil"
	"log"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/minio/minio-go"
)

type S3Client struct {
	client *minio.Client
	bucket string
}

func CreateClient(bucket string) S3Client {
	endpoint := utils.GetEnvironmentVariable("S3_URL", "localhost:4000")
	accessKeyID := utils.GetEnvironmentVariable("S3_ACCESS_KEY", "1234")
	secretAccessKey := utils.GetEnvironmentVariable("S3_SECRET_ACCESS_KEY", "1234")
	useSSL := false

	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}
	return S3Client{minioClient, bucket}
}

func (s3 *S3Client) CreateBucket(awsZone string) {
	if err := s3.client.MakeBucket(s3.bucket, awsZone); err != nil {
		if exists, err_exists := s3.client.BucketExists(s3.bucket); err_exists == nil && exists {
			log.Printf("We already own %s\n", s3.bucket)
			return
		}
		log.Fatalln(err)
	}
}

func (s3 *S3Client) GetObject(location string) ([]byte, error) {
	object, err := s3.client.GetObject(s3.bucket, location)
	if err != nil {
		return nil, err
	}
	data, _ := ioutil.ReadAll(object)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s3 *S3Client) AddObject(content, s3Location, collectionId string, scheduleId int64) {
	file := strings.NewReader(content)
	s3.client.PutObject(s3.bucket, s3Location, file, "application/octet-stream")
	log.Printf("Job %d Collection %q filed %q to s3:%s", scheduleId, collectionId, s3Location, s3.bucket)
}
