package s3

import (
	"io/ioutil"
	"log"
	"strings"

	"github.com/minio/minio-go"
)

type S3Client struct {
	client *minio.Client
	Bucket string
}

func CreateClient(bucket, endpoint, accessKeyID, secretAccessKey string, useSSL bool) S3Client {
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}
	return S3Client{minioClient, bucket}
}

func (s3 *S3Client) CreateBucket(awsZone string) {
	if err := s3.client.MakeBucket(s3.Bucket, awsZone); err != nil {
		if exists, err_exists := s3.client.BucketExists(s3.Bucket); err_exists == nil && exists {
			log.Printf("We already own %s\n", s3.Bucket)
			return
		}
		log.Fatalln(err)
	}
}

func (s3 *S3Client) GetReader(location string) (*minio.Object, error) {
	object, err := s3.client.GetObject(s3.Bucket, location)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (s3 *S3Client) GetObject(location string) ([]byte, error) {
	object, err := s3.GetReader(location)
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
	s3.client.PutObject(s3.Bucket, s3Location, file, "application/octet-stream")
	log.Printf("Job %d Collection %q filed %q to s3:%s", scheduleId, collectionId, s3Location, s3.Bucket)
}
