package s3

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/ONSdigital/go-ns/log"
	"github.com/minio/minio-go"
)

type S3Client struct {
	client *minio.Client
	Bucket string
}

func CreateClient(bucket, endpoint, accessKeyID, secretAccessKey string, useSSL bool) (S3Client, error) {
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Error(err, nil)
		return S3Client{}, err
	}
	return S3Client{minioClient, bucket}, nil
}

func (s3 *S3Client) CreateBucket(awsZone string) error {
	if err := s3.client.MakeBucket(s3.Bucket, awsZone); err != nil {
		if exists, err_exists := s3.client.BucketExists(s3.Bucket); err_exists == nil && exists {
			log.Trace("already own", log.Data{"bucket": s3.Bucket})
			return nil
		}
		log.Error(err, nil)
		return err
	}
	return nil
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
	log.Trace(fmt.Sprintf("Job %d Collection %q filed %q to s3:%s", scheduleId, collectionId, s3Location, s3.Bucket), nil)
}
