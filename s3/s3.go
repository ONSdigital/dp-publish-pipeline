package s3

import (
	"log"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/minio/minio-go"
)

func CreateS3Client() *minio.Client {
	endpoint := utils.GetEnvironmentVariable("S3_URL", "localhost:4000")
	accessKeyID := utils.GetEnvironmentVariable("S3_ACCESS_KEY", "1234")
	secretAccessKey := utils.GetEnvironmentVariable("S3_SECRET_ACCESS_KEY", "1234")
	useSSL := false

	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}
	return minioClient
}

func SetupBucket(client *minio.Client, bucketName string, location string) {
	err := client.MakeBucket(bucketName, location)
	if err != nil {
		log.Fatalln(err)
	} else {
		log.Printf("%s bucket aleady exits\n", bucketName)
	}
}

func AddFileToS3(client *minio.Client, bucketName string, content string, location string) {
	file := strings.NewReader(content)
	client.PutObject(bucketName, location, file, "application/octet-stream")
	log.Printf("Successfully created file at %s within %s", location, bucketName)
}
