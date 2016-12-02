package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/decrypt"
	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

type CollectionMessage struct {
	CollectionId  string
	EncryptionKey string
}

func uploadCollection(jsonMessage []byte) {
	var message CollectionMessage
	err := json.Unmarshal(jsonMessage, &message)
	if err != nil {
		log.Printf("Invalid json message received")
	} else {
		log.Printf("Uploading collectionId : %s", message.CollectionId)
		files := findStaticFiles(message.CollectionId)
		s3Client := s3.CreateS3Client()
		bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "static-content")
		for i := 0; i < len(files); i++ {
			file := files[i]
			content := decrypt.DecryptFile(file, message.EncryptionKey)
			// Split the string just after the <collectionId>/complete, this creates the
			// url needed within the secound element of the array.
			url := strings.Split(file, message.CollectionId+"/complete")[1]
			s3.AddFileToS3(s3Client, bucketName, string(content), url)
		}
	}
}

func findStaticFiles(collectionId string) []string {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	searchPath := filepath.Join(zebedeeRoot, "collections", collectionId, "complete")
	var files []string
	filepath.Walk(searchPath, func(path string, info os.FileInfo, _ error) error {
		base := filepath.Base(path)
		if strings.Contains(base, ".png") && !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files
}

func main() {
	bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "static-content")
	log.Printf("Starting Static Content Migrator")
	s3.SetupBucket(s3.CreateS3Client(), bucketName, "eu-west-1")
	master := kafka.CreateConsumer()
	defer func() {
		err := master.Close()
		if err != nil {
			panic(err)
		}
	}()
	topic := utils.GetEnvironmentVariable("TOPIC", "test")
	kafka.ProcessMessages(master, topic, uploadCollection)
	log.Printf("Service stopped")
}
