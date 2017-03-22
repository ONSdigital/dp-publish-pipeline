package decrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"io"
	"log"
	"os"

	"github.com/ONSdigital/dp-publish-pipeline/s3"
)

func decodeKey(key string) (cipher.Block, error) {
	aeskey, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		log.Println("Decrypted key is not base64 encoded", err.Error())
		return nil, err
	}
	block, err := aes.NewCipher(aeskey)
	if err != nil {
		log.Println("Failed to create cipher with the key provided", err.Error())
		return nil, err
	}
	return block, nil
}

func DecryptS3(s3Client s3.S3Client, path string, key string) ([]byte, error) {
	block, err := decodeKey(key)
	if err != nil {
		return nil, err
	}

	reader, err := s3Client.GetReader(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer reader.Close()

	return decrypt(block, reader, path), nil
}

func DecryptFile(path string, key string) ([]byte, error) {
	block, err := decodeKey(key)
	if err != nil {
		return nil, err
	}

	inFile, err := os.Open(path)
	if err != nil {
		log.Println("Failed to open file at", path, err.Error())
		return nil, err
	}

	return decrypt(block, inFile, path), nil
}

func decrypt(block cipher.Block, inFile io.Reader, path string) []byte {
	// The IV will be the first BlockSize bytes of inFile
	iv := make([]byte, block.BlockSize())
	if _, err := io.ReadFull(inFile, iv); err != nil {
		log.Panicf("Failed to read %q - %s", path, err)
	}
	stream := cipher.NewCTR(block, iv)
	reader := &cipher.StreamReader{S: stream, R: inFile}

	plaintextBuf := bytes.NewBuffer(nil)
	_, err := io.Copy(plaintextBuf, reader)
	if err != nil {
		log.Panicf("Failed to copy %q - %s", path, err)
	}

	return plaintextBuf.Bytes()
}
