package decrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"io"
	"os"

	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/go-ns/log"
)

func decodeKey(key string) (cipher.Block, error) {
	aeskey, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		log.ErrorC("Decrypted key is not base64 encoded", err, nil)
		return nil, err
	}
	block, err := aes.NewCipher(aeskey)
	if err != nil {
		log.ErrorC("Failed to create cipher with the key provided", err, nil)
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
		log.Error(err, nil)
		return nil, err
	}
	defer reader.Close()

	return decrypt(block, reader, path)
}

func DecryptFile(path string, key string) ([]byte, error) {
	block, err := decodeKey(key)
	if err != nil {
		return nil, err
	}

	inFile, err := os.Open(path)
	if err != nil {
		log.ErrorC("Failed to open file", err, log.Data{"path": path})
		return nil, err
	}

	return decrypt(block, inFile, path)
}

func decrypt(block cipher.Block, inFile io.Reader, path string) ([]byte, error) {
	// The IV will be the first BlockSize bytes of inFile
	iv := make([]byte, block.BlockSize())
	if _, err := io.ReadFull(inFile, iv); err != nil {
		log.ErrorC("Failed to read", err, log.Data{"path": path})
		return nil, err
	}
	stream := cipher.NewCTR(block, iv)
	reader := &cipher.StreamReader{S: stream, R: inFile}

	plaintextBuf := bytes.NewBuffer(nil)
	_, err := io.Copy(plaintextBuf, reader)
	if err != nil {
		log.ErrorC("Failed to copy", err, log.Data{"path": path})
		return nil, err
	}

	return plaintextBuf.Bytes(), nil
}
