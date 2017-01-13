package decrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"io"
	"log"
	"os"
)

func DecryptFile(path string, key string) ([]byte, error) {
	aeskey, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		log.Println("Decrypted key is not base64 encoded", err.Error())
		return nil, err
	}

	inFile, err := os.Open(path)
	if err != nil {
		log.Println("Failed to open file at", path, err.Error())
		return nil, err
	}

	block, err := aes.NewCipher(aeskey)
	if err != nil {
		log.Println("Failed to create cipher with the key provided", err.Error())
		return nil, err
	}
	return decrypt(block, inFile), nil
}

func decrypt(block cipher.Block, inFile *os.File) []byte {
	// The IV will be the first BlockSize bytes of inFile
	iv := make([]byte, block.BlockSize())
	if _, err := io.ReadFull(inFile, iv); err != nil {
		panic(err)
	}
	stream := cipher.NewCTR(block, iv)
	reader := &cipher.StreamReader{S: stream, R: inFile}

	plaintextBuf := bytes.NewBuffer(nil)
	_, err := io.Copy(plaintextBuf, reader)
	if err != nil {
		panic(err)
	}

	return plaintextBuf.Bytes()
}
