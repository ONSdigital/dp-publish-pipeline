package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

const wantPadding = false

func main() {
	key := []byte("example key 1234")
	encKey := make([]byte, len(key)*2)
	base64.StdEncoding.Encode(encKey, key)
	fmt.Printf("key: %v, encKey: %s\n", key, encKey)

	var (
		inFile, outFile *os.File
		err             error
		block           cipher.Block
	)
	if inFile, err = os.Open("plaintext-file"); err != nil {
		panic(err)
	}
	defer inFile.Close()
	if outFile, err = os.OpenFile("encrypted-file", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600); err != nil {
		panic(err)
	}
	defer outFile.Close()

	if block, err = aes.NewCipher(key); err != nil {
		panic(err)
	}

	iv := make([]byte, aes.BlockSize)
	if _, err = io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}
	if _, err = outFile.Write(iv); err != nil {
		panic(err)
	}

	stream := cipher.NewCTR(block, iv[:])
	writer := &cipher.StreamWriter{S: stream, W: outFile}
	fileLen, err := io.Copy(writer, inFile)
	if err != nil {
		panic(err)
	}

	if wantPadding {
		paddingLen := int(aes.BlockSize - fileLen%aes.BlockSize)
		if paddingLen > 0 {
			padding := bytes.Repeat([]byte{0}, aes.BlockSize)
			if _, err = outFile.Write(padding[:paddingLen]); err != nil {
				panic(err)
			}
		}
	}
}
