package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func encryptFile(path string, key string) {
	data, _ := ioutil.ReadFile(path)
	block, _ := aes.NewCipher([]byte(key))
	iv := []byte("16bitkey-1234567")
	stream := cipher.NewCTR(block, iv)
	ciphertext := make([]byte, len(data))
	stream.XORKeyStream(ciphertext, data)
	ioutil.WriteFile(path, append(iv, ciphertext...), 0777)
}

func main() {
	dir := flag.String("dir", "./master", "Directory to encrypt")
	flag.Parse()
	log.Printf("Exporting collection")
	key := "16bitkey-1234567"
	log.Printf("Key : %s", base64.StdEncoding.EncodeToString([]byte(key)))
	var files []string
	filepath.Walk(*dir, func(path string, info os.FileInfo, _ error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	for i := 0; i < len(files); i++ {
		encryptFile(files[i], key)
	}
	log.Printf("Encrypted %d files", len(files))
}
