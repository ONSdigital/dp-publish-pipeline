package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"io/ioutil"
)

func decrypt(block cipher.Block, ciphertext []byte) []byte {
	// The IV will be the first 16 bytes within the cipher text.
	iv := ciphertext[:block.BlockSize()]
	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(ciphertext))
	stream.XORKeyStream(plaintext, ciphertext[block.BlockSize():])
	return plaintext
}

func main() {
	aeskey, _ := base64.StdEncoding.DecodeString("2iyOwMI3YF+fF+SDqMlD8Q==")
	data, _ := ioutil.ReadFile("../test-data/collections/test-0001/complete/about/data.json")
	block, _ := aes.NewCipher(aeskey)
	fmt.Printf("ciphertext :%s\n", decrypt(block, data))
}
