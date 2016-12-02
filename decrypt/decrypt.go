package decrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"io/ioutil"
	"log"
)

func DecryptFile(path string, key string) []byte {
	aeskey, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		log.Printf("Decrypted key is not base64 encoded")
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("Failed to load file at %s", path)
	}

	block, err := aes.NewCipher(aeskey)
	if err != nil {
		log.Printf("Failed to create cipher with the key provided")
	}
	return decrypt(block, data)
}

func decrypt(block cipher.Block, ciphertext []byte) []byte {
	// The IV will be the first 16 bytes within the cipher text.
	iv := ciphertext[:block.BlockSize()]
	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(ciphertext))
	stream.XORKeyStream(plaintext, ciphertext[block.BlockSize():])
	return plaintext
}
