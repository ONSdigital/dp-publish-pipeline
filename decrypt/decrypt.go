package decrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"io/ioutil"
	"log"
)

func DecryptFile(path string, key string) ([]byte, error) {
	aeskey, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		log.Println("Decrypted key is not base64 encoded", err.Error())
		return nil, err
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Println("Failed to load file at", path, err.Error())
		return nil, err
	}

	block, err := aes.NewCipher(aeskey)
	if err != nil {
		log.Println("Failed to create cipher with the key provided", err.Error())
		return nil, err
	}
	return decrypt(block, data), nil
}

func decrypt(block cipher.Block, ciphertext []byte) []byte {
	// The IV will be the first 16 bytes within the cipher text.
	iv := ciphertext[:block.BlockSize()]
	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(ciphertext)-block.BlockSize())
	stream.XORKeyStream(plaintext, ciphertext[block.BlockSize():])
	return plaintext
}
