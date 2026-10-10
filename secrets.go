package main

// Per-job git credentials entered in the UI are stored in MongoDB encrypted with AES-256-GCM.
// The key is derived from SECRET_KEY (Kubernetes Secret); without it the UI cannot store credentials.

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"os"
)

var errNoSecretKey = errors.New("SECRET_KEY is not set: git keys entered in the UI cannot be stored")

func secretAEAD() (cipher.AEAD, error) {
	k := os.Getenv("SECRET_KEY")
	if k == "" {
		return nil, errNoSecretKey
	}
	sum := sha256.Sum256([]byte(k))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func encryptSecret(plain string) (string, error) {
	aead, err := secretAEAD()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(aead.Seal(nonce, nonce, []byte(plain), nil)), nil
}

func decryptSecret(enc string) (string, error) {
	if enc == "" {
		return "", nil
	}
	aead, err := secretAEAD()
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil || len(raw) < aead.NonceSize() {
		return "", errors.New("corrupted encrypted key")
	}
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], nil)
	if err != nil {
		return "", errors.New("could not decrypt the git key (has SECRET_KEY changed?)")
	}
	return string(plain), nil
}
