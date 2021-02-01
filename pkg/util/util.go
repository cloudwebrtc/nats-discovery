package util

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"time"
)

// RandomString generate a random string
func RandomString(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// Unmarshal parses the encoded data and stores the result
// in the value pointed to by v
func Unmarshal(data []byte, v interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	return dec.Decode(v)
}

// Marshal encodes v and returns encoded data
func Marshal(v interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(v)
	if err != nil {
		return []byte{}, err
	}
	return buf.Bytes(), nil
}
