package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"

	configv1 "github.com/openshift/api/config/v1"
)

const (
	// KMSPluginEndpointFmt holds the unix socket path where the KMS plugin would be run
	// uniquely distinguished by the kms key id
	KMSPluginEndpointFmt = "unix:///var/kube-kms/%s/socket.sock"
)

// EncodeKMSConfig encodes kms config into json format
func EncodeKMSConfig(config *configv1.KMSConfig) ([]byte, error) {
	return json.Marshal(config)
}

// HashKMSConfig returns a short FNV 64-bit hash for a KMSConfig struct
func HashKMSConfig(config configv1.KMSConfig) (string, error) {
	// TODO: also track collision count, only if reqd.
	// refer upstream PodTemplateHash implementation in kcm deployment controller
	hasher := fnv.New64a()
	hasher.Reset()

	encoded, err := EncodeKMSConfig(&config)
	if err != nil {
		return "", fmt.Errorf("could not generate hash for KMS config: %v", err)
	}

	fmt.Fprintf(hasher, "%s", encoded)
	return hex.EncodeToString(hasher.Sum(nil)[0:]), nil
}

func main() {
	keyConfigHash, err := HashKMSConfig(configv1.KMSConfig{
		Type: configv1.AWSKMSProvider,
		AWS: &configv1.AWSKMSConfig{
			KeyARN: "arn:aws:kms:ap-south-1:269733383066:key/556f99af-bf50-42b6-9a2d-2dfb8ab2905d",
			Region: "ap-south-1",
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(keyConfigHash)
}
