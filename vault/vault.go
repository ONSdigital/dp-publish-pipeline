package vault

import (
	vaultapi "github.com/hashicorp/vault/api"
)

type VaultClient struct {
	token  string
	client *vaultapi.Client
}

func CreateVaultClient(token, vaultAddress string) (*VaultClient, error) {
	config := vaultapi.Config{Address: vaultAddress}
	client, err := vaultapi.NewClient(&config)
	if err != nil {
		return nil, err
	}
	client.SetToken(token)
	return &VaultClient{token, client}, nil
}

func (c *VaultClient) Renew() error {
	// We only want the error code to see if the token was renewed.
	_, err := c.client.Auth().Token().RenewSelf(0)
	return err
}

func (c *VaultClient) Read(path string) (map[string]interface{}, error) {
	secret, err := c.client.Logical().Read(path)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		// If there is no secret and no error return a empty map.
		return make(map[string]interface{}), nil
	}
	return secret.Data, err
}
