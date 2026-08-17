package secret

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

const tokenEnvironment = "GRAPHENE_AGENT_TOKEN"

func Load(tokenFile string, lookupEnv func(string) (string, bool)) (string, error) {
	direct, directSet := lookupEnv(tokenEnvironment)
	if directSet && tokenFile != "" {
		return "", errors.New("GRAPHENE_AGENT_TOKEN and token file are mutually exclusive")
	}

	if directSet {
		token := strings.TrimSpace(direct)
		if token == "" {
			return "", errors.New("GRAPHENE_AGENT_TOKEN is empty")
		}
		return token, nil
	}

	if tokenFile == "" {
		return "", errors.New("agent token is required")
	}
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	if len(data) > 64<<10 {
		return "", errors.New("token file exceeds 64 KiB")
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", errors.New("token file is empty")
	}
	return token, nil
}
