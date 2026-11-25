package backuptarget

// s3creds.go : S3 credentials without a plaintext secret at rest in the
// environment.
//
//	WEFT_BACKUP_S3_ACCESS_KEY_ID_FROM      secretsource reference
//	WEFT_BACKUP_S3_SECRET_ACCESS_KEY_FROM  secretsource reference
//	WEFT_BACKUP_S3_SESSION_TOKEN_FROM      secretsource reference (optional)
//
// When the secret is given this way (or, deprecated, as the plaintext
// AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN the SDK would have read from the
// environment), the values are held in memory, the plaintext variables are
// unset, and a static provider is handed to the SDK explicitly. When none is
// set the SDK's default chain is untouched: shared credentials file, IAM
// role, instance profile, web identity hold no secret in the environment.

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/openweft/weft-block/internal/secretenv"
)

const (
	envS3AccessKeyID     = "WEFT_BACKUP_S3_ACCESS_KEY_ID"
	envS3SecretAccessKey = "WEFT_BACKUP_S3_SECRET_ACCESS_KEY"
	envS3SessionToken    = "WEFT_BACKUP_S3_SESSION_TOKEN"

	envAWSAccessKeyID     = "AWS_ACCESS_KEY_ID"
	envAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	envAWSSessionToken    = "AWS_SESSION_TOKEN"
)

// loadS3Secret prefers the _FROM reference, then the deprecated plaintext
// AWS variable. ErrUnset when neither is set.
func loadS3Secret(ctx context.Context, own, awsName string) ([]byte, error) {
	v, err := secretenv.LoadFrom(ctx, own)
	if errors.Is(err, secretenv.ErrUnset) {
		return secretenv.LoadHint(ctx, awsName, own+"_FROM")
	}
	return v, err
}

// s3Credentials returns a static provider, or nil to keep the SDK default chain.
func s3Credentials(ctx context.Context) (aws.CredentialsProvider, error) {
	secret, err := loadS3Secret(ctx, envS3SecretAccessKey, envAWSSecretAccessKey)
	if errors.Is(err, secretenv.ErrUnset) {
		if _, e := secretenv.LoadFrom(ctx, envS3AccessKeyID); e == nil || !errors.Is(e, secretenv.ErrUnset) {
			return nil, fmt.Errorf("%s_FROM is set without %s_FROM", envS3AccessKeyID, envS3SecretAccessKey)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("s3 credentials: %w", err)
	}
	defer clear(secret)
	id, err := secretenv.LoadFrom(ctx, envS3AccessKeyID)
	switch {
	case errors.Is(err, secretenv.ErrUnset):
		id = []byte(os.Getenv(envAWSAccessKeyID)) // an identifier, not a secret
	case err != nil:
		return nil, fmt.Errorf("s3 credentials: %w", err)
	}
	if len(id) == 0 {
		return nil, fmt.Errorf("s3 credentials: a secret access key is given without %s_FROM or %s", envS3AccessKeyID, envAWSAccessKeyID)
	}
	token, err := loadS3Secret(ctx, envS3SessionToken, envAWSSessionToken)
	if err != nil && !errors.Is(err, secretenv.ErrUnset) {
		return nil, fmt.Errorf("s3 credentials: %w", err)
	}
	creds := aws.Credentials{
		AccessKeyID:     string(id),
		SecretAccessKey: string(secret),
		SessionToken:    string(token),
		Source:          "weft-block",
	}
	clear(token)
	return aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil }), nil
}
