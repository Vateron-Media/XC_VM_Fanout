//go:build !unix

package supervisor

func LoadDriverConfig(string) (map[string]string, error)  { return map[string]string{}, nil }
func LoadEncoderConfig(string) (map[string]string, error) { return map[string]string{}, nil }
