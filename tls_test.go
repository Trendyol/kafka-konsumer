package kafka

import (
	"os"
	"testing"
)

const testRootCA = `-----BEGIN CERTIFICATE-----
MIIECTCCA3KgAwIBAgIUDnU7Oa0fU9GFOwU7EWJP3HsRchEwDQYJKoZIhvcNAQEL
BQAwgZkxCzAJBgNVBAYTAlVTMRAwDgYDVQQIDAdNb250YW5hMRAwDgYDVQQHDAdC
b3plbWFuMREwDwYDVQQKDAhTYXd0b290aDEYMBYGA1UECwwPQ29uc3VsdGluZ18x
MDI0MRgwFgYDVQQDDA93d3cud29sZnNzbC5jb20xHzAdBgkqhkiG9w0BCQEWEGlu
Zm9Ad29sZnNzbC5jb20wHhcNMjIxMjE2MjExNzQ5WhcNMjUwOTExMjExNzQ5WjCB
mTELMAkGA1UEBhMCVVMxEDAOBgNVBAgMB01vbnRhbmExEDAOBgNVBAcMB0JvemVt
YW4xETAPBgNVBAoMCFNhd3Rvb3RoMRgwFgYDVQQLDA9Db25zdWx0aW5nXzEwMjQx
GDAWBgNVBAMMD3d3dy53b2xmc3NsLmNvbTEfMB0GCSqGSIb3DQEJARYQaW5mb0B3
b2xmc3NsLmNvbTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAzazdR+y+tyTD
YxtUmHnhxzEWWdadd52N4ovtBBeyxuvkm5G+MVBil1i1fynes3EkC7+XCX8m3C3s
qC6yZCt6KzUZLaKAy5n9lHEbI41U2y5ijYEILfQkcids+cmO20x1upsB+D8Y9OZ/
+1eUksyIxLQAwqrU5YgYsxEvc8DWKQkCAwEAAaOCAUowggFGMB0GA1UdDgQWBBTT
Io8oLOAF7tPtw3E9ybI2Oh2/qDCB2QYDVR0jBIHRMIHOgBTTIo8oLOAF7tPtw3E9
ybI2Oh2/qKGBn6SBnDCBmTELMAkGA1UEBhMCVVMxEDAOBgNVBAgMB01vbnRhbmEx
EDAOBgNVBAcMB0JvemVtYW4xETAPBgNVBAoMCFNhd3Rvb3RoMRgwFgYDVQQLDA9D
b25zdWx0aW5nXzEwMjQxGDAWBgNVBAMMD3d3dy53b2xmc3NsLmNvbTEfMB0GCSqG
SIb3DQEJARYQaW5mb0B3b2xmc3NsLmNvbYIUDnU7Oa0fU9GFOwU7EWJP3HsRchEw
DAYDVR0TBAUwAwEB/zAcBgNVHREEFTATggtleGFtcGxlLmNvbYcEfwAAATAdBgNV
HSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwDQYJKoZIhvcNAQELBQADgYEAuIC/
svWDlVGBan5BhynXw8nGm2DkZaEElx0bO+kn+kPWiWo8nr8o0XU3IfMNZBeyoy2D
Uv9X8EKpSKrYhOoNgAVxCqojtGzG1n8TSvSCueKBrkaMWfvDjG1b8zLshvBu2ip4
q/I2+0j6dAkOGcK/68z7qQXByeGri3n28a1Kn6o=
-----END CERTIFICATE-----`

func TestTLSConfig_TLSConfig(t *testing.T) {
	// Given
	rootca, err := os.CreateTemp("", "rootca*.pem")
	if err != nil {
		t.Fatalf("Error creating rootca pem temp file %s", err.Error())
	}
	defer os.Remove(rootca.Name())

	intermediate, err := os.CreateTemp("", "intermediate*.pem")
	if err != nil {
		t.Fatalf("Error creating rootca pem temp file %s", err.Error())
	}
	defer os.Remove(intermediate.Name())

	if err := os.WriteFile(rootca.Name(), []byte(testRootCA), 0o644); err != nil {
		t.Fatalf("Error writing root CA to file: %s", err.Error())
	}

	tlsCfg := TLSConfig{
		RootCAPath:         rootca.Name(),
		IntermediateCAPath: intermediate.Name(),
	}

	logger := NewZapLogger(LogLevelInfo)

	// When
	_, err = tlsCfg.TLSConfig(logger)
	// Then
	if err != nil {
		t.Fatalf("Error when settings tls certificates %s", err.Error())
	}
}

func TestTLSConfig_IsEmpty(t *testing.T) {
	type fields struct {
		RootCAPath         string
		IntermediateCAPath string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name:   "Empty_When_Paths_Does_Not_Exist",
			fields: fields{},
			want:   true,
		},
		{
			name:   "Filled_When_Paths_Exist",
			fields: fields{RootCAPath: "somepath", IntermediateCAPath: "somepath"},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &TLSConfig{
				RootCAPath:         tt.fields.RootCAPath,
				IntermediateCAPath: tt.fields.IntermediateCAPath,
			}
			if got := c.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTLSConfig_Json(t *testing.T) {
	t.Run("Should_Convert_Nil_Config_To_Json", func(t *testing.T) {
		// Given
		var cfg *TLSConfig

		expected := "{}"
		// When
		result := cfg.JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
	t.Run("Should_Convert_To_Json", func(t *testing.T) {
		// Given
		cfg := &TLSConfig{
			RootCAPath:         "resources/ca",
			IntermediateCAPath: "resources/intCa",
		}

		expected := "{\"RootCAPath\": \"resources/ca\", \"IntermediateCAPath\": \"resources/intCa\"}"
		// When
		result := cfg.JSON()
		// Then
		if result != expected {
			t.Fatal("result must be equal to expected")
		}
	})
}
