package kafka

import (
	"os"
	"testing"
)

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

	tlsCfg := TLSConfig{
		RootCAPath:         rootca.Name(),
		IntermediateCAPath: intermediate.Name(),
	}

	// When
	_, err = tlsCfg.TLSConfig()
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
