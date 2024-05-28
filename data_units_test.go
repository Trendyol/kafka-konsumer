package kafka

import "testing"

func TestDcp_ResolveConnectionBufferSize(t *testing.T) {
	tests := []struct {
		input any
		name  string
		want  int
	}{
		{
			name:  "When_Client_Gives_Int_Value",
			input: 20971520,
			want:  20971520,
		},
		{
			name:  "When_Client_Gives_UInt_Value",
			input: uint(10971520),
			want:  10971520,
		},
		{
			name:  "When_Client_Gives_StringInt_Value",
			input: "15971520",
			want:  15971520,
		},
		{
			name:  "When_Client_Gives_KB_Value",
			input: "500kb",
			want:  500 * 1024,
		},
		{
			name:  "When_Client_Gives_MB_Value",
			input: "10mb",
			want:  10 * 1024 * 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := resolveUnionIntOrStringValue(tt.input); got != tt.want {
				t.Errorf("ResolveConnectionBufferSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConvertToBytes(t *testing.T) {
	testCases := []struct {
		input    string
		expected int
		err      bool
	}{
		{"1kb", 1024, false},
		{"5mb", 5 * 1024 * 1024, false},
		{"5,5mb", 5.5 * 1024 * 1024, false},
		{"8.5mb", 8.5 * 1024 * 1024, false},
		{"10,25 mb", 10.25 * 1024 * 1024, false},
		{"10gb", 10 * 1024 * 1024 * 1024, false},
		{"1KB", 1024, false},
		{"5MB", 5 * 1024 * 1024, false},
		{"12 MB", 12 * 1024 * 1024, false},
		{"10GB", 10 * 1024 * 1024 * 1024, false},
		{"123", 0, true},
		{"15TB", 0, true},
		{"invalid", 0, true},
		{"", 0, true},
		{"123  KB", 123 * 1024, false},
		{"1  MB", 1 * 1024 * 1024, false},
	}

	for _, tc := range testCases {
		result, err := convertSizeUnitToByte(tc.input)

		if tc.err && err == nil {
			t.Errorf("Expected an error for input %s, but got none", tc.input)
		}

		if !tc.err && err != nil {
			t.Errorf("Unexpected error for input %s: %v", tc.input, err)
		}

		if result != tc.expected {
			t.Errorf("For input %s, expected %d bytes, but got %d", tc.input, tc.expected, result)
		}
	}
}
