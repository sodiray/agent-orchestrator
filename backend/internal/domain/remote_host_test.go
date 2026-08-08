package domain

import "testing"

func TestValidateRemoteHostID(t *testing.T) {
	for _, tc := range []struct {
		id      RemoteHostID
		wantErr bool
	}{
		{id: "lab-host", wantErr: false},
		{id: "a1", wantErr: false},
		{id: "", wantErr: true},
		{id: "Lab-host", wantErr: true},
		{id: "lab_host", wantErr: true},
		{id: "-lab", wantErr: true},
		{id: "lab-", wantErr: true},
	} {
		if err := ValidateRemoteHostID(tc.id); (err != nil) != tc.wantErr {
			t.Errorf("ValidateRemoteHostID(%q) error = %v, wantErr %v", tc.id, err, tc.wantErr)
		}
	}
}

func TestValidateRemoteHostAddress(t *testing.T) {
	for _, tc := range []struct {
		address string
		wantErr bool
	}{
		{address: "127.0.0.1:3001", wantErr: false},
		{address: "[::1]:3001", wantErr: false},
		{address: "host", wantErr: true},
		{address: "host:0", wantErr: true},
		{address: " host:3001", wantErr: true},
	} {
		if err := ValidateRemoteHostAddress(tc.address); (err != nil) != tc.wantErr {
			t.Errorf("ValidateRemoteHostAddress(%q) error = %v, wantErr %v", tc.address, err, tc.wantErr)
		}
	}
}
