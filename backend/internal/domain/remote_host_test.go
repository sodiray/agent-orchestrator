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

func TestQualifiedSessionIDRoundTrip(t *testing.T) {
	qualified := QualifySessionID("build-host", "project-42")
	if qualified != "build-host~project-42" {
		t.Fatalf("qualified = %q", qualified)
	}
	parsed, ok := ParseQualifiedSessionID(qualified)
	if !ok {
		t.Fatal("ParseQualifiedSessionID() = false")
	}
	if parsed.HostID != "build-host" || parsed.SessionID != "project-42" {
		t.Fatalf("parsed = %#v", parsed)
	}
}

func TestParseQualifiedSessionIDRejectsNonQualifiedIDs(t *testing.T) {
	for _, id := range []SessionID{
		"project-42",
		"~project-42",
		"Build-host~project-42",
		"build-host~",
		"build-host~project~42",
	} {
		if _, ok := ParseQualifiedSessionID(id); ok {
			t.Errorf("ParseQualifiedSessionID(%q) = true", id)
		}
	}
}
