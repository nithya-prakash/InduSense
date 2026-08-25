package auth

import "testing"

func TestHashAndVerifyPasswordRoundTrip(t *testing.T) {
	hash, err := HashPassword("correct-horse-battery-staple1")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if !VerifyPassword(hash, "correct-horse-battery-staple1") {
		t.Error("expected the correct password to verify")
	}
	if VerifyPassword(hash, "wrong-password") {
		t.Error("expected an incorrect password to fail verification")
	}
}

func TestHashPasswordNeverStoresPlaintext(t *testing.T) {
	hash, err := HashPassword("hunter2")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if hash == "hunter2" {
		t.Fatal("hash must not equal the plaintext password")
	}
}

func TestValidatePasswordStrengthRejectsWeakPasswords(t *testing.T) {
	cases := []struct {
		password string
		wantErr  bool
	}{
		{"short1", true}, // too short
		{"alllettersnodigits", true},
		{"12345678", true}, // no letters
		{"ValidPass123", false},
		{"exactly8a", false},
	}
	for _, c := range cases {
		err := ValidatePasswordStrength(c.password)
		if (err != nil) != c.wantErr {
			t.Errorf("ValidatePasswordStrength(%q) error = %v, wantErr = %v", c.password, err, c.wantErr)
		}
	}
}
