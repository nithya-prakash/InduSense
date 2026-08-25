// Package auth implements password hashing, JWT issuance/validation with
// Redis-backed refresh-token revocation, and RBAC permission resolution —
// the domain logic Phase 10's REST API will wire into HTTP middleware. It
// has no HTTP dependency itself, so it can be (and is) tested directly
// against real Postgres and Redis before any API surface exists.
package auth

import (
	"fmt"
	"unicode"

	"golang.org/x/crypto/bcrypt"
)

func HashPassword(plain string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

func VerifyPassword(hash, plain string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)) == nil
}

// ValidatePasswordStrength enforces a minimum bar: 8+ characters, at least
// one letter and one digit. It's intentionally simple — this is a
// portfolio-scale system, not a compliance-driven one, so it demonstrates
// the control exists without pretending to implement a full password
// policy engine (breach-list checks, entropy scoring, etc.).
func ValidatePasswordStrength(password string) error {
	if len(password) < 8 {
		return fmt.Errorf("password must be at least 8 characters")
	}
	var hasLetter, hasDigit bool
	for _, r := range password {
		switch {
		case unicode.IsLetter(r):
			hasLetter = true
		case unicode.IsDigit(r):
			hasDigit = true
		}
	}
	if !hasLetter || !hasDigit {
		return fmt.Errorf("password must contain at least one letter and one digit")
	}
	return nil
}
