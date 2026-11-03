package sms

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"09121234567", "+989121234567"},
		{"+989121234567", "+989121234567"},
		{"00989121234567", "+989121234567"},
		{" 0912 123 4567 ", "+989121234567"},
		{"۰۹۱۲۱۲۳۴۵۶۷", "+989121234567"},
		{"٠٩١٢١٢٣٤٥٦٧", "+989121234567"},
		{"+14155550123", "+14155550123"},
	}
	for _, tt := range tests {
		got, err := NormalizePhone(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("NormalizePhone(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
		}
	}
}

func TestNormalizePhoneRejectsInvalid(t *testing.T) {
	for _, in := range []string{"", "12345", "abc", "+0123456789", "0912123456789012345"} {
		_, err := NormalizePhone(in)
		var verr *ValidationError
		if !errors.As(err, &verr) || verr.Field != "to" {
			t.Errorf("NormalizePhone(%q) error = %v, want ValidationError on field to", in, err)
		}
	}
}

func TestValidateText(t *testing.T) {
	tests := []struct {
		name string
		text string
		ok   bool
	}{
		{"latin", "Your code is 1234", true},
		{"latin at limit", strings.Repeat("a", 160), true},
		{"latin over limit", strings.Repeat("a", 161), false},
		{"persian", "کد تایید شما ۱۲۳۴ است", true},
		{"persian at limit", strings.Repeat("س", 70), true},
		{"persian over limit", strings.Repeat("س", 71), false},
		{"mixed text uses unicode limit", "code " + strings.Repeat("س", 66), false},
		{"emoji counts as two units", strings.Repeat("😀", 36), false},
		{"empty", "", false},
		{"only spaces", "   ", false},
		{"invalid utf8", "\xff\xfe", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateText(tt.text)
			if (err == nil) != tt.ok {
				t.Errorf("ValidateText() error = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

func TestStatusValid(t *testing.T) {
	if !StatusSent.Valid() || Status("lost").Valid() {
		t.Error("Status.Valid() gives wrong answer")
	}
}
