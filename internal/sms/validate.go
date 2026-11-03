package sms

import (
	"regexp"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Single-segment limits. The challenge says every SMS is one segment, so a
// text that would need more than one segment is rejected instead of split.
const (
	MaxGSMChars     = 160 // 7-bit GSM alphabet
	MaxUnicodeChars = 70  // UCS-2 (e.g. Persian), counted in UTF-16 units
)

var e164 = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

// NormalizePhone turns common Iranian and international formats into E.164:
//
//	09121234567, +989121234567, 00989121234567, ۰۹۱۲۱۲۳۴۵۶۷ -> +989121234567
func NormalizePhone(raw string) (string, error) {
	p := strings.Map(func(r rune) rune {
		switch {
		case r >= '۰' && r <= '۹': // Persian digits
			return '0' + (r - '۰')
		case r >= '٠' && r <= '٩': // Arabic-Indic digits
			return '0' + (r - '٠')
		case r == ' ' || r == '-' || r == '(' || r == ')':
			return -1 // drop formatting characters
		}
		return r
	}, strings.TrimSpace(raw))

	switch {
	case strings.HasPrefix(p, "00"):
		p = "+" + p[2:]
	case strings.HasPrefix(p, "09") && len(p) == 11:
		p = "+98" + p[1:]
	}
	if !e164.MatchString(p) {
		return "", invalid("to", "must be a valid phone number, e.g. 09121234567 or +989121234567")
	}
	return p, nil
}

// ValidateText checks that the text is non-empty and fits in one segment.
func ValidateText(text string) error {
	if strings.TrimSpace(text) == "" {
		return invalid("text", "must not be empty")
	}
	if !utf8.ValidString(text) {
		return invalid("text", "must be valid UTF-8")
	}
	if isGSM(text) {
		if utf8.RuneCountInString(text) > MaxGSMChars {
			return invalid("text", "must fit in one SMS segment (160 latin characters)")
		}
		return nil
	}
	if len(utf16.Encode([]rune(text))) > MaxUnicodeChars {
		return invalid("text", "must fit in one SMS segment (70 unicode characters)")
	}
	return nil
}

// isGSM approximates the GSM 03.38 alphabet with printable ASCII. Any other
// character (Persian, emoji, ...) forces UCS-2 encoding.
func isGSM(s string) bool {
	for _, r := range s {
		if (r < ' ' || r > '~') && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}
