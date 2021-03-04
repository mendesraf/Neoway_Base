package main

import (
	"bytes"
	"log"
	"regexp"
	"strconv"
	"unicode"
)

// Regexp pattern for CPF and CNPJ.
var (
	CPFRegexp  = regexp.MustCompile(`^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$`)
	CNPJRegexp = regexp.MustCompile(`^\d{2}\.?\d{3}\.?\d{3}\/?(:?\d{3}[1-9]|\d{2}[1-9]\d|\d[1-9]\d{2}|[1-9]\d{3})-?\d{2}$`)
)

// IsValidCPF verifies if the given string is a valid CPF document.
func IsValidCPF(doc string) bool {

	const (
		size = 9
		pos  = 10
	)

	return isValidCPFOrCNPJ(doc, CPFRegexp, size, pos)
}

// IsValidCNPJ verifies if the given string is a valid CNPJ document.
func IsValidCNPJ(doc string) bool {

	const (
		size = 12
		pos  = 5
	)

	return isValidCPFOrCNPJ(doc, CNPJRegexp, size, pos)
}

// isValidCPFOrCNPJ generates the digits for a given CPF or CNPJ and compares it with the original digits.
func isValidCPFOrCNPJ(doc string, pattern *regexp.Regexp, size int, position int) bool {

	if !pattern.MatchString(doc) {
		log.Printf("isValidCPFOrCNPJ() - doc: "+ doc +" inválido")
		return false
	}

	cleanNonDigits(&doc)

	// Invalidates documents with all digits equal.
	if allEq(doc) {
		return false
	}

	d := doc[:size]
	digit := calculateDigit(d, position)

	d = d + digit
	digit = calculateDigit(d, position+1)

	return doc == d+digit
}

// cleanNonDigits removes every rune that is not a digit.
func cleanNonDigits(doc *string) {

	buf := bytes.NewBufferString("")
	for _, r := range *doc {
		if unicode.IsDigit(r) {
			buf.WriteRune(r)
		}
	}

	*doc = buf.String()
}

// allEq checks if every rune in a given string is equal.
func allEq(doc string) bool {

	base := doc[0]
	for i := 1; i < len(doc); i++ {
		if base != doc[i] {
			return false
		}
	}

	return true
}

// calculateDigit calculates the next digit for the given document.
func calculateDigit(doc string, position int) string {

	var sum int
	for _, r := range doc {

		sum += toInt(r) * position
		position--

		if position < 2 {
			position = 9
		}
	}

	sum %= 11
	if sum < 2 {
		return "0"
	}

	return strconv.Itoa(11 - sum)
}

// allDigit checks if every rune in a given string is a digit.
func allDigit(doc string) bool {

	for _, r := range doc {
		if !unicode.IsDigit(r) {
			return false
		}
	}

	return true
}

// toInt converts a rune to an int.
func toInt(r rune) int {
	return int(r - '0')
}

// formatCPF formats CPF string
func formatCPF(doc string) string {

	if (len(doc) == 11) {
		return (doc[0:3] +"."+ doc[3:6] +"."+ doc[6:9] +"-"+ doc[9:])
	}

	return doc
}