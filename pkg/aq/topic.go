package aq

import (
	"strings"
	"unicode"
)

func normalizeTopic(topic string) string {
	//replace not allowed characters to underscore
	topic = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		}
		return '_'
	}, topic)
	return topic
}
