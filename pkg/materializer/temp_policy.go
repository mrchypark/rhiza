package materializer

import (
	"fmt"
	"strings"
)

type sqlPolicyToken struct {
	text   string
	bare   bool
	symbol bool
}

// sqlPolicyTokens is a lexical guard, not a SQL parser. Keep quoted names and
// literals intact: SQLite also accepts single-quoted schema identifiers.
func sqlPolicyTokens(query string) ([]sqlPolicyToken, error) {
	var tokens []sqlPolicyToken
	for i := 0; i < len(query); {
		c := query[i]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v' {
			i++
			continue
		}
		if strings.HasPrefix(query[i:], "\ufeff") {
			i += len("\ufeff")
			continue
		}
		if c == '-' && i+1 < len(query) && query[i+1] == '-' {
			i += 2
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(query) && query[i+1] == '*' {
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				break
			}
			i += end + 4
			continue
		}
		if c == '\'' || c == '"' || c == '`' || c == '[' {
			end := c
			if c == '[' {
				end = ']'
			}
			i++
			var b strings.Builder
			closed := false
			for i < len(query) {
				if query[i] == end {
					i++
					if end != ']' && i < len(query) && query[i] == end {
						b.WriteByte(end)
						i++
						continue
					}
					closed = true
					break
				}
				b.WriteByte(query[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated SQL quote")
			}
			tokens = append(tokens, sqlPolicyToken{text: b.String()})
			continue
		}
		if c == '?' || c == ':' || c == '@' || c == '$' || c == '#' {
			i++
			for i < len(query) && (sqlNameByte(query[i]) || query[i] == ':') {
				i++
			}
			if c != '?' && i < len(query) && query[i] == '(' {
				for i < len(query) && query[i] != ')' {
					i++
				}
				if i < len(query) {
					i++
				}
			}
			tokens = append(tokens, sqlPolicyToken{text: "?"})
			continue
		}
		if sqlNameByte(c) {
			start := i
			for i < len(query) && sqlNameByte(query[i]) {
				i++
			}
			tokens = append(tokens, sqlPolicyToken{text: query[start:i], bare: true})
			continue
		}
		tokens = append(tokens, sqlPolicyToken{text: string(c), symbol: true})
		i++
	}
	return tokens, nil
}

func sqlNameByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 128 || c == '$'
}

func validateReplicatedTempSQL(query string) error {
	tokens, err := sqlPolicyTokens(query)
	if err != nil {
		return err
	}
	for i, t := range tokens {
		if t.bare && strings.EqualFold(t.text, "CREATE") && i+1 < len(tokens) && tokens[i+1].bare && (strings.EqualFold(tokens[i+1].text, "TEMP") || strings.EqualFold(tokens[i+1].text, "TEMPORARY")) {
			return fmt.Errorf("TEMP objects are not allowed on the replicated write API")
		}
		if strings.EqualFold(t.text, "temp") && i+1 < len(tokens) && tokens[i+1].symbol && tokens[i+1].text == "." {
			return fmt.Errorf("temp qualifiers are not allowed on the replicated write API")
		}
	}
	return nil
}

func singleSchemaMaintenanceSQL(query string) bool {
	tokens, err := sqlPolicyTokens(query)
	if err != nil || len(tokens) < 3 || !tokens[0].bare || !(strings.EqualFold(tokens[0].text, "ALTER") || strings.EqualFold(tokens[0].text, "DROP")) || !tokens[1].bare || !strings.EqualFold(tokens[1].text, "TABLE") {
		return false
	}
	for len(tokens) > 0 && tokens[len(tokens)-1].symbol && tokens[len(tokens)-1].text == ";" {
		tokens = tokens[:len(tokens)-1]
	}
	for _, t := range tokens {
		if t.symbol && t.text == ";" {
			return false
		}
	}
	return true
}
