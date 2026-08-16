// Package sqlguard rejects statements that can modify a database.
//
// The check is lexical, not a full SQL parser: dbbridge accepts four dialects
// (PostgreSQL, MySQL, ClickHouse, Oracle) and no single grammar covers them.
// What it does do is tokenize properly — comments, string literals, quoted and
// backtick identifiers and PostgreSQL dollar quoting are all consumed as such —
// so the classic evasions of a regular expression (a keyword hidden in a
// comment or a literal, a second statement after a semicolon) do not work.
//
// It is a defence in depth measure. The authoritative control is still a target
// database user that only holds read privileges.
package sqlguard

import (
	"strings"
	"unicode"
)

// writeKeywords are statement verbs and clauses that either modify data,
// modify schema, change session state or run arbitrary code. A bare (unquoted)
// occurrence anywhere in the statement is enough to reject it: in every
// supported engine these are reserved words, so a legitimate read-only query
// spells them quoted when it needs them as identifiers.
var writeKeywords = map[string]struct{}{
	"ALTER": {}, "ANALYZE": {}, "ATTACH": {}, "BEGIN": {}, "CALL": {},
	"COMMIT": {}, "COPY": {}, "CREATE": {}, "DEALLOCATE": {}, "DELETE": {},
	"DETACH": {}, "DO": {}, "DROP": {}, "EXEC": {}, "EXECUTE": {},
	"GRANT": {}, "HANDLER": {}, "IMPORT": {}, "INSERT": {}, "INTO": {},
	"KILL": {}, "LOAD": {}, "LOCK": {}, "MERGE": {}, "OPTIMIZE": {},
	"PREPARE": {}, "RENAME": {}, "REPLACE": {}, "REVOKE": {}, "ROLLBACK": {},
	"SAVEPOINT": {}, "SET": {}, "START": {}, "SYSTEM": {}, "TRUNCATE": {},
	"UPDATE": {}, "UPSERT": {}, "USE": {}, "VACUUM": {},
}

// readVerbs are the statement verbs a read-only query may start with.
var readVerbs = map[string]struct{}{
	"SELECT": {}, "WITH": {}, "SHOW": {}, "EXPLAIN": {}, "DESC": {},
	"DESCRIBE": {}, "TABLE": {}, "VALUES": {},
}

// ReadOnly reports whether sql is a single read-only statement. The returned
// error explains which rule rejected it and is safe to show to the caller: it
// never echoes the statement back.
func ReadOnly(sql string) error {
	words, statements := scan(sql)

	if len(words) == 0 {
		return &Error{Reason: "statement is empty"}
	}
	if statements > 1 {
		return &Error{Reason: "multiple statements are not allowed"}
	}

	verb := strings.ToUpper(words[0])
	if _, ok := readVerbs[verb]; !ok {
		return &Error{Reason: "only read-only statements are allowed, got " + verb}
	}

	for _, w := range words {
		upper := strings.ToUpper(w)
		if _, ok := writeKeywords[upper]; ok {
			return &Error{Reason: "only read-only statements are allowed, found " + upper}
		}
	}

	return nil
}

// Error is returned by ReadOnly when a statement is rejected.
type Error struct {
	Reason string
}

func (e *Error) Error() string { return e.Reason }

// scan tokenizes sql into its bare (unquoted, uncommented) words and counts how
// many statements it contains. Everything inside a comment, a string literal or
// a quoted identifier is skipped, so it can never contribute a keyword.
func scan(sql string) (words []string, statements int) {
	src := []rune(sql)
	n := len(src)
	var word strings.Builder
	// A statement is only counted once it has content, so a trailing semicolon
	// (or several) does not read as an extra statement.
	pending := false

	flush := func() {
		if word.Len() > 0 {
			words = append(words, word.String())
			word.Reset()
		}
	}

	for i := 0; i < n; {
		c := src[i]

		switch {
		// Line comments: -- ... and MySQL's # ...
		case c == '-' && i+1 < n && src[i+1] == '-',
			c == '#':
			flush()
			for i < n && src[i] != '\n' {
				i++
			}

		// Block comment, including the nested form PostgreSQL accepts.
		case c == '/' && i+1 < n && src[i+1] == '*':
			flush()
			depth := 1
			i += 2
			for i < n && depth > 0 {
				if src[i] == '/' && i+1 < n && src[i+1] == '*' {
					depth++
					i += 2
					continue
				}
				if src[i] == '*' && i+1 < n && src[i+1] == '/' {
					depth--
					i += 2
					continue
				}
				i++
			}

		// PostgreSQL dollar quoting: $tag$ ... $tag$.
		case c == '$':
			if tag, end, ok := dollarTag(src, i); ok {
				flush()
				pending = true
				i = skipDollarQuoted(src, end, tag)
				continue
			}
			word.WriteRune(c)
			i++

		// String literals and quoted identifiers.
		case c == '\'' || c == '"' || c == '`':
			flush()
			pending = true
			i = skipQuoted(src, i, c)

		case c == ';':
			flush()
			if pending {
				statements++
				pending = false
			}
			i++

		case unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_':
			word.WriteRune(c)
			pending = true
			i++

		default:
			flush()
			if !unicode.IsSpace(c) {
				pending = true
			}
			i++
		}
	}

	flush()
	if pending {
		statements++
	}
	return words, statements
}

// skipQuoted returns the index just past a literal opened at i with quote.
// A doubled quote escapes itself in every supported dialect; a backslash escape
// is additionally honoured for single quotes, which is what MySQL does.
func skipQuoted(src []rune, i int, quote rune) int {
	n := len(src)
	i++ // opening quote
	for i < n {
		if src[i] == '\\' && quote == '\'' && i+1 < n {
			i += 2
			continue
		}
		if src[i] == quote {
			if i+1 < n && src[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return n
}

// dollarTag recognizes a dollar-quote opener at i and returns its tag and the
// index just past it.
func dollarTag(src []rune, i int) (tag string, end int, ok bool) {
	n := len(src)
	j := i + 1
	for j < n && (unicode.IsLetter(src[j]) || unicode.IsDigit(src[j]) || src[j] == '_') {
		j++
	}
	if j < n && src[j] == '$' {
		return string(src[i : j+1]), j + 1, true
	}
	return "", 0, false
}

// skipDollarQuoted returns the index just past the closing tag of a
// dollar-quoted string that starts at i.
func skipDollarQuoted(src []rune, i int, tag string) int {
	rest := string(src[i:])
	if idx := strings.Index(rest, tag); idx >= 0 {
		return i + len([]rune(rest[:idx])) + len([]rune(tag))
	}
	return len(src)
}
