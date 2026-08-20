package sqlguard

import "testing"

func TestReadOnlyAccepts(t *testing.T) {
	cases := []string{
		"SELECT 1",
		"select id, name from users where name = 'delete from users'",
		"SELECT 1; ",
		"  \n SELECT 1 \n ",
		"WITH t AS (SELECT 1) SELECT * FROM t",
		"SHOW TABLES",
		"EXPLAIN SELECT * FROM t",
		"SELECT * FROM \"update\"",
		"SELECT * FROM `insert`",
		"SELECT 1 -- drop table users",
		"SELECT 1 /* insert into t values (1) */",
		"SELECT 1 /* nested /* insert */ still a comment */",
		"SELECT 'it''s a drop table'",
		"SELECT $tag$ delete from users $tag$",
		"SELECT * FROM t WHERE s = 'a\\'; drop table t; --'",
	}
	for _, sql := range cases {
		if err := ReadOnly(sql); err != nil {
			t.Errorf("ReadOnly(%q) = %v, want nil", sql, err)
		}
	}
}

func TestReadOnlyRejects(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"DELETE FROM users",
		"delete from users",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET a = 1",
		"DROP TABLE users",
		"TRUNCATE t",
		"CREATE TABLE t (a int)",
		"SELECT 1; DROP TABLE users",
		"SELECT 1 ; delete from users ;",
		"SELECT * INTO other FROM t",
		"WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
		"SET search_path = evil",
		"CALL do_bad()",
		"/* comment */ DROP TABLE t",
	}
	for _, sql := range cases {
		if err := ReadOnly(sql); err == nil {
			t.Errorf("ReadOnly(%q) = nil, want an error", sql)
		}
	}
}

// TestReadOnlyErrorDoesNotEchoStatement keeps the rejection message free of the
// submitted SQL: it travels back to the client and into logs.
func TestReadOnlyErrorDoesNotEchoStatement(t *testing.T) {
	err := ReadOnly("DELETE FROM secret_table WHERE token = 'hunter2'")
	if err == nil {
		t.Fatal("ReadOnly accepted a DELETE")
	}
	for _, leak := range []string{"secret_table", "hunter2"} {
		if contains(err.Error(), leak) {
			t.Errorf("error %q leaks %q", err, leak)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
