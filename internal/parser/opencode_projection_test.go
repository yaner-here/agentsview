package parser

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This DDL is the relevant subset emitted by the official
// 0.0.0-dev-202609070426 CLI, not a schema inferred from the reader.
const openCodeProjectionSchema = `
CREATE TABLE session_message (
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES session(id),
 type TEXT NOT NULL, seq INTEGER NOT NULL,
 time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL, data TEXT NOT NULL
);
CREATE UNIQUE INDEX session_message_session_seq_idx ON session_message(session_id, seq);
CREATE INDEX session_message_session_type_seq_idx ON session_message(session_id, type, seq);
`

func projectionDB(t *testing.T) (string, *sql.DB, *OpenCodeSeeder) {
	t.Helper()
	path, seed, db := newTestDB(t)
	_, err := db.Exec(openCodeProjectionSchema)
	require.NoError(t, err)
	seed.AddProject("project", "/workspace/example")
	seed.AddSessionDirectory("ses_projection", "project", "", "Projection", "/workspace/example", 1700000000000, 1700000009000)
	return path, db, seed
}

func insertProjection(t *testing.T, db *sql.DB, id, session, kind string, seq, created int64, data string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO session_message (id,session_id,type,seq,time_created,time_updated,data)
		VALUES (?,?,?,?,?,?,?)`, id, session, kind, seq, created, created, data)
	require.NoError(t, err)
}

func TestOpenCodeProjectionProviderMixedTranscripts(t *testing.T) {
	path, db, seed := projectionDB(t)
	seed.AddSession("ses_legacy", "project", "", "Legacy", 1700000000000, 1700000009000)
	seed.AddMessage("legacy", "ses_legacy", 1700000000000, 1700000000000, `{"role":"user"}`)
	seed.AddPart("legacy-part", "legacy", "ses_legacy", 1700000000000, 1700000000000, `{"type":"text","text":"legacy prompt"}`)
	// An old-format copy must not double the projected conversation or usage.
	seed.AddMessage("shadow", "ses_projection", 1700000000000, 1700000000000, `{"role":"user"}`)
	seed.AddPart("shadow-part", "shadow", "ses_projection", 1700000000000, 1700000000000, `{"type":"text","text":"obsolete shadow"}`)
	insertProjection(t, db, "msg_user", "ses_projection", "user", 3, 1700000002000, `{"text":"check the build"}`)
	insertProjection(t, db, "msg_switch", "ses_projection", "model-switched", 4, 1700000003000, `{"model":{"id":"example-model","providerID":"example"}}`)
	// Deliberately older timestamp: seq, not time_created, owns ordering.
	insertProjection(t, db, "msg_assistant", "ses_projection", "assistant", 8, 1700000001000, `{
	 "model":{"id":"example-model","providerID":"example"},
	 "content":[{"type":"text","id":"z","text":"Starting"},
	 {"type":"reasoning","id":"a","text":"Check output"},
	 {"type":"tool","id":"call_build","name":"bash","state":{"status":"completed",
	 "input":{"command":"build"},"structured":{"exit":2},"content":[{"type":"text","text":"build failed"}]},
	 "time":{"completed":1700000004000}}],
	 "tokens":{"input":10,"output":5,"reasoning":2,"cache":{"read":3,"write":4}},"cost":999
	}`)
	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{Roots: []string{filepath.Dir(path)}})
	require.True(t, ok)
	sources, err := provider.Discover(t.Context())
	require.NoError(t, err)
	require.Len(t, sources, 2)
	for _, source := range sources {
		fp, err := provider.Fingerprint(t.Context(), source)
		require.NoError(t, err)
		out, err := provider.Parse(t.Context(), ParseRequest{Source: source, Fingerprint: fp})
		require.NoError(t, err)
		require.Len(t, out.Results, 1)
		result := out.Results[0].Result
		if result.Session.ID == "opencode:ses_legacy" {
			require.Len(t, result.Messages, 1)
			assert.Equal(t, "legacy prompt", result.Messages[0].Content)
			continue
		}
		assert.Equal(t, "opencode:ses_projection", result.Session.ID)
		assert.Equal(t, 1, result.Session.UserMessageCount)
		assert.Equal(t, fp.Hash, result.Session.File.Hash)
		require.Len(t, result.Messages, 2)
		assert.Equal(t, "check the build", result.Messages[0].Content)
		m := result.Messages[1]
		assert.Equal(t, "Starting\n[Thinking]\nCheck output\n[/Thinking]", m.Content)
		assert.Equal(t, 1, m.Ordinal)
		assert.Equal(t, "msg_assistant", m.SourceUUID)
		assert.Equal(t, time.Date(2023, 11, 14, 22, 13, 21, 0, time.UTC), m.Timestamp.UTC())
		assert.True(t, m.HasThinking)
		assert.Equal(t, "example-model", m.Model)
		assert.Equal(t, "example", m.ProviderID)
		assert.Equal(t, 17, m.ContextTokens)
		assert.Equal(t, 5, m.OutputTokens)
		assert.JSONEq(t, `{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":3,"cache_creation_input_tokens":4}`, string(m.TokenUsage))
		require.Len(t, m.ToolCalls, 1)
		tc := m.ToolCalls[0]
		assert.Equal(t, "call_build", tc.ToolUseID)
		assert.JSONEq(t, `{"command":"build"}`, tc.InputJSON)
		require.Len(t, tc.ResultEvents, 1)
		assert.Equal(t, "errored", tc.ResultEvents[0].Status)
		assert.Equal(t, "build failed", tc.ResultEvents[0].Content)
	}
	assert.True(t, OpenCodeSQLiteSessionExists(path, "ses_projection"))
	found, ok, err := provider.FindSource(t.Context(), FindSourceRequest{RawSessionID: "ses_projection"})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, OpenCodeSQLiteVirtualPath(path, "ses_projection"), found.DisplayPath)
}

func TestOpenCodeProjectionUpdatesAndDeletes(t *testing.T) {
	path, db, _ := projectionDB(t)
	insertProjection(t, db, "msg_user", "ses_projection", "user", 1, 1700000000000, `{"text":"prompt"}`)
	insertProjection(t, db, "msg_assistant", "ses_projection", "assistant", 2, 1700000001000, `{"content":[{"type":"text","text":"draft"}]}`)
	before, err := ListOpenCodeSessionMeta(path)
	require.NoError(t, err)
	require.Len(t, before, 1)
	// Below the session watermark, and without changing seq or row count.
	_, err = db.Exec(`UPDATE session_message SET time_updated=1700000003000,
		data='{"content":[{"type":"text","text":"finished"}],"tokens":{"input":0,"output":7}}'
		WHERE id='msg_assistant'`)
	require.NoError(t, err)
	after, err := ListOpenCodeSessionMeta(path)
	require.NoError(t, err)
	assert.Equal(t, before[0].FileMtime, after[0].FileMtime)
	assert.NotEqual(t, before[0].ChildDigest, after[0].ChildDigest)
	_, msgs, err := parseOpenCodeDBSession(path, "ses_projection", "test")
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, "finished", msgs[1].Content)
	assert.Equal(t, 7, msgs[1].OutputTokens)
	assert.True(t, msgs[1].HasContextTokens, "known zero remains known")
	// Above the metadata watermark, the active-session poll must also advance.
	_, err = db.Exec(`UPDATE session_message SET time_updated=1700000020000 WHERE id='msg_assistant'`)
	require.NoError(t, err)
	mtime, err := OpenCodeSourceMtime(path + "#ses_projection")
	require.NoError(t, err)
	assert.Equal(t, int64(1700000020000000000), mtime)
	_, err = db.Exec(`DELETE FROM session_message WHERE id='msg_assistant'`)
	require.NoError(t, err)
	deleted, err := ListOpenCodeSessionMeta(path)
	require.NoError(t, err)
	assert.NotEqual(t, after[0].ChildDigest, deleted[0].ChildDigest)
	_, msgs, err = parseOpenCodeDBSession(path, "ses_projection", "test")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "prompt", msgs[0].Content)
}

func TestOpenCodeProjectionMessageVariants(t *testing.T) {
	for _, tt := range []struct {
		name, kind, data, content string
		system, compact, usage    bool
	}{
		{name: "attachment", kind: "user", data: `{"text":"","files":[{"uri":"data:image/png;base64,AAAA","mime":"image/png","name":"example.png"}]}`, content: "[Attachment: example.png]"},
		{name: "system", kind: "system", data: `{"text":"instructions"}`, content: "instructions", system: true},
		{name: "synthetic", kind: "synthetic", data: `{"text":"generated context"}`, content: "generated context", system: true},
		{name: "compaction", kind: "compaction", data: `{"summary":"summary","recent":"already archived"}`, content: "summary", system: true, compact: true},
		{name: "shell", kind: "shell", data: `{"command":"echo hello","output":"hello","callID":"shell1","time":{"completed":1700000000001}}`, content: "echo hello", system: true},
		{name: "usage only", kind: "assistant", data: `{"content":[],"tokens":{"input":0,"output":0}}`, usage: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path, db, _ := projectionDB(t)
			insertProjection(t, db, "msg_variant", "ses_projection", tt.kind, 1, 1700000000000, tt.data)
			sess, msgs, err := parseOpenCodeDBSession(path, "ses_projection", "test")
			require.NoError(t, err)
			require.NotNil(t, sess)
			require.Len(t, msgs, 1)
			assert.Equal(t, tt.content, msgs[0].Content)
			assert.Equal(t, tt.system, msgs[0].IsSystem)
			assert.Equal(t, tt.compact, msgs[0].IsCompactBoundary)
			assert.Equal(t, tt.usage, msgs[0].HasOutputTokens)
			if tt.system {
				assert.Zero(t, sess.UserMessageCount)
			}
			if tt.kind == "shell" {
				require.Len(t, msgs[0].ToolCalls, 1)
				require.Len(t, msgs[0].ToolCalls[0].ResultEvents, 1)
				assert.Equal(t, "hello", msgs[0].ToolCalls[0].ResultEvents[0].Content)
			}
		})
	}
}

func TestOpenCodeProjectionRejectsUnknownOrMalformedMessages(t *testing.T) {
	for _, tt := range []struct{ name, kind, data string }{
		{"bad JSON", "assistant", "{"},
		{"null data", "assistant", "null"},
		{"bad content", "assistant", `{"content":"not an array"}`},
		{"unknown message", "future-message", `{}`},
		{"unknown content", "assistant", `{"content":[{"type":"future-content"}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path, db, _ := projectionDB(t)
			insertProjection(t, db, "msg_bad", "ses_projection", tt.kind, 1, 1700000000000, tt.data)
			_, _, err := parseOpenCodeDBSession(path, "ses_projection", "test")
			require.ErrorContains(t, err, "decoding opencode projection")
		})
	}
}

func TestOpenCodeProjectionToolFinalizationKeepsSnapshotFingerprint(t *testing.T) {
	path, db, _ := projectionDB(t)
	insertProjection(t, db, "msg_tool", "ses_projection", "assistant", 7, 1700000001000,
		`{"content":[{"type":"tool","id":"call1","name":"read","state":{"status":"running","input":{"file_path":"example.txt"},"structured":{},"content":[]}}]}`)
	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{Roots: []string{filepath.Dir(path)}})
	require.True(t, ok)
	source, ok, err := provider.FindSource(t.Context(), FindSourceRequest{RawSessionID: "ses_projection"})
	require.NoError(t, err)
	require.True(t, ok)
	oldFP, err := provider.Fingerprint(t.Context(), source)
	require.NoError(t, err)
	initial, err := provider.Parse(t.Context(), ParseRequest{Source: source, Fingerprint: oldFP})
	require.NoError(t, err)
	require.Len(t, initial.Results, 1)
	require.Len(t, initial.Results[0].Result.Messages, 1)
	require.Len(t, initial.Results[0].Result.Messages[0].ToolCalls, 1)
	assert.Empty(t, initial.Results[0].Result.Messages[0].ToolCalls[0].ResultEvents)
	_, err = db.Exec(`UPDATE session_message SET time_updated=1700000010000,
		data='{"content":[{"type":"tool","id":"call1","name":"read","state":{"status":"completed","input":{"file_path":"example.txt"},"structured":{},"content":[{"type":"text","text":"file contents"}]},"time":{"completed":1700000010000}}]}'`)
	require.NoError(t, err)
	newFP, err := provider.Fingerprint(t.Context(), source)
	require.NoError(t, err)
	// Parse after discovery raced the writer: it must stamp its own snapshot.
	out, err := provider.Parse(t.Context(), ParseRequest{Source: source, Fingerprint: oldFP})
	require.NoError(t, err)
	require.Len(t, out.Results, 1)
	result := out.Results[0].Result
	assert.Equal(t, newFP.Hash, result.Session.File.Hash)
	assert.NotEqual(t, oldFP.Hash, result.Session.File.Hash)
	assert.Equal(t, newFP.MTimeNS, result.Session.File.Mtime)
	require.Len(t, result.Messages, 1)
	require.Len(t, result.Messages[0].ToolCalls, 1)
	require.Len(t, result.Messages[0].ToolCalls[0].ResultEvents, 1)
	assert.Equal(t, "completed", result.Messages[0].ToolCalls[0].ResultEvents[0].Status)
	assert.Equal(t, "file contents", result.Messages[0].ToolCalls[0].ResultEvents[0].Content)
}

func TestOpenCodeProjectionSchemaAppearsAfterDiscovery(t *testing.T) {
	path, seed, db := newTestDB(t)
	seed.AddProject("project", "/workspace/example")
	seed.AddSession("ses_upgrade", "project", "", "Upgrade", 1700000000000, 1700000009000)
	before, err := ListOpenCodeSessionMeta(path)
	require.NoError(t, err)
	require.Len(t, before, 1)
	_, err = db.Exec(openCodeProjectionSchema)
	require.NoError(t, err)
	insertProjection(t, db, "msg_upgrade", "ses_upgrade", "user", 1, 1700000001000, `{"text":"new projection"}`)
	after, err := ListOpenCodeSessionMeta(path)
	require.NoError(t, err)
	require.Len(t, after, 1)
	assert.Equal(t, before[0].FileMtime, after[0].FileMtime)
	assert.NotEqual(t, before[0].ChildDigest, after[0].ChildDigest)
	_, msgs, err := parseOpenCodeDBSession(path, "ses_upgrade", "test")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "new projection", msgs[0].Content)
}

func TestOpenCodeProjectionSingleSessionQueriesUseIndexes(t *testing.T) {
	path, db, _ := projectionDB(t)
	for _, query := range []string{
		"SELECT " + openCodeSessionCompositeMtimeExpr + " FROM session s" + openCodeSessionCompositeMtimeJoins + " WHERE s.id = ?",
		"SELECT " + openCodeSessionCompositeCountsExpr + " FROM session s" + openCodeSessionCompositeMtimeJoins + " WHERE s.id = ?",
	} {
		query, err := openCodeProjectionFreshnessQuery(db, path, query, true)
		require.NoError(t, err)
		rows, err := db.Query("EXPLAIN QUERY PLAN "+query, "ses_projection")
		require.NoError(t, err)
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			plan = append(plan, detail)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		assert.NotContains(t, strings.Join(plan, "\n"), "SCAN session_message", "polls must not scan unrelated projections")
		assert.Contains(t, strings.Join(plan, "\n"), "SEARCH session_message USING INDEX", "projection freshness must use session_id")
	}
}
