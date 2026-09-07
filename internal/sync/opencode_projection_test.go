package sync_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

func TestOpenCodeProjectionSyncRefreshAndIsolation(t *testing.T) {
	for _, count := range []int{2, 100} {
		t.Run(fmt.Sprintf("sessions_%d", count), func(t *testing.T) {
			env := setupSingleAgentTestEnv(t, parser.AgentOpenCode)
			oc := createOpenCodeDB(t, env.opencodeDir)
			oc.addProject(t, "project", "/workspace/example")
			// Upgrade the shared legacy fixture to the official projection layout.
			oc.mustExec(t, "add projection schema", `
				ALTER TABLE session ADD COLUMN directory TEXT NOT NULL DEFAULT '';
				CREATE TABLE session_message (
				 id TEXT PRIMARY KEY, session_id TEXT NOT NULL, type TEXT NOT NULL,
				 seq INTEGER NOT NULL, time_created INTEGER NOT NULL,
				 time_updated INTEGER NOT NULL, data TEXT NOT NULL);
				CREATE UNIQUE INDEX session_message_session_seq_idx ON session_message(session_id,seq);`)
			for i := range count {
				id := fmt.Sprintf("ses_%03d", i)
				oc.addSession(t, id, "project", 1700000000000, 1700000009000)
				oc.mustExec(t, "insert user", `INSERT INTO session_message VALUES (?,?,?,?,?,?,?)`,
					id+"_user", id, "user", 1, 1700000000000, 1700000000000, `{"text":"prompt"}`)
				oc.mustExec(t, "insert assistant", `INSERT INTO session_message VALUES (?,?,?,?,?,?,?)`,
					id+"_assistant", id, "assistant", 2, 1700000001000, 1700000001000,
					`{"content":[{"type":"text","text":"draft"}]}`)
			}
			first := env.engine.SyncAll(t.Context(), nil)
			require.False(t, first.Aborted)
			require.Equal(t, count, first.Synced)
			assertMessageContent(t, env.db, "opencode:ses_000", "prompt", "draft")
			require.Zero(t, env.engine.SyncAll(t.Context(), nil).Synced, "unchanged projections should skip parsing")
			// The session's watermark, seq and number of rows are unchanged.
			oc.mustExec(t, "finish assistant", `UPDATE session_message SET time_updated=1700000003000,
				data='{"content":[{"type":"text","text":"finished"}],"tokens":{"input":0,"output":9}}'
				WHERE id='ses_000_assistant'`)
			second := newOpenCodeTestEngine(t, env).SyncAll(t.Context(), nil)
			require.False(t, second.Aborted)
			assert.Equal(t, 1, second.Synced, "only the updated projection must be written")
			assertMessageContent(t, env.db, "opencode:ses_000", "prompt", "finished")
			assertMessageContent(t, env.db, "opencode:ses_001", "prompt", "draft")
			messages := fetchMessages(t, env.db, "opencode:ses_000")
			require.Len(t, messages, 2)
			assert.Equal(t, 9, messages[1].OutputTokens)
			oc.mustExec(t, "delete assistant", `DELETE FROM session_message WHERE id='ses_000_assistant'`)
			third := newOpenCodeTestEngine(t, env).SyncAll(t.Context(), nil)
			require.False(t, third.Aborted)
			assert.Equal(t, 1, third.Synced)
			assertMessageContent(t, env.db, "opencode:ses_000", "prompt")
			oc.mustExec(t, "write malformed projection", `UPDATE session_message
				SET time_updated=1700000020000, data='{' WHERE id='ses_000_user'`)
			failed := newOpenCodeTestEngine(t, env).SyncAll(t.Context(), nil)
			assert.Zero(t, failed.Synced)
			assertMessageContent(t, env.db, "opencode:ses_000", "prompt")
		})
	}
}
