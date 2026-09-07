package parser

import (
	"database/sql"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

// OpenCode's V2 reader consumes session_message projections, not the durable
// event log. The producer updates data and time_updated in place, leaving seq
// unchanged. See the pinned producer references in session-format-sources.md.
func openCodeHasProjectionCached(db *sql.DB, path string) (bool, error) {
	state, cacheable := StatSQLiteContainerState(path)
	openCodeSessionSchemaCacheMu.Lock()
	entry, hit := openCodeSessionSchemaCache[path]
	openCodeSessionSchemaCacheMu.Unlock()
	if cacheable && hit && entry.state == state && entry.projectionOnce {
		return entry.hasProjection, nil
	}
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master
		WHERE type = 'table' AND name = 'session_message')`).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		var columns int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('session_message')
			WHERE name IN ('id','session_id','type','seq','time_created','time_updated','data')`).Scan(&columns); err != nil {
			return false, err
		}
		if columns != 7 {
			return false, fmt.Errorf("unsupported opencode session_message schema")
		}
		indexed, err := openCodeTableIndexesColumn(db, "session_message", "session_id")
		if err != nil {
			return false, err
		}
		legacy, err := openCodeSupportsCompositeMtime(db)
		if err != nil {
			return false, err
		}
		if !indexed || !legacy {
			return false, fmt.Errorf("unsupported opencode projection freshness schema or indexes")
		}
	}
	if cacheable {
		openCodeSessionSchemaCacheMu.Lock()
		entry = openCodeSessionSchemaCache[path]
		if entry.state != state {
			entry = openCodeSessionSchemaCacheEntry{state: state}
		}
		entry.hasProjection, entry.projectionOnce = exists, true
		openCodeSessionSchemaCache[path] = entry
		openCodeSessionSchemaCacheMu.Unlock()
	}
	return exists, nil
}

// Extend only the internal freshness queries. Both branches retain their
// session_id indexes for single-session lookups; whole-container discovery
// groups each branch once. No transcript bytes are read by these queries.
func openCodeProjectionFreshnessQuery(db *sql.DB, path, query string, singleSession bool) (string, error) {
	has, err := openCodeHasProjectionCached(db, path)
	if err != nil || !has {
		return query, err
	}
	filter := ""
	if singleSession {
		// SQLite does not push a correlated outer predicate through UNION ALL.
		// Put it in both branches or each single-session poll scans the archive.
		filter = " WHERE session_id = s.id"
	}
	messages := `(SELECT id, session_id, time_updated FROM message` + filter + `
		UNION ALL SELECT 'projection:' || id || ':' || seq || ':' || type AS id,
		session_id, time_updated FROM session_message` + filter + `)`
	return strings.ReplaceAll(query, "FROM message", "FROM "+messages), nil
}

// Prefer a nonempty projection stream per session. Merely creating the new
// table must not hide V1 sessions in the same database. Do not merge the two
// transcripts: projected copies would count the same messages and usage twice.
func parseOpenCodeProjection(
	db *sql.DB, path, sessionID, machine string,
) (*ParsedSession, []ParsedMessage, bool, error) {
	query, err := openCodeProjectionFreshnessQuery(db, path,
		"SELECT "+openCodeSessionCompositeMtimeExpr+", "+openCodeSessionCompositeCountsExpr+
			" FROM session s"+openCodeSessionCompositeMtimeJoins+" WHERE s.id = ?", true)
	if err != nil {
		return nil, nil, false, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, nil, false, err
	}
	defer tx.Rollback()
	var projected bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM session_message
		WHERE session_id = ?)`, sessionID).Scan(&projected); err != nil {
		return nil, nil, false, err
	}
	if !projected {
		return nil, nil, false, nil
	}
	// Pin metadata, content and its fingerprint to one SQLite read snapshot.
	openCodeSessionChildLookups.Add(1)
	var agg openCodeChildAggregate
	if err := tx.QueryRow(query, sessionID).Scan(
		&agg.watermark, &agg.sessionTime, &agg.projectTime, &agg.messages, &agg.parts,
		&agg.messageIdent, &agg.partIdent,
	); err != nil {
		return nil, nil, false, err
	}
	s, err := loadOneOpenCodeSession(tx, sessionID, true)
	if err != nil {
		return nil, nil, false, err
	}
	var worktree string
	if err := tx.QueryRow("SELECT worktree FROM project WHERE id = ?", s.projectID).Scan(&worktree); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, false, err
	}
	cwd := resolveOpenCodeWorktree(s.directory, worktree)
	if !openCodeUsableWorktree(worktree) {
		worktree = cwd
	}
	rows, err := tx.Query(`SELECT id, type, data, time_created FROM session_message
		WHERE session_id = ? ORDER BY seq`, sessionID)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()
	var parsed []ParsedMessage
	found := false
	for rows.Next() {
		found = true
		var id, kind, data string
		var created int64
		if err := rows.Scan(&id, &kind, &data, &created); err != nil {
			return nil, nil, true, err
		}
		m, visible, err := decodeOpenCodeProjection(kind, data, cwd)
		if err != nil {
			return nil, nil, true, fmt.Errorf("decoding opencode projection %s: %w", id, err)
		}
		if !visible {
			continue
		}
		m.Ordinal, m.SourceUUID, m.Timestamp = len(parsed), id, millisToTime(created)
		m.ContentLength = len(m.Content)
		parsed = append(parsed, m)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, found, err
	}
	if err := rows.Close(); err != nil {
		return nil, nil, found, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, found, err
	}
	sess, msgs, err := assembleOpenCodeSession(s, cwd, worktree, path+"#"+s.id,
		agg.watermark*1_000_000, machine, "", parsed)
	if sess != nil {
		sess.File.Hash = agg.digest(true)
	}
	return sess, msgs, found, err
}

type openCodeProjectionContent struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	Name  string `json:"name"`
	Text  string `json:"text"`
	State struct {
		Status  string                     `json:"status"`
		Input   jsontext.Value             `json:"input"`
		Content []openCodeProjectionOutput `json:"content"`
		Error   struct {
			Message string `json:"message"`
		} `json:"error"`
		Structured struct {
			Exit int `json:"exit"`
		} `json:"structured"`
	} `json:"state"`
	Time struct {
		Completed int64 `json:"completed"`
	} `json:"time"`
}

type openCodeProjectionOutput struct {
	Type string `json:"type"`
	Text string `json:"text"`
	Name string `json:"name"`
	Mime string `json:"mime"`
}

func decodeOpenCodeProjection(kind, data, cwd string) (ParsedMessage, bool, error) {
	if !gjson.Parse(data).IsObject() {
		return ParsedMessage{}, false, fmt.Errorf("projection data must be an object")
	}
	var d struct {
		Text    string                      `json:"text"`
		Summary string                      `json:"summary"`
		Command string                      `json:"command"`
		Output  string                      `json:"output"`
		CallID  string                      `json:"callID"`
		Finish  string                      `json:"finish"`
		Files   []openCodeProjectionOutput  `json:"files"`
		Content []openCodeProjectionContent `json:"content"`
		Model   struct {
			ID         string `json:"id"`
			ProviderID string `json:"providerID"`
		} `json:"model"`
		Time struct {
			Completed int64 `json:"completed"`
		} `json:"time"`
	}
	if err := json.Unmarshal([]byte(data), &d); err != nil {
		return ParsedMessage{}, false, err
	}
	m := ParsedMessage{}
	switch kind {
	case "model-switched", "agent-switched":
		return m, false, nil
	case "user", "system", "synthetic":
		m.Role, m.Content, m.IsSystem = RoleUser, d.Text, kind != "user"
		for _, f := range d.Files {
			m.Content = strings.TrimSpace(m.Content + "\n" + openCodeAttachmentLabel(f))
		}
	case "compaction":
		m.Role, m.Content, m.IsSystem, m.IsCompactBoundary = RoleAssistant, d.Summary, true, true
	case "shell":
		m.Role, m.IsSystem, m.Content, m.HasToolUse = RoleUser, true, d.Command, true
		input, err := json.Marshal(map[string]string{"command": d.Command})
		if err != nil {
			return m, false, err
		}
		tc := ParsedToolCall{ToolUseID: d.CallID, ToolName: "bash", Category: NormalizeToolCategory("bash"), InputJSON: string(input)}
		if d.Time.Completed != 0 {
			tc.ResultEvents = []ParsedToolResultEvent{{ToolUseID: d.CallID, Status: "completed", Content: d.Output, Timestamp: millisToTime(d.Time.Completed)}}
		}
		m.ToolCalls = []ParsedToolCall{tc}
	case "assistant":
		m.Role, m.Model, m.ProviderID, m.StopReason = RoleAssistant, d.Model.ID, d.Model.ProviderID, d.Finish
		applyOpenCodeTokenUsage(&m, openCodeMessageData{}, data, nil)
		var texts []string
		for _, c := range d.Content {
			switch c.Type {
			case "text":
				texts = append(texts, c.Text)
			case "reasoning":
				if c.Text != "" {
					m.HasThinking = true
					texts = append(texts, "[Thinking]\n"+c.Text+"\n[/Thinking]")
				}
			case "tool":
				m.HasToolUse = true
				tc, err := openCodeProjectionTool(c, cwd)
				if err != nil {
					return m, false, err
				}
				m.ToolCalls = append(m.ToolCalls, tc)
			default:
				return m, false, fmt.Errorf("unsupported assistant content type %q", c.Type)
			}
		}
		m.Content = strings.Join(texts, "\n")
	default:
		return m, false, fmt.Errorf("unsupported message type %q", kind)
	}
	// Keep usage-only assistant finalizations as well as attachment-only input.
	return m, strings.TrimSpace(m.Content) != "" || m.HasToolUse || len(m.TokenUsage) > 0, nil
}

func openCodeAttachmentLabel(f openCodeProjectionOutput) string {
	name := f.Name
	if name == "" {
		name = f.Mime
	}
	if name == "" {
		name = "file"
	}
	return "[Attachment: " + name + "]"
}

func openCodeProjectionTool(c openCodeProjectionContent, cwd string) (ParsedToolCall, error) {
	tc := ParsedToolCall{ToolUseID: c.ID, ToolName: c.Name, Category: NormalizeToolCategory(c.Name), InputJSON: string(c.State.Input)}
	if c.Name == "skill" {
		tc.SkillName = gjson.Get(tc.InputJSON, "name").String()
		if tc.SkillName == "" {
			tc.SkillName = gjson.Get(tc.InputJSON, "skill").String()
		}
	} else {
		tc.SkillName = inferOpenCodeSkillName(c.Name, tc.InputJSON, cwd)
	}
	status := c.State.Status
	switch status {
	case "pending", "running":
		return tc, nil
	case "error":
		status = "errored"
	case "completed":
		if c.Name == "invalid" || (c.Name == "bash" && c.State.Structured.Exit != 0) {
			status = "errored"
		}
	default:
		return tc, fmt.Errorf("unsupported tool status %q", status)
	}
	var output []string
	for _, item := range c.State.Content {
		switch item.Type {
		case "text":
			output = append(output, item.Text)
		case "file":
			output = append(output, openCodeAttachmentLabel(item))
		default:
			return tc, fmt.Errorf("unsupported tool output type %q", item.Type)
		}
	}
	if c.State.Error.Message != "" {
		output = append(output, c.State.Error.Message)
	}
	tc.ResultEvents = []ParsedToolResultEvent{{ToolUseID: c.ID, Status: status,
		Content: strings.Join(output, "\n"), Timestamp: millisToTime(c.Time.Completed)}}
	return tc, nil
}
