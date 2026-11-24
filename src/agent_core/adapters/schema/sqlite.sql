CREATE TABLE IF NOT EXISTS records (
    scope TEXT NOT NULL, key TEXT NOT NULL, version INTEGER NOT NULL,
    value TEXT NOT NULL, PRIMARY KEY(scope, key));
CREATE TABLE IF NOT EXISTS spaces (
    scope TEXT PRIMARY KEY, space_id TEXT NOT NULL, dimensions INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS memory (
    scope TEXT NOT NULL, task_id TEXT NOT NULL, field TEXT NOT NULL,
    source_id TEXT NOT NULL, text TEXT NOT NULL, vector TEXT NOT NULL, source_hash TEXT NOT NULL,
    UNIQUE(scope, task_id, source_id));
CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(tokens);
CREATE TABLE IF NOT EXISTS file_notes (
    scope TEXT NOT NULL,
    file_id TEXT NOT NULL,
    name TEXT NOT NULL,
    summary TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(scope, file_id));
CREATE INDEX IF NOT EXISTS idx_file_notes_scope_updated
    ON file_notes (scope, updated_at, file_id);
CREATE VIRTUAL TABLE IF NOT EXISTS file_notes_fts USING fts5(name_tokens, summary_tokens);

CREATE TABLE IF NOT EXISTS agent_core_schema (
    id INTEGER PRIMARY KEY CHECK (id=1),
    version INTEGER NOT NULL
);
INSERT INTO agent_core_schema (id,version)
SELECT 1,1 WHERE NOT EXISTS (SELECT 1 FROM agent_core_schema WHERE id=1);
