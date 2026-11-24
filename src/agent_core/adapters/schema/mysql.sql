CREATE TABLE IF NOT EXISTS records (
    scope VARBINARY(1024) NOT NULL,
    `key` VARBINARY(768) NOT NULL,
    version BIGINT NOT NULL,
    value JSON NOT NULL,
    PRIMARY KEY(scope, `key`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS spaces (
    scope VARBINARY(1024) PRIMARY KEY,
    space_id VARCHAR(255) NOT NULL,
    dimensions INT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS memory (
    scope VARBINARY(1024) NOT NULL,
    task_id VARCHAR(32) NOT NULL,
    field VARCHAR(64) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    text LONGTEXT NOT NULL,
    `vector` JSON NOT NULL,
    source_hash VARCHAR(128) NOT NULL,
    PRIMARY KEY(scope, task_id, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS file_notes (
    scope VARBINARY(1024) NOT NULL,
    file_id VARCHAR(255) NOT NULL,
    name VARCHAR(200) NOT NULL,
    summary TEXT NOT NULL,
    updated_at VARCHAR(64) NOT NULL,
    PRIMARY KEY(scope, file_id),
    INDEX idx_file_notes_scope_updated (scope, updated_at, file_id),
    FULLTEXT INDEX ft_file_notes_name (name) WITH PARSER ngram,
    FULLTEXT INDEX ft_file_notes_summary (summary) WITH PARSER ngram
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS agent_core_schema (
    id INT PRIMARY KEY,
    version INT NOT NULL
) ENGINE=InnoDB;
INSERT INTO agent_core_schema (id,version)
SELECT 1,1 WHERE NOT EXISTS (SELECT 1 FROM agent_core_schema WHERE id=1);
