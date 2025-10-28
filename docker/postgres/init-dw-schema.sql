-- SCHEMA
CREATE SCHEMA IF NOT EXISTS dw;

-- AUTHORS
CREATE TABLE IF NOT EXISTS dw.authors (
    author_id        BIGSERIAL PRIMARY KEY,
    author_name      TEXT NOT NULL UNIQUE
);

-- SECTIONS
CREATE TABLE IF NOT EXISTS dw.sections (
    section_id       BIGSERIAL PRIMARY KEY,
    section_key      TEXT NOT NULL UNIQUE,    -- e.g. "australia-news"
    section_name     TEXT,
    pillar_name      TEXT
);

-- PUBLICATIONS
CREATE TABLE IF NOT EXISTS dw.publications (
    publication_id   BIGSERIAL PRIMARY KEY,
    publication_name TEXT NOT NULL UNIQUE
);

-- ARTICLES (fact / transactional)
CREATE TABLE IF NOT EXISTS dw.articles (
    article_id           TEXT PRIMARY KEY,
    type                 TEXT,
    section_id           BIGINT REFERENCES dw.sections(section_id),
    publication_date     TIMESTAMP WITH TIME ZONE NOT NULL,  -- giữ lại ngày gốc
    title                TEXT,
    headline             TEXT,
    trail_text           TEXT,
    body                 TEXT,
    wordcount            INT,
    publication_id       BIGINT REFERENCES dw.publications(publication_id),
    thumbnail_url        TEXT,
    web_url              TEXT,
    has_thumbnail        BOOLEAN,
    is_live_blog         BOOLEAN,
    topic_country        TEXT,
    ingested_at          TIMESTAMP WITH TIME ZONE,
    source_system        TEXT,
    raw_s3_key           TEXT,
    processed_by         TEXT
);

-- ARTICLE ↔ AUTHOR many-to-many
CREATE TABLE IF NOT EXISTS dw.article_authors (
    article_id   TEXT REFERENCES dw.articles(article_id) ON DELETE CASCADE,
    author_id    BIGINT REFERENCES dw.authors(author_id),
    ord          INT DEFAULT 0,
    PRIMARY KEY (article_id, author_id)
);

-- ARTICLE KEYWORDS (one-to-many)
CREATE TABLE IF NOT EXISTS dw.article_keywords (
    article_id TEXT REFERENCES dw.articles(article_id) ON DELETE CASCADE,
    keyword    TEXT NOT NULL,
    PRIMARY KEY (article_id, keyword)
);

-- Indexes for optimization
CREATE INDEX IF NOT EXISTS idx_articles_pubdate ON dw.articles (publication_date);
CREATE INDEX IF NOT EXISTS idx_articles_section ON dw.articles (section_id);
CREATE INDEX IF NOT EXISTS idx_articles_topic_country ON dw.articles (topic_country);