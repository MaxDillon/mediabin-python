-- Initial schema migration (up)

CREATE SCHEMA IF NOT EXISTS media;

CREATE TABLE media.media (
  id            TEXT PRIMARY KEY, -- base64 encoded any string

  title         TEXT NOT NULL,
  description   TEXT,
  origin_url    TEXT,
  video_url     TEXT,
  thumbnail_url TEXT,

  timestamp_created   TIMESTAMP,              -- Date of publication
  timestamp_installed TIMESTAMP,     -- Date first added to library
  timestamp_updated   TIMESTAMP,     -- Date last change was made

  status TEXT NOT NULL,

  object_path TEXT -- ex. SX/C5/SXC5CK...
);

CREATE TABLE media.tags (
  resource_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  UNIQUE(resource_id, tag),
  FOREIGN KEY(resource_id) REFERENCES media.media(id)
);

CREATE TABLE metadata (
  datadir_location TEXT PRIMARY KEY -- Path to folder holding all media directories
);
