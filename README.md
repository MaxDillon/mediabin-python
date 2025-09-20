# Storage Spec
- Ledger file contains tables `metadata`, `media.media`, `media.tags`
- Metadata contains info fullpath of datadir
- Datadir has a 3 level deep directory structure defined from a hex encoded hash of the media id
    - First two directories are from first 4 bytes e.g. `SX/C5/SXC5CK...`
    - Files stored are video, metadata, preview (optional), thumbnail (optional)
- Id can be any string but is meant for disambiguation. 
    - When the source is yt-dlp, id is `{extractor}__{id}`
- Tags table used as catchall for filtering; Has a schema of (resource_id, tag_id)
- Tags can be prefixed with a tag domain e.g. 
    - `actor:martin_short`
    - `category:comedy`
- Resource Id is also prefixed with a table name e.g. `media:...` to allow for other esources to be tagged in the future
    - `(actor:martin_short, gender:male)`

```sql
CREATE TABLE media.media (
  id            TEXT PRIMARY KEY, -- base64 encoded any string

  title         TEXT NOT NULL,
  description   TEXT,
  origin_url    TEXT,
  video_url     TEXT,
  thumbnail_url TEXT,

  timestamp_created   INTEGER,              -- Date of publication
  timestamp_installed INTEGER NOT NULL,     -- Date first added to library
  timestamp_updated   INTEGER NOT NULL,     -- Date last change was made

  object_path TEXT NOT NULL -- ex. SX/C5/SXC5CK...
);

CREATE TABLE media.tags (
  resource_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  UNIQUE(resource_id, tag),
  FOREIGN KEY(resource_id) REFERENCES media(id) ON DELETE CASCADE,
  FOREIGN KEY(resource_id) REFERENCES actors(id) ON DELETE CASCADE
);

CREATE TABLE metadata(
    datadir_location TEXT -- Path to folder holding all media directories
);
```


# TODO:
- [ ] In metadata table, specify how many allowable concurrent downloads
- [ ] prevent status callback from writing directly to db. Instead enqueue for worker thread to handle
- [ ] create keepalive duckdb connection for worker thread to use
- [ ] create new connections as needed in cli commands 