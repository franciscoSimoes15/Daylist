-- === RAW TABLES ===
CREATE EXTERNAL TABLE raw_recording (id STRING, gid STRING, name STRING, artist_credit STRING, length STRING, comment STRING, edits_pending STRING, last_updated STRING, video STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/recording';

CREATE EXTERNAL TABLE raw_artist (id STRING, gid STRING, name STRING, sort_name STRING, begin_date_year STRING, begin_date_month STRING, begin_date_day STRING, end_date_year STRING, end_date_month STRING, end_date_day STRING, type STRING, area STRING, gender STRING, comment STRING, edits_pending STRING, last_updated STRING, ended STRING, begin_area STRING, end_area STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/artist';

CREATE EXTERNAL TABLE raw_artist_credit (id STRING, name STRING, artist_count STRING, ref_count STRING, created STRING, edits_pending STRING, gid STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/artist_credit';

CREATE EXTERNAL TABLE raw_artist_credit_name (artist_credit STRING, position STRING, artist STRING, name STRING, join_phrase STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/artist_credit_name';

CREATE EXTERNAL TABLE raw_release_group (id STRING, gid STRING, name STRING, artist_credit STRING, type STRING, comment STRING, edits_pending STRING, last_updated STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/release_group';

CREATE EXTERNAL TABLE raw_release (id STRING, gid STRING, name STRING, artist_credit STRING, release_group STRING, status STRING, packaging STRING, language STRING, script STRING, barcode STRING, comment STRING, edits_pending STRING, quality STRING, last_updated STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/release';

CREATE EXTERNAL TABLE raw_medium (id STRING, release STRING, position STRING, format STRING, name STRING, edits_pending STRING, last_updated STRING, track_count STRING, gid STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/medium';

CREATE EXTERNAL TABLE raw_track (id STRING, gid STRING, recording STRING, medium STRING, position STRING, number STRING, name STRING, artist_credit STRING, length STRING, edits_pending STRING, last_updated STRING, is_data_track STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/track';

CREATE EXTERNAL TABLE raw_tag (id STRING, name STRING, ref_count STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/tag';

CREATE EXTERNAL TABLE raw_recording_tag (recording STRING, tag STRING, count STRING, last_updated STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/recording_tag';

CREATE EXTERNAL TABLE raw_artist_tag (artist STRING, tag STRING, count STRING, last_updated STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/artist_tag';

CREATE EXTERNAL TABLE raw_l_recording_recording (id STRING, link STRING, entity0 STRING, entity1 STRING, edits_pending STRING, last_updated STRING, link_order STRING, entity0_credit STRING, entity1_credit STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/l_recording_recording';

CREATE EXTERNAL TABLE raw_users (user_id STRING, username STRING, age STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/users';

CREATE EXTERNAL TABLE raw_friends (user_id STRING, friend_id STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/friends';

CREATE EXTERNAL TABLE raw_fav_artists (user_id STRING, artist_id STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/fav_artists';

CREATE EXTERNAL TABLE raw_history (user_id STRING, recording_id STRING, `timestamp` STRING, time_of_day STRING, duration_ms STRING, completed STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/listening_history';

CREATE EXTERNAL TABLE raw_streaming_events (event_type STRING, user_id STRING, recording_id STRING, ts STRING, duration_ms STRING, position_ms STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/streaming_events';

CREATE EXTERNAL TABLE raw_notifications (user_id STRING, artist_id STRING, release_id STRING, release_name STRING, notified_at STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/notifications';

CREATE EXTERNAL TABLE raw_listening_history_real (user_id STRING, user_name STRING, `timestamp` STRING, track_name STRING, artist_name STRING, release_name STRING, recording_mbid STRING, duration_ms STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/listening_history_real';

CREATE EXTERNAL TABLE raw_lb_recording_mbids (mbid STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/aulas/francisco_jose_simoes/project/data/lb_recording_mbids';

CREATE EXTERNAL TABLE raw_implicit (
  user_id         STRING,
  recording_id    STRING,
  plays           STRING,
  avg_duration    STRING,
  completion_rate STRING,
  implicit_score  STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
LOCATION '/aulas/francisco_jose_simoes/project/data/implicit';

CREATE EXTERNAL TABLE raw_tod_profile (
  user_id      STRING,
  recording_id STRING,
  time_of_day  STRING,
  play_count   STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
LOCATION '/aulas/francisco_jose_simoes/project/data/tod_profile';

CREATE EXTERNAL TABLE raw_friend_history (
  user_id      STRING,
  recording_id STRING,
  friend_plays STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
LOCATION '/aulas/francisco_jose_simoes/project/data/friend_history';