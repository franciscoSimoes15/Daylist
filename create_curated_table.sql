-- === CURATED TABLES ===
CREATE TABLE curated_recording STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, gid, name, CAST(artist_credit AS BIGINT) AS artist_credit, CAST(length AS BIGINT) AS length, comment, CAST(edits_pending AS INT) AS edits_pending, last_updated, CAST(video AS INT) AS video FROM raw_recording WHERE id IS NOT NULL AND artist_credit IS NOT NULL AND name != '' AND lower(name) != 'unknown' AND lower(name) != '[unknown]';

CREATE TABLE curated_artist STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, gid, name, sort_name, CAST(begin_date_year AS INT) AS begin_date_year, CAST(begin_date_month AS INT) AS begin_date_month, CAST(begin_date_day AS INT) AS begin_date_day, CAST(end_date_year AS INT) AS end_date_year, CAST(end_date_month AS INT) AS end_date_month, CAST(end_date_day AS INT) AS end_date_day, CAST(type AS INT) AS type, CAST(area AS INT) AS area, CAST(gender AS INT) AS gender, comment, CAST(edits_pending AS INT) AS edits_pending, last_updated, CAST(ended AS BOOLEAN) AS ended, CAST(begin_area AS INT) AS begin_area, CAST(end_area AS INT) AS end_area FROM raw_artist WHERE id IS NOT NULL AND name != '' AND lower(name) != 'unknown' AND lower(name) != '[unknown]';

CREATE TABLE curated_artist_credit STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, name, CAST(artist_count AS INT) AS artist_count, CAST(ref_count AS INT) AS ref_count, created, CAST(edits_pending AS INT) AS edits_pending, gid FROM raw_artist_credit;

CREATE TABLE curated_artist_credit_name STORED AS ORC AS SELECT CAST(artist_credit AS BIGINT) AS artist_credit, CAST(position AS INT) AS position, CAST(artist AS BIGINT) AS artist, name, join_phrase FROM raw_artist_credit_name;

CREATE TABLE curated_release_group STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, gid, name, CAST(artist_credit AS BIGINT) AS artist_credit, CAST(type AS INT) AS type, comment, CAST(edits_pending AS INT) AS edits_pending, last_updated FROM raw_release_group;

CREATE TABLE curated_release STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, gid, name, CAST(artist_credit AS BIGINT) AS artist_credit, CAST(release_group AS BIGINT) AS release_group, CAST(status AS INT) AS status, CAST(packaging AS INT) AS packaging, CAST(language AS INT) AS language, CAST(script AS INT) AS script, barcode, comment, CAST(edits_pending AS INT) AS edits_pending, CAST(quality AS INT) AS quality, last_updated FROM raw_release;

CREATE TABLE curated_medium STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, CAST(release AS BIGINT) AS release, CAST(position AS INT) AS position, CAST(format AS INT) AS format, name, CAST(edits_pending AS INT) AS edits_pending, last_updated, CAST(track_count AS INT) AS track_count, gid FROM raw_medium;

CREATE TABLE curated_track STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, gid, CAST(recording AS BIGINT) AS recording, CAST(medium AS BIGINT) AS medium, CAST(position AS INT) AS position, number, name, CAST(artist_credit AS BIGINT) AS artist_credit, CAST(length AS BIGINT) AS length, CAST(edits_pending AS INT) AS edits_pending, last_updated, CAST(is_data_track AS BOOLEAN) AS is_data_track FROM raw_track;

CREATE TABLE curated_tag STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, name, CAST(ref_count AS INT) AS ref_count FROM raw_tag;

CREATE TABLE curated_recording_tag STORED AS ORC AS SELECT CAST(recording AS BIGINT) AS recording, CAST(tag AS BIGINT) AS tag, CAST(count AS INT) AS count, last_updated FROM raw_recording_tag;

CREATE TABLE curated_artist_tag STORED AS ORC AS SELECT CAST(artist AS BIGINT) AS artist, CAST(tag AS BIGINT) AS tag, CAST(count AS INT) AS count, last_updated FROM raw_artist_tag;

CREATE TABLE curated_l_recording_recording STORED AS ORC AS SELECT CAST(id AS BIGINT) AS id, CAST(link AS BIGINT) AS link, CAST(entity0 AS BIGINT) AS entity0, CAST(entity1 AS BIGINT) AS entity1, CAST(edits_pending AS INT) AS edits_pending, last_updated, CAST(link_order AS INT) AS link_order, entity0_credit, entity1_credit FROM raw_l_recording_recording;

CREATE TABLE curated_users STORED AS ORC AS SELECT CAST(user_id AS BIGINT) AS user_id, username, CAST(age AS INT) AS age, country FROM raw_users WHERE user_id IS NOT NULL AND username != '' AND username != 'username' AND lower(username) != 'unknown';

CREATE TABLE curated_friends STORED AS ORC AS SELECT CAST(user_id AS BIGINT) AS user_id, CAST(friend_id AS BIGINT) AS friend_id FROM raw_friends;

CREATE TABLE curated_fav_artists STORED AS ORC AS SELECT CAST(user_id AS BIGINT) AS user_id, CAST(artist_id AS BIGINT) AS artist_id FROM raw_fav_artists;

CREATE TABLE curated_history STORED AS ORC AS SELECT CAST(user_id AS BIGINT) AS user_id, CAST(recording_id AS BIGINT) AS recording_id, `timestamp`, time_of_day, CAST(duration_ms AS BIGINT) AS duration_ms, CAST(completed AS BOOLEAN) AS completed, CAST(NULL AS STRING) AS event_id FROM raw_history WHERE user_id IS NOT NULL AND recording_id IS NOT NULL;

CREATE TABLE curated_streaming_events STORED AS ORC AS SELECT CAST(NULL AS STRING) AS event_id, event_type, CAST(user_id AS BIGINT) AS user_id, CAST(recording_id AS BIGINT) AS recording_id, ts, CAST(duration_ms AS BIGINT) AS duration_ms, CAST(position_ms AS BIGINT) AS position_ms FROM raw_streaming_events;

CREATE TABLE curated_notifications STORED AS ORC AS SELECT CAST(user_id AS BIGINT) AS user_id, CAST(artist_id AS BIGINT) AS artist_id, CAST(release_id AS BIGINT) AS release_id, release_name, notified_at FROM raw_notifications;

CREATE TABLE curated_listening_history_real STORED AS ORC AS SELECT CAST(user_id AS BIGINT) AS user_id, user_name, `timestamp`, track_name, artist_name, release_name, recording_mbid, CAST(duration_ms AS BIGINT) AS duration_ms FROM raw_listening_history_real;

CREATE TABLE curated_lb_recording_mbids STORED AS ORC AS SELECT mbid FROM raw_lb_recording_mbids;


SELECT COUNT(DISTINCT h.recording_id) FROM curated_history h
LEFT JOIN curated_recording r ON h.recording_id = r.id
WHERE r.id IS NULL;
-- if > 0 → missing recordings in curated table

CREATE TABLE curated_implicit STORED AS ORC AS
SELECT
  CAST(user_id         AS BIGINT) AS user_id,
  CAST(recording_id    AS BIGINT) AS recording_id,
  CAST(plays           AS INT)    AS plays,
  CAST(avg_duration    AS DOUBLE) AS avg_duration,
  CAST(completion_rate AS DOUBLE) AS completion_rate,
  CAST(implicit_score  AS DOUBLE) AS implicit_score
FROM raw_implicit
WHERE user_id IS NOT NULL AND recording_id IS NOT NULL;

CREATE TABLE curated_tod_profile STORED AS ORC AS
SELECT
  CAST(user_id      AS BIGINT) AS user_id,
  CAST(recording_id AS BIGINT) AS recording_id,
  time_of_day,
  CAST(play_count   AS INT)    AS play_count
FROM raw_tod_profile
WHERE user_id IS NOT NULL;

CREATE TABLE curated_friend_history STORED AS ORC AS
SELECT
  CAST(user_id      AS BIGINT) AS user_id,
  CAST(recording_id AS BIGINT) AS recording_id,
  CAST(friend_plays AS INT)    AS friend_plays
FROM raw_friend_history
WHERE user_id IS NOT NULL AND recording_id IS NOT NULL;
