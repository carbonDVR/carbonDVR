-- 
-- PostgreSQL
--

CREATE SCHEMA carbon_v2;
SET SCHEMA 'carbon_v2';

CREATE SEQUENCE uniqueid;

CREATE TABLE show (
  show_id        text PRIMARY KEY,
  show_type      character(2),
  name           text,
  imageurl       text
);

CREATE TABLE episode (
  show_id        text,
  episode_id     text,
  title          text,
  description    text,
  part_code      text,
  imageurl       text,
  PRIMARY KEY (show_id, episode_id),
  FOREIGN KEY (show_id) REFERENCES show(show_id)
);

CREATE TABLE channel (
  major          integer,
  minor          integer,
  actual         integer,
  program        integer,
  PRIMARY KEY (major, minor)
  );

CREATE TABLE tuner (
  device_id      text,
  ipaddress      inet,
  tuner_id       integer
  );

CREATE TABLE schedule (
  schedule_id    SERIAL PRIMARY KEY,
  channel_major  integer,
  channel_minor  integer,
  start_time     timestamp with time zone,
  duration       interval,
  show_id        text,
  episode_id     text,
  rerun_code     character(1),
  FOREIGN KEY (channel_major, channel_minor) REFERENCES channel(major, minor),
  FOREIGN KEY (show_id, episode_id) REFERENCES episode(show_id, episode_id)
  );

CREATE TABLE subscription (
  show_id        text PRIMARY KEY,
  priority       integer
  );

CREATE TABLE recording_state (
  state          integer,
  description    text
  );

CREATE TABLE recording (
  recording_id   int4 PRIMARY KEY,
  show_id        text,
  episode_id     text,
  date_recorded  timestamp with time zone,
  duration       interval,
  rerun_code     character(1),
  FOREIGN KEY (show_id, episode_id) REFERENCES episode(show_id, episode_id)
  );

CREATE TABLE file_raw_video (
  recording_id   int4 PRIMARY KEY,
  filename       text
  );

CREATE TABLE file_transcoded_video (
  recording_id   int4 PRIMARY KEY,
  filename       text,
  state          int
  );

CREATE TABLE file_bif (
  recording_id   int4 PRIMARY KEY,
  filename       text
  );

CREATE TABLE playback_position (
  recording_id   int4 PRIMARY KEY,
  position       int4
  );

CREATE OR REPLACE VIEW recorded_episodes_by_id AS
  SELECT recording.recording_id, recording.show_id, recording.episode_id
  FROM recording
  LEFT JOIN file_raw_video ON (recording.recording_id = file_raw_video.recording_id)
  LEFT JOIN file_transcoded_video ON (recording.recording_id = file_transcoded_video.recording_id)
  WHERE file_raw_video.filename IS NOT NULL
  OR file_transcoded_video.filename IS NOT NULL;

