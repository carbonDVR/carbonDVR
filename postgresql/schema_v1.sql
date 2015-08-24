--
-- PostgreSQL
--

CREATE SCHEMA carbon_v1;
SET SCHEMA 'carbon_v1';

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
  imageurl       text,
  PRIMARY KEY (show_id, episode_id),
  FOREIGN KEY (show_id) REFERENCES show(show_id)
);

CREATE TABLE channel (
  channel_id     text PRIMARY KEY,
  name           text
);

CREATE TABLE schedule (
  schedule_id    SERIAL PRIMARY KEY,
  channel_id     text,
  start_time     timestamp without time zone,
  stop_time      timestamp without time zone,
  show_id        text,
  episode_id     text,
  rerun_code     character(1),
  FOREIGN KEY (channel_d) REFERENCES channel(channel_id),
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
  date_recorded  timestamp without time zone,
  duration       interval,
  category_code  character(1),
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

CREATE TABLE archive_state (
  recording_id   int4 PRIMARY KEY,
  state          integer
  );

-- used in recorder.py
CREATE OR REPLACE VIEW recorded_episodes_by_id AS
  SELECT recording.recording_id, recording.show_id, recording.episode_id
  FROM recording 
  LEFT JOIN file_raw_video ON (recording.recording_id = file_raw_video.recording_id)
  LEFT JOIN file_transcoded_video ON (recording.recording_id = file_transcoded_video.recording_id)
  WHERE file_raw_video.filename IS NOT NULL
  OR file_transcoded_video.filename IS NOT NULL;

-- not used
CREATE OR REPLACE VIEW upcomingrecordings_base AS
  SELECT schedule.schedule_id, schedule.channel_id, schedule.start_time, schedule.show_id, schedule.episode_id
  FROM schedule, subscription
  WHERE schedule.show_id = subscription.show_id
  AND start_time > timezone('utc'::text, now());

-- used in defunct function in trinTVDB.py
CREATE OR REPLACE VIEW upcoming_recordings_by_id_pruned AS
  SELECT DISTINCT ON (schedule.show_id, schedule.episode_id) schedule.schedule_id, schedule.channel_id, schedule.start_time, schedule.show_id, schedule.episode_id
  FROM schedule, subscription
  WHERE schedule.show_id = subscription.show_id
  AND start_time > timezone('utc'::text, now())
  AND (schedule.show_id, schedule.episode_id) NOT IN (SELECT show_id, episode_id FROM recorded_episodes_by_id)
  ORDER BY schedule.show_id, episode_id, start_time;

-- not used
CREATE OR REPLACE VIEW upcoming_recordings AS
  SELECT schedule_id, timezone('utc'::text, v.start_time) AS start_time, channel.name AS channel, "show".name AS "show", episode.title AS episode 
  FROM upcoming_recordings_by_id_pruned v, channel, show, episode
  WHERE v.channel_id = channel.channel_id
  AND v.show_id = show.show_id
  AND v.show_id = episode.show_id
  AND v.episode_id = episode.episode_id
  ORDER BY start_time;

-- not used
CREATE OR REPLACE VIEW recorded_shows AS
  SELECT recording.recording_id, show.name AS show, episode.title AS episode, timezone('utc'::text, recording.date_recorded) AS date_recorded, recording.duration
  FROM recording, show, episode
  WHERE recording.show_id = show.show_id
  AND recording.show_id = episode.show_id
  AND recording.episode_id = episode.episode_id
  AND recording_id IN (SELECT recording_id FROM file_raw_video UNION SELECT recording_id FROM file_transcoded_video)
  ORDER BY date_recorded;

-- not used
CREATE VIEW subscriptions AS
  SELECT show.show_id, show.name, subscription.priority
  FROM subscription, show
  WHERE subscription.show_id = show.show_id
  ORDER BY show.name;

-- not used
CREATE VIEW upcoming_schedule AS
  SELECT s.schedule_id, timezone('utc'::text, s.start_time) AS start_time, (s.stop_time - s.start_time) AS duration, channel.name AS channel, s.show_id, show.name AS show, episode.title AS episode 
  FROM schedule s, channel, show, episode
  WHERE s.channel_id = channel.channel_id
  AND s.show_id = "show".show_id
  AND s.show_id = episode.show_id
  AND s.episode_id = episode.episode_id
  AND s.start_time > timezone('utc'::text, now())
  ORDER BY timezone('utc'::text, s.start_time), (s.stop_time - s.start_time);

