USE pynformatics;
ALTER TABLE runs add COLUMN source_hash varchar(32);
ALTER TABLE runs add COLUMN ejudge_url varchar(50);

-- Добавить ключ на таблицу с комментариями для связи с pynformatics.runs
USE ejudge;
ALTER TABLE mdl_run_comments ADD COLUMN py_run_id INTEGER;
UPDATE mdl_run_comments AS cmt INNER JOIN (
    SELECT id, ej_run_id, ej_contest_id FROM pynformatics.runs
) AS run
ON run.ej_contest_id = cmt.contest_id AND run.ej_run_id = cmt.run_id
SET cmt.py_run_id =run.id;

-- Мигрировать run-ы из ejudge.runs в pynformatics.runs
USE ejudge;
-- Нагружает одно ядро на 100%
-- IO > 80%
-- Отработало примерно за 40 минут
-- Создано 1'500'000 записей
INSERT INTO pynformatics.runs (
    ej_run_id,
    ej_contest_id,
    ej_score,
    ej_status,
    ej_lang_id,
    ej_test_num,
    ej_create_time,
    ej_last_change_time
) SELECT
    ejruns.run_id,
    ejruns.contest_id,
    ejruns.score,
    ejruns.status,
    ejruns.lang_id,
    ejruns.test_num,
    ejruns.create_time,
    ejruns.last_change_time
FROM ejudge.runs as ejruns
LEFT JOIN (
    SELECT id, ej_run_id, ej_contest_id FROM pynformatics.runs
) AS run
ON run.ej_contest_id = ejruns.contest_id
   AND run.ej_run_id = ejruns.run_id
WHERE run.id IS NULL; -- ejudge runs where pynformatics.runs are not exists

UPDATE pynformatics.runs AS pyruns
SET pyruns.create_time = pyruns.ej_create_time
WHERE pyruns.problem_id IS NULL;

UPDATE pynformatics.runs AS pyruns
SET pyruns.problem_id = (
  SELECT mproblem.id
  FROM moodle.mdl_ejudge_problem AS ejproblem
  LEFT JOIN ejudge.runs as ejruns -- Join лишний
        ON ejproblem.problem_id = ejruns.prob_id
           AND ejproblem.ejudge_contest_id = ejruns.contest_id
  INNER JOIN moodle.mdl_problems AS mproblem
        ON ejproblem.id = mproblem.pr_id
  WHERE pyruns.ej_contest_id = ejruns.contest_id
        AND pyruns.ej_run_id = ejruns.run_id
        AND mproblem.pr_id IS NOT NULL AND mproblem.pr_id != 0 LIMIT 1
)
WHERE pyruns.problem_id is NULL;

-- Create CacheMeta
USE pynformatics;
CREATE TABLE cache_meta (
  id SERIAL PRIMARY KEY,
  prefix VARCHAR(30) NOT NULL,
  label VARCHAR (30) NOT NULL,
  `key` VARCHAR(64) NOT NULL,
  invalidate_args VARCHAR(4096) NOT NULL,
  created TIMESTAMP,
  when_expire TIMESTAMP
);

-- Create monitor links
USE pynformatics;
CREATE TABLE monitor_link (
  id SERIAL PRIMARY KEY,
  author_id INTEGER NOT NULL,
  link VARCHAR(20) NOT NULL,
  internal_link VARCHAR(4096) NOT NULL
);

USE pynformatics;
CREATE TABLE monitor_cache_meta (
  id SERIAL PRIMARY KEY,
  prefix VARCHAR(30) NOT NULL,
  label VARCHAR (30) NOT NULL,
  problem_id INTEGER,
  `key` VARCHAR(64) NOT NULL,
  invalidate_args VARCHAR(4096) NOT NULL,
  created TIMESTAMP,
  when_expire TIMESTAMP
);


USE pynformatics;
CREATE TABLE rejudge (
  id SERIAL PRIMARY KEY,
  run_id  INTEGER,
  ejudge_contest_id INTEGER,
  ejudge_url VARCHAR(50) NOT NULL
)