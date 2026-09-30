-- ejudge's JSON API doesn't return short_name, so problems imported through
-- it have no short id. VARCHAR(100) is the wider of the model definitions;
-- compare with SHOW CREATE TABLE moodle.mdl_ejudge_problem before running.
ALTER TABLE moodle.mdl_ejudge_problem MODIFY short_id VARCHAR(100) NULL DEFAULT NULL;
