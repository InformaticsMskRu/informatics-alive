ALTER TABLE pynformatics.runs ADD COLUMN IF NOT EXISTS ignore_user_ids tinyint(1) DEFAULT FALSE;
