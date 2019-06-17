USE pynformatics;

ALTER TABLE runs add COLUMN context_id int(11) DEFAULT NULL;
ALTER TABLE runs add COLUMN context_source int(11) DEFAULT NULL;
ALTER TABLE runs add COLUMN is_visible tinyint(1) DEFAULT NULL;

