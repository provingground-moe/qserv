--
-- Migration script from version 2 to version 3 of QMeta database:
--   - add resultQuery column to QInfo table
--

ALTER TABLE `QInfo` ADD COLUMN (`resultQuery` TEXT);
