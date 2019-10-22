CREATE TABLE IF NOT EXISTS urls (
	`id` integer primary key,
	`url` text not null,
	`status` integer unsigned null,
	`level` integer unsigned not null,
	`body` text null
);

CREATE INDEX IF NOT EXISTS `IDX_urls_url` ON `urls` (`url`);
CREATE INDEX IF NOT EXISTS `IDX_urls_status` ON `urls` (`status`);
CREATE INDEX IF NOT EXISTS `IDX_urls_level` ON `urls` (`level`);
