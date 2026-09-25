<?php
// Interop harness: MAIN's real ClusterApi from a panel checkout (XCVM_PANEL_DIR),
// with the panel's test fake of xcvm_core and a SQLite file. Test-only.

use XcVm\Core\Config\SettingsManager;
use XcVm\Infrastructure\Database\DatabaseFactory;

$rPanel = rtrim((string) getenv('XCVM_PANEL_DIR'), '/');
$rDbFile = (string) getenv('XCVM_INTEROP_DB');
require $rPanel . '/tests/bootstrap.php';

$rNew = !file_exists($rDbFile);
$rDb = new TestDb(new PDO('sqlite:' . $rDbFile));
if ($rNew) {
	foreach (['029_create_cluster_nodes', '030_create_cluster_commands', '031_create_cluster_enrolment', '032_create_cluster_audit'] as $rName) {
		$rSql = (string) file_get_contents($rPanel . '/src/migrations/database/up/' . $rName . '.sql');
		$rSql = (string) preg_replace('/^--.*$/m', '', $rSql);
		$rSql = (string) preg_replace('/`id` (bigint\(20\) unsigned|int\(11\)) NOT NULL AUTO_INCREMENT/', '`id` INTEGER PRIMARY KEY AUTOINCREMENT', $rSql);
		$rSql = (string) preg_replace('/,\s*PRIMARY KEY \(`id`\)/', '', $rSql);
		$rSql = (string) preg_replace('/,\s*(UNIQUE )?KEY `\w+` \([^)]*\)/', '', $rSql);
		$rSql = (string) preg_replace('/ unsigned| COLLATE \w+/', '', $rSql);
		$rDb->exec((string) preg_replace('/\) ENGINE=[^;]*;/', ');', $rSql));
	}
	$rDb->exec('ALTER TABLE `cluster_node_epochs` ADD COLUMN `agent_eph_pub` binary(32) DEFAULT NULL');
	$rDb->exec('ALTER TABLE `cluster_enrol_requests` ADD COLUMN `agent_eph_pub` binary(32) DEFAULT NULL');
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `root_ready` tinyint(1) NOT NULL DEFAULT 0');
	$rDb->exec('CREATE TABLE `servers` (`id` INTEGER PRIMARY KEY, `status` int NOT NULL DEFAULT 0)');
	$rDb->exec('INSERT INTO `servers` (`id`, `status`) VALUES (7, 0)');
	$rDb->exec('CREATE TABLE `streams_servers` (`server_stream_id` INTEGER PRIMARY KEY, `stream_id` int, `server_id` int, `parent_id` int, `pid` int, `to_analyze` int, `current_source` text, `monitor_pid` int, `stream_status` int DEFAULT 0, `stream_started` int, `stream_info` text, `audio_codec` text, `video_codec` text, `resolution` int, `bitrate` int, `compatible` int)');
	$rDb->exec('INSERT INTO `streams_servers` (`server_stream_id`, `stream_id`, `server_id`, `pid`) VALUES (70, 100, 7, 0)');
	$rDb->exec('CREATE TABLE `streams` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `type` int, `stream_display_name` text, `stream_source` text, `target_container` text, `year` text, `movie_properties` text, `rating` int, `read_native` int, `movie_symlink` int, `remove_subtitles` int, `transcode_profile_id` int, `order` int, `added` int, `category_id` text, `tv_archive_server_id` int, `tv_archive_pid` int)');
	$rDb->exec("INSERT INTO `streams` (`id`, `type`, `tv_archive_server_id`) VALUES (100, 1, 7)");
	$rDb->exec('CREATE TABLE `recordings` (`id` INTEGER PRIMARY KEY, `created_id` int, `category_id` text, `bouquets` text, `title` text, `description` text, `start` int, `end` int, `source_id` int, `status` int)');
	$rDb->exec("INSERT INTO `recordings` VALUES (1, NULL, '[]', '[]', 'Match', '', 1800000000, 1800003600, 7, 1)");
	$rDb->exec('CREATE TABLE `lines_live` (`activity_id` INTEGER PRIMARY KEY AUTOINCREMENT, `user_id` int, `stream_id` int, `server_id` int, `proxy_id` int, `user_agent` text, `user_ip` text, `container` text, `pid` int, `date_start` int, `geoip_country_code` text, `isp` text, `external_device` text, `hls_last_read` int, `hls_end` int DEFAULT 0, `hmac_id` int, `hmac_identifier` text, `uuid` text)');
	$rDb->exec('CREATE TABLE `bouquets` (`id` INTEGER PRIMARY KEY, `bouquet_movies` text)');
	$rDb->exec('CREATE TABLE `streams_logs` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `stream_id` int, `server_id` int, `action` text, `source` text, `date` int)');
}
DatabaseFactory::set($rDb);
$rSettings = ['cluster_api_enabled' => 1, 'lb_token_rotation_min' => (int) (getenv('XCVM_INTEROP_ROTATION') ?: 60), 'lb_new_node_mode' => 'legacy'];
SettingsManager::set($rSettings);
$rMain = ['server_ip' => '127.0.0.1', 'http_broadcast_port' => (int) getenv('XCVM_INTEROP_PORT')];
$rCrypto = new \XcVm\Tests\Support\FakeClusterCrypto();
