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
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `features` varchar(255) DEFAULT NULL');
	// Migrations 045 and 046: the node's audit, and the MAIN port it last reached.
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `audit` text DEFAULT NULL');
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `main_port` int DEFAULT NULL');
	// The replica's servers and node sections read every column (a missing one travels as null).
	$rDb->exec('CREATE TABLE `servers` (`id` INTEGER PRIMARY KEY, `status` int NOT NULL DEFAULT 0, `server_name` text, `enabled` int DEFAULT 1, `http_broadcast_port` int, `total_clients` int)');
	$rDb->exec("INSERT INTO `servers` (`id`, `status`, `server_name`, `http_broadcast_port`, `total_clients`) VALUES (7, 0, 'LB 7', 8080, 1000)");
	// The crontab section: enabled rows whose role fits the node's mode.
	$rDb->exec("CREATE TABLE `crontab` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `filename` text, `time` text, `enabled` int DEFAULT 1, `role` text NOT NULL DEFAULT 'all')");
	$rDb->exec("INSERT INTO `crontab` (`filename`, `time`, `enabled`, `role`) VALUES ('cache', '* * * * *', 1, 'all'), ('users', '*/5 * * * *', 1, 'legacy'), ('epg', '0 */6 * * *', 1, 'main'), ('servers', '* * * * *', 0, 'all')");
	$rDb->exec('CREATE TABLE `streams_servers` (`server_stream_id` INTEGER PRIMARY KEY, `stream_id` int, `server_id` int, `parent_id` int, `pid` int, `to_analyze` int, `current_source` text, `monitor_pid` int, `stream_status` int DEFAULT 0, `stream_started` int, `stream_info` text, `audio_codec` text, `video_codec` text, `resolution` int, `bitrate` int, `compatible` int)');
	$rDb->exec('INSERT INTO `streams_servers` (`server_stream_id`, `stream_id`, `server_id`, `pid`) VALUES (70, 100, 7, 0)');
	$rDb->exec('CREATE TABLE `streams` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `type` int, `stream_display_name` text, `stream_source` text, `target_container` text, `year` text, `movie_properties` text, `rating` int, `read_native` int, `movie_symlink` int, `remove_subtitles` int, `transcode_profile_id` int, `order` int, `added` int, `category_id` text, `tv_archive_server_id` int, `tv_archive_pid` int)');
	$rDb->exec("INSERT INTO `streams` (`id`, `type`, `tv_archive_server_id`) VALUES (100, 1, 7)");
	$rDb->exec('CREATE TABLE `recordings` (`id` INTEGER PRIMARY KEY, `created_id` int, `category_id` text, `bouquets` text, `title` text, `description` text, `start` int, `end` int, `source_id` int, `status` int)');
	$rDb->exec("INSERT INTO `recordings` VALUES (1, NULL, '[]', '[]', 'Match', '', 1800000000, 1800003600, 7, 1)");
	$rDb->exec('CREATE TABLE `lines_live` (`activity_id` INTEGER PRIMARY KEY AUTOINCREMENT, `user_id` int, `stream_id` int, `server_id` int, `proxy_id` int, `user_agent` text, `user_ip` text, `container` text, `pid` int, `date_start` int, `geoip_country_code` text, `isp` text, `external_device` text, `hls_last_read` int, `hls_end` int DEFAULT 0, `hmac_id` int, `hmac_identifier` text, `uuid` text)');
	$rDb->exec('CREATE TABLE `bouquets` (`id` INTEGER PRIMARY KEY, `bouquet_movies` text)');
	$rDb->exec('CREATE TABLE `cluster_changes` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `section` varchar(32), `op` varchar(8), `kind` varchar(16), `value` varchar(255), `time` int)');
	$rDb->exec('CREATE TABLE `blocked_ips` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `ip` varchar(39), `notes` text, `date` int)');
	$rDb->exec('CREATE TABLE `blocked_uas` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `user_agent` varchar(255), `exact_match` int DEFAULT 0)');
	$rDb->exec('CREATE TABLE `blocked_isps` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `isp` text, `blocked` int DEFAULT 0)');
	$rDb->exec('CREATE TABLE `blocked_asns` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `asn` int, `blocked` int DEFAULT 0)');
	// live_streaming_pass is the secrets section's; the settings section withholds it.
	$rDb->exec('CREATE TABLE `settings` (`id` int, `server_name` text, `seg_time` int, `api_pass` text, `live_streaming_pass` text, `cloudflare` int)');
	$rDb->exec("INSERT INTO `settings` VALUES (1, 'Interop', 6, 'secret', 'InteropStreamPass', 0)");
	$rDb->exec('CREATE TABLE `rtmp_ips` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `ip` varchar(255), `password` varchar(128), `push` int, `pull` int)');
	$rDb->exec('CREATE TABLE `streams_logs` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `stream_id` int, `server_id` int, `action` text, `source` text, `date` int)');
}
DatabaseFactory::set($rDb);
$rSettings = ['cluster_api_enabled' => 1, 'lb_token_rotation_min' => (int) (getenv('XCVM_INTEROP_ROTATION') ?: 60), 'lb_new_node_mode' => 'legacy'];
if (getenv('XCVM_INTEROP_TRANSPORT')) {
	// The transport policy as an admin set it (https_required drill).
	$rSettings['cluster_transport'] = (string) getenv('XCVM_INTEROP_TRANSPORT');
}
if (getenv('XCVM_INTEROP_POLICY_VER') !== false) {
	$rSettings['cluster_policy_ver'] = (int) getenv('XCVM_INTEROP_POLICY_VER');
}
if (getenv('XCVM_INTEROP_BUS')) {
	// MAIN's cluster bus (a redis-server the test runs), with one ingest permit per lane.
	\XcVm\Domain\Cluster\ClusterBus::useSocket((string) getenv('XCVM_INTEROP_BUS'));
	$rSettings['cluster_ingest_concurrency'] = 1;
}
if (method_exists(\XcVm\Core\Config\OpensslExtra::class, 'usePrevFile')) {
	// The secrets section's previous OPENSSL_EXTRA: none, and never the deploy root's config/.
	\XcVm\Core\Config\OpensslExtra::usePrevFile(dirname($rDbFile) . '/openssl_extra.prev');
}
SettingsManager::set($rSettings);
$rMain = ['server_ip' => '127.0.0.1', 'http_broadcast_port' => (int) getenv('XCVM_INTEROP_PORT'), 'enable_https' => 1, 'domain_name' => 'main.invalid', 'https_broadcast_port' => 1];
$rCrypto = new \XcVm\Tests\Support\FakeClusterCrypto();
if (class_exists(\XcVm\Domain\Cluster\ConnectionDigest::class)) {
	// Every heartbeat's digest is checked, and snapshots are staged next to the DB.
	\XcVm\Domain\Cluster\ConnectionDigest::useState(dirname($rDbFile) . '/digest/', 0);
	\XcVm\Domain\Cluster\ConnectionSnapshot::useDir(dirname($rDbFile) . '/snap/');
}
