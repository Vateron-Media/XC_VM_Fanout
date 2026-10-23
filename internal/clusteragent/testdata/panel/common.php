<?php
// Interop harness: MAIN's real ClusterApi from a panel checkout (XCVM_PANEL_DIR),
// with the panel's test fake of xcvm_core and a SQLite file. Test-only.

use XcVm\Core\Config\SettingsManager;
use XcVm\Infrastructure\Database\DatabaseFactory;

$rPanel = rtrim((string) getenv('XCVM_PANEL_DIR'), '/');
$rDbFile = (string) getenv('XCVM_INTEROP_DB');
require $rPanel . '/tests/bootstrap.php';

/**
 * A table of the panel's install schema (src/bin/install/database.sql) as
 * SQLite takes it, so the harness's tables have every column the panel's
 * code reads and do not drift from it: KEY lines, collations, `unsigned`,
 * comments and ON UPDATE go; an AUTO_INCREMENT column becomes SQLite's
 * INTEGER PRIMARY KEY AUTOINCREMENT; an enum is text.
 */
function interopTable(string $rPanel, string $rName): string {
	static $rSchema = null;
	$rSchema ??= (string) file_get_contents($rPanel . '/src/bin/install/database.sql');
	if (!preg_match('/CREATE TABLE IF NOT EXISTS `' . preg_quote($rName, '/') . '` \((.*?)\n\) ENGINE=[^;]*;/s', $rSchema, $rMatch)) {
		throw new RuntimeException('database.sql has no table ' . $rName);
	}
	$rAuto = null;
	$rCols = [];
	foreach (explode("\n", trim($rMatch[1])) as $rLine) {
		$rLine = rtrim(trim($rLine), ',');
		if ($rLine === '' || preg_match('/^(UNIQUE |FULLTEXT |SPATIAL )?(KEY|INDEX) /', $rLine) || str_starts_with($rLine, 'CONSTRAINT ')) {
			continue;
		}
		if (preg_match('/^`(\w+)` .*AUTO_INCREMENT/', $rLine, $rCol)) {
			$rAuto = $rCol[1];
			$rCols[] = '`' . $rAuto . '` INTEGER PRIMARY KEY AUTOINCREMENT';
			continue;
		}
		if (str_starts_with($rLine, 'PRIMARY KEY')) {
			if ($rAuto === null) {
				$rCols[] = (string) preg_replace('/ USING \w+$/', '', $rLine);
			}
			continue;
		}
		$rLine = (string) preg_replace(["/ COMMENT '(?:[^'\\\\]|\\\\.)*'/", '/ (COLLATE|CHARACTER SET) \w+/', '/ unsigned/', '/ ON UPDATE CURRENT_TIMESTAMP(\(\))?/', "/ (enum|set)\('[^)]*\)/"], ['', '', '', '', ' text'], $rLine);
		$rCols[] = $rLine;
	}
	return 'CREATE TABLE `' . $rName . '` (' . implode(', ', $rCols) . ')';
}

// A panel whose tests run on MariaDB (TestDb::connect) gets one schema the
// harness's PHP processes share, named after the Go test (their parent), which
// TestDb's orphan cleanup drops once that test has ended; XCVM_INTEROP_DB is
// then the lock its first process seeds it under. An older panel: SQLite.
$rMaria = method_exists(TestDb::class, 'connect');
$rIgnore = $rMaria ? 'INSERT IGNORE' : 'INSERT OR IGNORE';
if ($rMaria) {
	$rLock = fopen($rDbFile, 'c+');
	flock($rLock, LOCK_EX);
	$rNew = fstat($rLock)['size'] === 0;
	$rDb = new TestDb('xcvm_t' . posix_getppid() . '_' . crc32($rDbFile));
} else {
	$rNew = !file_exists($rDbFile);
	$rDb = new TestDb(new PDO('sqlite:' . $rDbFile));
}
if ($rNew && $rMaria) {
	// The panel's own install schema: every table and column a live MAIN has.
	// The harness seeds its own servers, settings and crontab.
	$rDb->exec((string) file_get_contents($rPanel . '/src/bin/install/database.sql'));
	foreach (['servers', 'settings', 'crontab'] as $rTable) {
		$rDb->exec('TRUNCATE `' . $rTable . '`');
	}
} elseif ($rNew) {
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
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `arch` varchar(8) DEFAULT NULL');
	// Migration 054: whether the node's agent holds its relay proxy's port.
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `relay_down_since` int DEFAULT NULL');
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `relay_error` varchar(255) DEFAULT NULL');
	// Migration 055: the owners whose chunk digest named no request.
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `digest_n1` varchar(255) DEFAULT NULL');
	// Migration 056: the lanes' lag and the MAIN URLs the node cannot reach.
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `p0_lag_since` int DEFAULT NULL');
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `p1_lag_since` int DEFAULT NULL');
	$rDb->exec('ALTER TABLE `cluster_nodes` ADD COLUMN `unreachable_urls` varchar(1024) DEFAULT NULL');
	// The tables the panel's code under test reads, from its own install
	// schema. The replica's servers and node sections read every column.
	foreach (['servers', 'crontab', 'streams_servers', 'streams', 'recordings', 'settings', 'cluster_stream_ver', 'streams_types', 'profiles', 'streams_options', 'streams_arguments', 'bouquets', 'streams_categories'] as $rTable) {
		$rDb->exec(interopTable($rPanel, $rTable));
	}
	$rDb->exec('CREATE TABLE `lines_live` (`activity_id` INTEGER PRIMARY KEY AUTOINCREMENT, `user_id` int, `stream_id` int, `server_id` int, `proxy_id` int, `user_agent` text, `user_ip` text, `container` text, `pid` int, `date_start` int, `geoip_country_code` text, `isp` text, `external_device` text, `hls_last_read` int, `hls_end` int DEFAULT 0, `hmac_id` int, `hmac_identifier` text, `uuid` text)');
	$rDb->exec('CREATE TABLE `cluster_changes` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `section` varchar(32), `op` varchar(8), `kind` varchar(16), `value` varchar(255), `time` int)');
	$rDb->exec('CREATE TABLE `blocked_ips` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `ip` varchar(39), `notes` text, `date` int)');
	$rDb->exec('CREATE TABLE `blocked_uas` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `user_agent` varchar(255), `exact_match` int DEFAULT 0)');
	$rDb->exec('CREATE TABLE `blocked_isps` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `isp` text, `blocked` int DEFAULT 0)');
	$rDb->exec('CREATE TABLE `blocked_asns` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `asn` int, `blocked` int DEFAULT 0)');
	$rDb->exec('CREATE TABLE `rtmp_ips` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `ip` varchar(255), `password` varchar(128), `push` int, `pull` int)');
	$rDb->exec('CREATE TABLE `streams_logs` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `stream_id` int, `server_id` int, `action` text, `source` text, `date` int)');
	// The resellers' DNS the servers section carries for verify_host (XC_VM #236).
	$rDb->exec('CREATE TABLE `users` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `reseller_dns` text, `status` int DEFAULT 1)');
}
if ($rNew) {
	$rDb->exec("INSERT INTO `servers` (`id`, `status`, `server_name`, `http_broadcast_port`, `total_clients`) VALUES (7, 0, 'LB 7', 8080, 1000)");
	// xc_cluster_sim's other nodes (XCVM_INTEROP_SERVERS, e.g. "8,9").
	foreach (array_filter(array_map('intval', explode(',', (string) getenv('XCVM_INTEROP_SERVERS')))) as $rSid) {
		$rDb->exec("INSERT INTO `servers` (`id`, `status`, `server_name`, `http_broadcast_port`, `total_clients`) VALUES ($rSid, 0, 'LB $rSid', 8080, 1000)");
	}
	// The crontab section: enabled rows whose role fits the node's mode.
	$rDb->exec("INSERT INTO `crontab` (`filename`, `time`, `enabled`, `role`) VALUES ('cache', '* * * * *', 1, 'all'), ('users', '*/5 * * * *', 1, 'legacy'), ('epg', '0 */6 * * *', 1, 'main'), ('servers', '* * * * *', 0, 'all')");
	$rDb->exec('INSERT INTO `streams_servers` (`server_stream_id`, `stream_id`, `server_id`, `pid`) VALUES (70, 100, 7, 0)');
	$rDb->exec("INSERT INTO `streams` (`id`, `type`, `tv_archive_server_id`) VALUES (100, 1, 7)");
	// The recording of stream 100 on server 7 the content test finishes.
	$rDb->exec("INSERT INTO `recordings` (`id`, `stream_id`, `created_id`, `category_id`, `bouquets`, `title`, `description`, `start`, `end`, `source_id`, `status`) VALUES (1, 100, NULL, '[]', '[]', 'Match', '', 1800000000, 1800003600, 7, 1)");
	// Migration 047's data: every server holding a stream gets its row at
	// version 0, and the counter starts at 1 (R2 streams versions).
	$rDb->exec($rIgnore . ' INTO `cluster_stream_ver` (`server_id`, `stream_id`, `ver`, `updated_at`) SELECT `server_id`, `stream_id`, 0, 0 FROM `streams_servers` WHERE `server_id` > 0 AND `stream_id` > 0');
	$rDb->exec($rIgnore . ' INTO `cluster_stream_ver` (`server_id`, `stream_id`, `ver`, `updated_at`) SELECT `tv_archive_server_id`, `id`, 0, 0 FROM `streams` WHERE `tv_archive_server_id` > 0');
	$rDb->exec($rIgnore . ' INTO `cluster_stream_ver` (`server_id`, `stream_id`, `ver`, `updated_at`) SELECT `source_id`, `stream_id`, 0, 0 FROM `recordings` WHERE `source_id` > 0 AND `stream_id` > 0');
	$rDb->exec($rIgnore . " INTO `cluster_meta` (`name`, `value`, `updated_at`) VALUES ('stream_ver', '1', 0)");
	// live_streaming_pass is the secrets section's; the settings section withholds it.
	$rDb->exec("INSERT INTO `settings` (`id`, `server_name`, `seg_time`, `api_pass`, `live_streaming_pass`, `cloudflare`) VALUES (1, 'Interop', 6, 'secret', 'InteropStreamPass', 0)");
}
if ($rMaria) {
	if ($rNew) {
		fwrite($rLock, 'seeded');
	}
	flock($rLock, LOCK_UN);
	fclose($rLock);
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
if (getenv('XCVM_INTEROP_OFFAIR')) {
	// The admin's not_on_air video, which the artefact op serves (ArtefactRegistry).
	$rSettings['not_on_air_video_path'] = (string) getenv('XCVM_INTEROP_OFFAIR');
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
if (method_exists(\XcVm\Domain\Cluster\ReplicaBuilder::class, 'useXferDir')) {
	// A section staged for its parts: beside the DB, never a shared tmp/.
	\XcVm\Domain\Cluster\ReplicaBuilder::useXferDir(dirname($rDbFile) . '/xfer/');
}
// xc_cluster_sim: <db>.clock holds how far MAIN's clock runs ahead of this
// machine's, in ms. Each request fixes MAIN's clock there while it exists.
if (is_file($rDbFile . '.clock')) {
	\XcVm\Domain\Cluster\ClusterClock::fix((int) round(microtime(true) * 1000) + (int) file_get_contents($rDbFile . '.clock'));
}
$rCrypto = new \XcVm\Tests\Support\FakeClusterCrypto();
// xc_cluster_sim: while <db>.unlicensed exists MAIN's licence is gone. No
// token and no lease is issued, and sessions go on (graceful revocation).
if (is_file($rDbFile . '.unlicensed')) {
	$rCrypto->rLicensed = false;
	$rCrypto->rRefuseIssue = 'LICENCE';
	$rCrypto->rRefuseLease = 'LICENCE';
}
if (class_exists(\XcVm\Domain\Cluster\ConnectionDigest::class)) {
	// Every heartbeat's digest is checked, and snapshots are staged next to the DB.
	\XcVm\Domain\Cluster\ConnectionDigest::useState(dirname($rDbFile) . '/digest/', 0);
	\XcVm\Domain\Cluster\ConnectionSnapshot::useDir(dirname($rDbFile) . '/snap/');
}
