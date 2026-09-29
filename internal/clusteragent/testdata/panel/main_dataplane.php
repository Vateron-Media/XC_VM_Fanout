<?php
// argv: config dir, MAIN's sign pub (hex), box pub (hex), uuid; node 7's sign
// pub and box pub (hex). MAIN's data-plane client (ADR 0004, Phase 9's eighth
// increment) against the panel's real MainDataPlane: `cluster:main-dataplane
// on` with the keys `xc_agent keygen` printed, then a file MAIN reads from
// node 7 and a stream it relays from it. Prints {ref} of the file.
require __DIR__ . '/common.php';

use XcVm\Core\Cluster\DataPlane;
use XcVm\Domain\Cluster\MainDataPlane;

if (!defined('CONFIG_PATH')) {
	define('CONFIG_PATH', rtrim($argv[1], '/') . '/');
}
[, , $rSign, $rBox, $rUuid, $rNodeSign, $rNodeBox] = $argv;
$rDb->query('INSERT OR IGNORE INTO `servers` (`id`, `status`, `server_name`, `is_main`, `server_ip`, `http_broadcast_port`, `total_clients`) VALUES (1, 1, ?, 1, ?, 80, 1000)', 'MAIN', '127.0.0.1');
$rDb->query('DELETE FROM `cluster_nodes` WHERE `server_id` = 7');
$rDb->query("INSERT INTO `cluster_nodes` (`server_id`, `node_uuid`, `state`, `mode`, `flows`, `gen`, `node_sign_pub`, `node_box_pub`, `epoch`, `created_at`, `updated_at`) VALUES (7, '0f8fad5b-d9cb-469f-a165-70867728950e', 'active', 1, 128, 3, ?, ?, 1, 0, 0)", hex2bin($rNodeSign), hex2bin($rNodeBox));
MainDataPlane::useSeams(static fn() => $rCrypto);
$rWhy = MainDataPlane::enable(static fn(string $rState, string $rId): array => ['node_uuid' => $rId, 'sign_pub' => $rSign, 'box_pub' => $rBox]);
if ($rWhy !== null) {
	fwrite(STDERR, $rWhy . "\n");
	exit(1);
}
// The uuid MAIN chose is the agent's own: the test's keygen ran with it.
$rMeta = MainDataPlane::identity();
if (!MainDataPlane::ensureFile(7, '/movies/a.mkv') || !MainDataPlane::ensureRelay(7, 100)) {
	fwrite(STDERR, "no ticket\n");
	exit(1);
}
echo json_encode(['ref' => DataPlane::ref(7, '/movies/a.mkv'), 'uuid' => $rMeta['node_uuid']]);
