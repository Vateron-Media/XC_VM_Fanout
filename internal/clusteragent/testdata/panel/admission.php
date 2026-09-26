<?php
// argv: op, … — admission of a new viewer (ADR 0004, ninth Phase 6 increment).
//   lines                                     MAIN's lines: 70 valid (max 1), 71 expired
//   node <flows.json> <socket> <line> <uuid>  the node's PHP registering a viewer with a
//                                             limited token and no adm claim, as live.php does
//   stored <socket> <uuid>                    whether the agent holds the viewer
require __DIR__ . '/common.php';

use XcVm\Core\Cluster\AgentClient;
use XcVm\Core\Cluster\AgentConnections;
use XcVm\Core\Cluster\NodeFlows;

switch ($argv[1]) {
	case 'lines':
		$rDb->exec('CREATE TABLE IF NOT EXISTS `lines` (`id` INTEGER PRIMARY KEY, `max_connections` int, `enabled` int, `admin_enabled` int, `exp_date` int, `pair_id` int)');
		$rDb->exec('CREATE TABLE IF NOT EXISTS `cluster_reservations` (`id` char(32) PRIMARY KEY, `identity` varchar(255), `server_id` int, `stream_id` int, `created_at` int, `exp` int)');
		$rDb->exec('DELETE FROM `lines`');
		$rDb->query('INSERT INTO `lines` VALUES (70, 1, 1, 1, NULL, NULL), (71, 1, 1, 1, ?, NULL)', time() - 60);
		echo 'OK';
		break;
	case 'node':
		NodeFlows::usePath($argv[2]);
		AgentClient::useSocket($argv[3]);
		$rRecord = ['user_id' => (int) $argv[4], 'stream_id' => 100, 'server_id' => 7, 'user_agent' => 'VLC/3 ü', 'user_ip' => '10.0.0.9', 'container' => 'ts', 'pid' => 0, 'date_start' => time(), 'hls_end' => 0, 'uuid' => $argv[5]];
		$rToken = ['uuid' => $argv[5], 'user_info' => ['max_connections' => 1]];
		$rAdmission = AgentConnections::admission($rToken, $rRecord, time());
		echo json_encode(['header' => $rAdmission !== null, 'result' => AgentConnections::register($argv[5], $rRecord, $rAdmission)]);
		break;
	case 'stored':
		AgentClient::useSocket($argv[2]);
		echo json_encode(AgentConnections::get($argv[3]) !== false);
		break;
}
