<?php
// argv: op, … — the LB's PHP recording a viewer through its agent, and MAIN's store.
//   node <flows.json> <spool dir> <socket>   the stream endpoints' seam calls, as JSON
//   main                                     MAIN's lines_live row for the viewer
//   close <remove>                           MAIN closes it: a conn.close command for server 7
//   ghost <uuid>                             a row for server 7 the node never had (a drift)
//   digest                                   MAIN's digest of server 7's open connections
//   seed <socket>                            cluster:seed-connections on the node (MAIN's store → agent)
require __DIR__ . '/common.php';

use XcVm\Core\Cluster\AgentClient;
use XcVm\Core\Cluster\AgentConnections;
use XcVm\Core\Cluster\EventSpool;
use XcVm\Core\Cluster\NodeFlows;
use XcVm\Domain\Stream\ConnectionTracker;

switch ($argv[1]) {
	case 'node':
		NodeFlows::usePath($argv[2]);
		EventSpool::useDir(rtrim($argv[3], '/') . '/');
		AgentClient::useSocket($argv[4]);
		$rSet = ['redis_handler' => 0];
		$rRec = ['user_id' => 7, 'stream_id' => 100, 'server_id' => 7, 'proxy_id' => 0, 'user_agent' => 'VLC', 'user_ip' => '10.0.0.9', 'container' => 'hls', 'pid' => null, 'date_start' => 1800000000, 'geoip_country_code' => 'PT', 'isp' => 'ISP', 'external_device' => '', 'hls_end' => 0, 'hls_last_read' => 1800000000, 'on_demand' => 0, 'identity' => 7, 'uuid' => 'v1'];
		$rOut = ['open' => ConnectionTracker::openRecord($rSet, $rRec, ['uuid' => 'not used']) === true];
		$rFound = ConnectionTracker::findByUuid($rSet, 'v1', '`activity_id`');
		$rOut['found_ip'] = $rFound['user_ip'] ?? null;
		$rOut['updated'] = ConnectionTracker::updateLive($rSet, $rFound, ['pid' => 4242]);
		$rOut['beat'] = ConnectionTracker::heartbeat($rSet, 'v1', 1800000100)['hls_last_read'] ?? null;
		$rOut['accepted'] = ConnectionTracker::acceptedIP($rSet, 7);
		$rOut['local_rows'] = 0; // nothing was written to a store here: MAIN's comes from the events
		echo json_encode($rOut);
		break;
	case 'main':
		$rDb->query("SELECT `server_id`, `pid`, `hls_end`, `user_ip` FROM `lines_live` WHERE `uuid` = 'v1'");
		echo json_encode($rDb->get_rows());
		break;
	case 'ghost':
		$rDb->query('INSERT INTO `lines_live` (`uuid`, `server_id`, `user_id`, `stream_id`, `hls_end`) VALUES (?, 7, 8, 100, 0)', $argv[2]);
		break;
	case 'digest':
		echo json_encode(\XcVm\Domain\Cluster\ConnectionDigest::of(\XcVm\Domain\Cluster\ConnectionDigest::stored(7)));
		break;
	case 'seed':
		AgentClient::useSocket($argv[2]);
		echo json_encode(AgentConnections::seed(\XcVm\Cli\Commands\ClusterSeedConnectionsCommand::stored(7)));
		break;
	case 'close':
		echo \XcVm\Domain\Cluster\CommandBus::enqueue($rCrypto, 7, 'conn.close', ['uuid' => 'v1', 'remove' => $argv[2] === '1']);
		break;
}
