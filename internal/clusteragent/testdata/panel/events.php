<?php
// argv: op, … — the LB's PHP side of the event lanes, and what MAIN holds.
//   flows <bits>                     mode 1 with these flow bits for server 7
//   write <flows.json> <spool dir>   stream state and a log through the real
//                                    StreamStateWriter / LogSink (flows as the agent wrote them)
//   state                            MAIN's row, logs and cursors, as JSON
require __DIR__ . '/common.php';

use XcVm\Core\Cluster\EventSpool;
use XcVm\Core\Cluster\LogSink;
use XcVm\Core\Cluster\NodeFlows;
use XcVm\Domain\Stream\StreamStateWriter;

switch ($argv[1]) {
	case 'flows':
		\XcVm\Domain\Cluster\NodeRegistry::update(7, ['mode' => 1, 'flows' => (int) $argv[2]]);
		echo 'OK';
		break;
	case 'write':
		NodeFlows::usePath($argv[2]);
		EventSpool::useDir(rtrim($argv[3], '/') . '/');
		StreamStateWriter::update(100, 7, ['pid' => 4242, 'current_source' => 'http://bob:pw@origin/live/bob/pw/1.ts']);
		LogSink::write('stream', [['stream_id' => 100, 'server_id' => 7, 'action' => 'start', 'source' => 'http://bob:pw@origin/1', 'date' => 1]]);
		echo 'OK';
		break;
	case 'state':
		$rDb->query('SELECT `pid`, `current_source` FROM `streams_servers` WHERE `server_stream_id` = 70');
		$rRow = $rDb->get_row();
		$rDb->query('SELECT `action`, `source`, `server_id` FROM `streams_logs`');
		$rLogs = $rDb->get_rows();
		$rNode = \XcVm\Domain\Cluster\NodeRegistry::byServer(7);
		echo json_encode(['pid' => (int) $rRow['pid'], 'source' => $rRow['current_source'], 'logs' => $rLogs, 'p0' => (int) $rNode['useq_p0'], 'p1' => (int) $rNode['useq_p1']]);
		break;
}
