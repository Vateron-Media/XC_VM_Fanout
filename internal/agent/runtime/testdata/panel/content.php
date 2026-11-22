<?php
// argv: op, … — the LB's PHP side of content, and what MAIN holds.
//   record <flows.json> <spool dir> <socket>   finish recording 1 as RecordCommand
//                                              does on a CONTENT node: the VOD id
//                                              through the agent's socket, then
//                                              status 2 and a worker pid as events
//   state                                      MAIN's recording, VOD and worker pid
require __DIR__ . '/common.php';

use XcVm\Core\Cluster\AgentClient;
use XcVm\Core\Cluster\EventSpool;
use XcVm\Core\Cluster\NodeFlows;
use XcVm\Domain\Stream\ContentSink;

switch ($argv[1]) {
	case 'record':
		NodeFlows::usePath($argv[2]);
		EventSpool::useDir(rtrim($argv[3], '/') . '/');
		AgentClient::useSocket($argv[4]);
		$rReply = AgentClient::main('recording_complete', ['recording_id' => 1, 'stream_icon' => null]);
		$rAgain = AgentClient::main('recording_complete', ['recording_id' => 1]);
		ContentSink::recordingDone(1, 7);
		ContentSink::workerPid(100, 'tv_archive', 4242);
		echo json_encode([$rReply['stream_id'] ?? null, $rAgain['stream_id'] ?? null]);
		break;
	case 'state':
		$rDb->query('SELECT `status`, `created_id` FROM `recordings` WHERE `id` = 1');
		$rRec = $rDb->get_row();
		$rDb->query('SELECT COUNT(*) AS `n` FROM `streams_servers` WHERE `stream_id` = ? AND `server_id` = 7 AND `pid` = 1', (int) $rRec['created_id']);
		$rAttached = (int) $rDb->get_row()['n'];
		$rDb->query('SELECT `tv_archive_pid` FROM `streams` WHERE `id` = 100');
		echo json_encode(['status' => (int) $rRec['status'], 'created_id' => (int) $rRec['created_id'], 'attached' => $rAttached, 'archive_pid' => (int) $rDb->get_row()['tv_archive_pid']]);
		break;
}
