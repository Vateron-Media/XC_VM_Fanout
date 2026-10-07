<?php
// argv: op, … — the data plane (ADR 0004, Phase 8) against the panel's real
// code, with MAIN (server 1) the parent and owner of what node 7 pulls:
//   setup <size>                          server 1 is MAIN; node 7 relays stream 100 from it and
//                                         reads a file of <size> bytes MAIN holds (bumped): its path
//   relay <wire> <header> <target>        MAIN's RelayGuard: the child's server id, or "refused"
//   xfile <wire> <auth> <target> <o> <n>  MAIN's FileTicketServer: {status, digest, body (base64)}
require __DIR__ . '/common.php';

use XcVm\Core\Cluster\DataPlaneTrust;
use XcVm\Core\Cluster\FileTicketServer;
use XcVm\Core\Cluster\RelayGuard;
use XcVm\Core\Cluster\StreamVersions;

if (!defined('SERVER_ID')) {
	define('SERVER_ID', 1); // MAIN
}
// MAIN's trust sources as they are, but for the crypto: the harness's fake xcvm_core.
DataPlaneTrust::useCrypto($rCrypto);
DataPlaneTrust::useSources(null, null, null, null, true);

switch ($argv[1]) {
	case 'setup':
		$rFile = dirname($rDbFile) . '/movie.mkv';
		$rBytes = '';
		for ($i = 0; strlen($rBytes) < (int) $argv[2]; $i++) {
			$rBytes .= hash('sha256', 'chunk' . $i, true);
		}
		file_put_contents($rFile, substr($rBytes, 0, (int) $argv[2]));
		$argv[2] = $rFile;
		$rDb->query($rIgnore . ' INTO `servers` (`id`, `status`, `server_name`, `is_main`, `server_ip`, `http_broadcast_port`, `total_clients`) VALUES (1, 1, ?, 1, ?, ?, 1000)', 'MAIN', '127.0.0.1', (int) getenv('XCVM_INTEROP_PORT'));
		$rDb->query('UPDATE `streams_servers` SET `parent_id` = 1 WHERE `stream_id` = 100 AND `server_id` = 7');
		$rDb->query('UPDATE `streams` SET `stream_source` = ? WHERE `id` = 100', json_encode(['s:1:' . $argv[2]]));
		StreamVersions::bump([100], $rDb);
		echo $rFile;
		break;
	case 'relay':
		$rSid = RelayGuard::relay(100, ['REQUEST_METHOD' => 'GET', 'REQUEST_URI' => $argv[4], RelayGuard::TICKET => $argv[2], RelayGuard::AUTH => $argv[3]], (int) floor(microtime(true) * 1000));
		echo $rSid === null ? 'refused' : $rSid;
		break;
	case 'xfile':
		$rOut = FileTicketServer::serve(
			['REQUEST_METHOD' => 'GET', 'REQUEST_URI' => $argv[4], FileTicketServer::TICKET => $argv[2], FileTicketServer::AUTH => $argv[3]],
			['o' => $argv[5], 'n' => $argv[6]],
			['lb_scan_roots' => [dirname($rDbFile)]]
		);
		echo json_encode(['status' => $rOut['status'], 'digest' => $rOut['headers']['X-XCVM-File-Digest'] ?? '', 'body' => base64_encode($rOut['body'])]);
		break;
}
