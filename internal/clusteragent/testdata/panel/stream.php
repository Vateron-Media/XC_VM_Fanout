<?php
// argv: op, … — MAIN's side of the R2 streams section for the interop node
// (server 7), as MAIN's writers change it:
//   add <id>           a stream assigned to server 7 (bumped)
//   edit <id> <name>   a carried column changed by a save (bumped)
//   quiet <id> <name>  a carried column changed by a write that bumps nothing (the resync's)
//   drop <id>          the stream taken off server 7 (bumped)
//   reset              every stream at once (the floor raised)
//   head               MAIN's head version
require __DIR__ . '/common.php';

use XcVm\Core\Cluster\StreamVersions;

$rID = (int) ($argv[2] ?? 0);
switch ($argv[1]) {
	case 'add':
		$rDb->query("INSERT INTO `streams` (`id`, `type`, `stream_display_name`, `stream_source`) VALUES (?, 1, 'Added', '[\"http://origin/live/added.ts\"]')", $rID);
		$rDb->query('INSERT INTO `streams_servers` (`stream_id`, `server_id`, `pid`) VALUES (?, 7, 0)', $rID);
		echo StreamVersions::bump([$rID], $rDb);
		break;
	case 'edit':
	case 'quiet':
		$rDb->query('UPDATE `streams` SET `stream_display_name` = ? WHERE `id` = ?', $argv[3], $rID);
		echo $argv[1] === 'edit' ? StreamVersions::bump([$rID], $rDb) : 0;
		break;
	case 'drop':
		$rDb->query('DELETE FROM `streams_servers` WHERE `stream_id` = ? AND `server_id` = 7', $rID);
		$rDb->query('UPDATE `streams` SET `tv_archive_server_id` = 0 WHERE `id` = ?', $rID);
		$rDb->query('DELETE FROM `recordings` WHERE `stream_id` = ?', $rID);
		echo StreamVersions::bump([$rID], $rDb);
		break;
	case 'reset':
		echo StreamVersions::reset($rDb);
		break;
	case 'head':
		echo StreamVersions::head($rDb);
		break;
}
