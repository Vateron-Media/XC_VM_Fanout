<?php
// argv: op, … — MAIN's side of the artefacts granted to server 7, the
// admin's not_on_air video being <video>.
//   offer <video>            ArtefactGrants::offerOffAir; print the artefact.fetch cmd_ids, JSON
//   tampered <video> <what>  queue an artefact.fetch whose grant names another
//                            sha256 or size than the video's; print its cmd_id
//   audit                    print MAIN's audit rows, JSON
require __DIR__ . '/common.php';
$rSettings[\XcVm\Core\Cluster\ReplicaSections::OFF_AIR['not_on_air']] = $argv[2] ?? '';
switch ($argv[1]) {
	case 'offer':
		\XcVm\Domain\Cluster\ArtefactGrants::offerOffAir($rCrypto, $rSettings);
		$rDb->query("SELECT `cmd_id` FROM `cluster_commands` WHERE `type` = 'artefact.fetch' ORDER BY `id` ASC;");
		echo json_encode(array_column($rDb->get_rows(), 'cmd_id'));
		break;
	case 'tampered':
		$rGrant = \XcVm\Domain\Cluster\ArtefactGrants::grant(\XcVm\Domain\Cluster\ArtefactRegistry::describe('offair/not_on_air', $rSettings));
		if ($argv[3] === 'sha256') {
			$rGrant['sha256'] = hash('sha256', 'not the video');
		} else {
			$rGrant['size']++;
		}
		echo \XcVm\Domain\Cluster\CommandBus::enqueue($rCrypto, 7, 'artefact.fetch', ['artefact' => $rGrant]);
		break;
	case 'audit':
		$rDb->query('SELECT * FROM `cluster_audit`;');
		echo json_encode($rDb->get_rows());
		break;
}
