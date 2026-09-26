<?php
// Block (or, with "del", unblock) an IP on MAIN as the panel does: the row and
// its change-log entry. Test-only.
require __DIR__ . '/common.php';
$rIP = (string) ($argv[1] ?? '');
if (($argv[2] ?? '') === 'del') {
	$rDb->query('DELETE FROM `blocked_ips` WHERE `ip` = ?', $rIP);
	\XcVm\Core\Cluster\BlocklistChanges::del('ip', [$rIP]);
} else {
	$rDb->query('INSERT INTO `blocked_ips` (`ip`, `date`) VALUES (?, 0)', $rIP);
	\XcVm\Core\Cluster\BlocklistChanges::set('ip', [$rIP]);
}
echo "OK\n";
