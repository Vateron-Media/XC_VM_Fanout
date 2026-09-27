<?php
// Hold or give back one of MAIN's ingest permits on the cluster bus, as a
// request MAIN is serving would: permit.php <socket> hold <lane> | release <lane> <id>.
require __DIR__ . '/common.php';
use XcVm\Domain\Cluster\ClusterBus;
use XcVm\Domain\Cluster\ClusterSemaphore;
ClusterBus::useSocket($argv[1]);
if ($argv[2] === 'hold') {
	$rID = ClusterSemaphore::acquireIngest($argv[3], 1);
	if (!is_string($rID)) {
		fwrite(STDERR, "no permit\n");
		exit(1);
	}
	echo $rID;
} else {
	ClusterSemaphore::releaseIngest($argv[3], $argv[4]);
}
