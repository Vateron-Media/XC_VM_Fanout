<?php
// argv: op, … — the admin side of the command channel for server 7.
//   flows <bits>            set the node's flow bits
//   enqueue <type> <json>   queue a command, print its cmd_id
//   result <cmd_id>         print the outcome as JSON, or "pending"
require __DIR__ . '/common.php';
switch ($argv[1]) {
	case 'flows':
		\XcVm\Domain\Cluster\NodeRegistry::update(7, ['flows' => (int) $argv[2]]);
		echo 'OK';
		break;
	case 'enqueue':
		echo \XcVm\Domain\Cluster\CommandBus::enqueue($rCrypto, 7, $argv[2], json_decode($argv[3], true));
		break;
	case 'result':
		$rOut = \XcVm\Domain\Cluster\CommandBus::result($argv[2]);
		echo $rOut === null ? 'pending' : json_encode($rOut);
		break;
}
