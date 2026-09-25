<?php
// Expire every token of the interop node (as an outage past exp would), without
// moving the clock: the rows go, so their z is gone too.
require __DIR__ . '/common.php';
$rDb->query('DELETE FROM `cluster_node_epochs` WHERE `server_id` = 7');
echo "OK\n";
