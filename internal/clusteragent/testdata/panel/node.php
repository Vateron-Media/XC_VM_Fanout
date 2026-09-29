<?php
// Print what MAIN keeps for the interop node (server 7): the policy it dials,
// the port it reached, its audit, whether its relay proxy holds its port, and
// the owners whose chunk digest named no request.
require __DIR__ . '/common.php';
$rDb->query('SELECT `policy_ver`, `main_port`, `audit`, `features`, `relay_down_since`, `relay_error`, `digest_n1` FROM `cluster_nodes` WHERE `server_id` = 7;');
echo json_encode($rDb->get_row(), JSON_UNESCAPED_SLASHES);
