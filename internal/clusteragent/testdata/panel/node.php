<?php
// Print what MAIN keeps for the interop node (server 7): the policy it dials,
// the port it reached, its audit, and whether its relay proxy holds its port.
require __DIR__ . '/common.php';
$rDb->query('SELECT `policy_ver`, `main_port`, `audit`, `features`, `relay_down_since`, `relay_error` FROM `cluster_nodes` WHERE `server_id` = 7;');
echo json_encode($rDb->get_row(), JSON_UNESCAPED_SLASHES);
