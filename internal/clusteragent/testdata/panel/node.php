<?php
// Print what MAIN keeps for the interop node (server 7): the policy it dials,
// the port it reached, and its audit.
require __DIR__ . '/common.php';
$rDb->query('SELECT `policy_ver`, `main_port`, `audit`, `features` FROM `cluster_nodes` WHERE `server_id` = 7;');
echo json_encode($rDb->get_row(), JSON_UNESCAPED_SLASHES);
