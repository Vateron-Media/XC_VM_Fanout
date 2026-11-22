<?php
// argv: uuid — MAIN's stored hls_last_read for a viewer, as JSON (null: no row).
require __DIR__ . '/common.php';
$rDb->query('SELECT `hls_last_read` FROM `lines_live` WHERE `uuid` = ?', $argv[1]);
$rRow = $rDb->get_row();
echo json_encode(is_array($rRow) ? (int) $rRow['hls_last_read'] : null);
