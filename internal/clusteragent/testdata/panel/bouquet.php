<?php
// Add a bouquet too large for one config reply: MAIN answers it too_large, and
// stages it in parts for an agent that says "parts". Test-only.
require __DIR__ . '/common.php';
$rDb->query("INSERT INTO `bouquets` (`id`, `bouquet_name`, `bouquet_channels`, `bouquet_order`) VALUES (3, 'Everything', ?, 2)", '[' . implode(',', range(100000, 700000)) . ']');
echo "OK\n";
