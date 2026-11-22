<?php
// Block 80,000 IPv6 addresses written out in full and log a reset: the whole
// blocklist section is then too large for one config reply, and MAIN stages it
// in parts for an agent that says "blocklist_parts". Test-only.
require __DIR__ . '/common.php';
foreach (array_chunk(range(0, 79999), 500) as $rChunk) {
	$rDb->query('INSERT INTO `blocked_ips` (`ip`, `date`) VALUES ' . implode(', ', array_map(static fn(int $i): string => sprintf("('2001:0db8:0000:0000:0000:0000:%04x:%04x', 0)", ($i >> 16) & 0xffff, $i & 0xffff), $rChunk)));
}
\XcVm\Core\Cluster\BlocklistChanges::reset('ip');
echo "OK\n";
