<?php
// argv: <flows.json> <lease_state.json> <lb_lease_fence> <lb_fence_drain_min>
// The node's PHP judging the lease state the agent wrote (Core\Cluster\NodeLease,
// ADR 0004 Phase 9, "The fence a lease's end draws"): its verdict, as JSON.
// The settings are passed as the stream endpoints pass theirs.
require __DIR__ . '/common.php';

use XcVm\Core\Cluster\NodeFlows;
use XcVm\Core\Cluster\NodeLease;

NodeFlows::usePath($argv[1]);
NodeLease::usePath($argv[2]);
echo json_encode(NodeLease::verdict(['lb_lease_fence' => (int) $argv[3], 'lb_fence_drain_min' => (int) $argv[4]]));
