<?php
// argv: SAS [server id, default 7] → the admin's decision on its pending code enrolment.
require __DIR__ . '/common.php';
echo \XcVm\Domain\Cluster\EnrolCodeService::approve($rCrypto, (int) ($argv[2] ?? 7), $argv[1], $rSettings, $rMain);
