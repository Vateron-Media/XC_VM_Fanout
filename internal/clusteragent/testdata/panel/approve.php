<?php
// argv: SAS → the admin's decision on server 7's pending code enrolment.
require __DIR__ . '/common.php';
echo \XcVm\Domain\Cluster\EnrolCodeService::approve($rCrypto, 7, $argv[1], $rSettings, $rMain);
