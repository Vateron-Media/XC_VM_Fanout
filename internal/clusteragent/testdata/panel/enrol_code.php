<?php
// argv: [server id, default 7] → a fresh enrolment code for it at this harness's URL.
require __DIR__ . '/common.php';
echo \XcVm\Domain\Cluster\EnrolCodeService::generate($rCrypto, (int) ($argv[1] ?? 7), 'http://127.0.0.1:' . getenv('XCVM_INTEROP_PORT'))['code'];
