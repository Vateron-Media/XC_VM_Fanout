<?php
// argv: none → a fresh enrolment code for server 7 at this harness's URL.
require __DIR__ . '/common.php';
echo \XcVm\Domain\Cluster\EnrolCodeService::generate($rCrypto, 7, 'http://127.0.0.1:' . getenv('XCVM_INTEROP_PORT'))['code'];
