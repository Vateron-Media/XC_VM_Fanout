<?php
// argv: node_uuid, sign_pub hex, box_pub hex, eph_pub hex → JSON of issueFirst + panel key.
require __DIR__ . '/common.php';
$rOut = \XcVm\Domain\Cluster\EnrolmentService::issueFirst($rCrypto, 7, $argv[1], hex2bin($argv[2]), hex2bin($argv[3]), hex2bin($argv[4]), $rSettings, $rMain);
$rOut['token_sealed'] = base64_encode($rOut['token_sealed']);
// As LbInstallFlow writes it: issueFirst returns the lease raw, like the token.
$rLease = $rOut['lease'] ?? null;
unset($rOut['lease']);
$rOut += \XcVm\Domain\Cluster\LeaseService::wire($rLease);
$rOut['panel_sign_pub'] = base64_encode($rCrypto->info()['panel_sign_pub']);
echo json_encode($rOut, JSON_UNESCAPED_SLASHES);
