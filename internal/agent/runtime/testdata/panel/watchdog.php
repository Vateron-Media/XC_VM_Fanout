<?php
// stdin: an agent telemetry sample → the watchdog_data MAIN would store (JSON).
$rPanel = rtrim((string) getenv('XCVM_PANEL_DIR'), '/');
require $rPanel . '/tests/bootstrap.php';
$rTel = json_decode((string) stream_get_contents(STDIN), true);
echo json_encode(\XcVm\Domain\Cluster\HeartbeatService::toWatchdogData(is_array($rTel) ? $rTel : [], []));
