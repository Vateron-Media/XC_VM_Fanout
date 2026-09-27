<?php
// php -S router: every request goes through the real ClusterApi.
require __DIR__ . '/common.php';

// Every request and every throwable, beside the harness DB: a fatal must not
// reach the response body (a node would read it as a reply), and php -S does not
// pass this script's STDERR through, so a 500 otherwise says nothing at all.
// interopNodeEnv prints this file when a test fails.
$rLog = static function (string $rLine): void {
	@file_put_contents((string) getenv('XCVM_INTEROP_DB') . '.log', $rLine, FILE_APPEND);
};
$rHeaders = [];
foreach ($_SERVER as $rKey => $rValue) {
	if (str_starts_with($rKey, 'HTTP_')) {
		$rHeaders[str_replace('_', '-', substr($rKey, 5))] = $rValue;
	}
}
if (isset($_SERVER['CONTENT_TYPE'])) {
	$rHeaders['Content-Type'] = $_SERVER['CONTENT_TYPE'];
}
try {
	$rRes = \XcVm\Domain\Cluster\ClusterApi::handle($rCrypto, [
	'method' => $_SERVER['REQUEST_METHOD'],
	'path' => (string) parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH),
	'query' => (string) ($_SERVER['QUERY_STRING'] ?? ''),
	'headers' => $rHeaders,
	'body' => (string) file_get_contents('php://input'),
	'ip' => $_SERVER['REMOTE_ADDR'] ?? '',
	// nginx's $server_port: the MAIN port a node reached (ClusterEndpoint::nodeUses).
	'port' => (int) ($_SERVER['SERVER_PORT'] ?? 0),
	], $rSettings, $rMain);
} catch (\Throwable $rE) {
	// The node would read a fatal's output as a reply, so it goes to the test's
	// stderr and the node gets a bare 500.
	$rLog('ClusterApi threw: ' . $rE . "\n");
	http_response_code(500);
	return true;
}
$rLog(sprintf("%s %s -> %d\n", $_SERVER['REQUEST_METHOD'], (string) parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), $rRes['status']));
http_response_code($rRes['status']);
foreach ($rRes['headers'] as $rName => $rValue) {
	header($rName . ': ' . $rValue);
}
echo $rRes['body'];
return true;
