<?php
// php -S router: every request goes through the real ClusterApi.
require __DIR__ . '/common.php';
$rHeaders = [];
foreach ($_SERVER as $rKey => $rValue) {
	if (str_starts_with($rKey, 'HTTP_')) {
		$rHeaders[str_replace('_', '-', substr($rKey, 5))] = $rValue;
	}
}
if (isset($_SERVER['CONTENT_TYPE'])) {
	$rHeaders['Content-Type'] = $_SERVER['CONTENT_TYPE'];
}
$rRes = \XcVm\Domain\Cluster\ClusterApi::handle($rCrypto, [
	'method' => $_SERVER['REQUEST_METHOD'],
	'path' => (string) parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH),
	'query' => (string) ($_SERVER['QUERY_STRING'] ?? ''),
	'headers' => $rHeaders,
	'body' => (string) file_get_contents('php://input'),
	'ip' => $_SERVER['REMOTE_ADDR'] ?? '',
], $rSettings, $rMain);
http_response_code($rRes['status']);
foreach ($rRes['headers'] as $rName => $rValue) {
	header($rName . ': ' . $rValue);
}
echo $rRes['body'];
return true;
