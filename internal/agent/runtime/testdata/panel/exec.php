<?php
// The node's `console.php cluster:exec` (argv: cluster:exec [--types]), run
// as the agent runs it: the panel's real ClusterExecCommand, over the node
// tree under XCVM_INTEROP_NODE: config/cluster/agent.json (the agent's
// state), config/cluster/artefacts/ (its downloads), content/video/ (where
// an off-air video is placed). Test-only.
$rNode = rtrim((string) getenv('XCVM_INTEROP_NODE'), '/') . '/';
define('CONFIG_PATH', $rNode . 'config/');
define('VIDEO_PATH', $rNode . 'content/video/');
require rtrim((string) getenv('XCVM_PANEL_DIR'), '/') . '/tests/bootstrap.php';
if (!in_array('--types', $argv, true) && function_exists('posix_geteuid') && posix_geteuid() === 0) {
	// The node's tree is the agent's user's (xc_vm); a suite run as root
	// hands it to nobody, as the panel's own tests do (AgentUser).
	\XcVm\Tests\Support\AgentUser::own(CONFIG_PATH . 'cluster', VIDEO_PATH);
}
exit((new \XcVm\Cli\Commands\ClusterExecCommand())->execute(array_slice($argv, 2)));
