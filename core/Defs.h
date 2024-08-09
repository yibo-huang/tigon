//
// Created by Yi Lu on 9/10/18.
//

#pragma once

namespace star
{

enum class ExecutorStatus {
	START,
	CLEANUP,
	C_PHASE,
	S_PHASE,
	Analysis,
	LockRequest,
	LockResponse,
	Execute,
	Kiva_READ,
	Kiva_COMMIT,
	Aria_COLLECT_XACT,
	Aria_READ,
	Aria_COMMIT,
	AriaFB_READ,
	STOP,
	EXIT
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

} // namespace star
