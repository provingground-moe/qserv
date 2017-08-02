/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header

#include "replica_core/WorkerReplicationRequest.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/BlockPost.h"
#include "replica_core/SuccessRateGenerator.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerReplicationRequest");

/// Maximum duration for the request execution
const unsigned int maxDurationMillisec = 10000;

/// Random interval for the incremental execution
lsst::qserv::replica_core::BlockPost incrementIvalMillisec (1000, 2000);

/// Random generator of success/failure rates
lsst::qserv::replica_core::SuccessRateGenerator successRateGenerator(0.9);

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

std::string
WorkerReplicationRequest::status2string (CompletionStatus status) {
    switch (status) {
        case CompletionStatus::NONE:          return "NONE";
        case CompletionStatus::IN_PROGRESS:   return "IN_PROGRESS";
        case CompletionStatus::IS_CANCELLING: return "IS_CANCELLING";
        case CompletionStatus::CANCELLED:     return "CANCELLED";
        case CompletionStatus::SUCCEEDED:     return "SUCCEEDED";
        case CompletionStatus::FAILED:        return "FAILED";
    }
    throw std::logic_error("WorkerReplicationRequest::status2string - unhandled status: " + std::to_string(status));
}

WorkerReplicationRequest::pointer
WorkerReplicationRequest::create (WorkerReplicationRequest::Priority priority,
                                  const std::string&                 id,
                                  const std::string&                 database,
                                  unsigned int                       chunk) {

    return WorkerReplicationRequest::pointer (
        new WorkerReplicationRequest (priority,
                                      id,
                                      database,
                                      chunk)
    );
}

WorkerReplicationRequest::WorkerReplicationRequest (WorkerReplicationRequest::Priority priority,
                                                    const std::string&                 id,
                                                    const std::string&                 database,
                                                    unsigned int                       chunk)
    :   _priority(priority),
        _id      (id),
        _database(database),
        _chunk   (chunk),
        _status  (CompletionStatus::NONE),

        _durationMillisec(0)
{}

WorkerReplicationRequest::~WorkerReplicationRequest () {
}

void
WorkerReplicationRequest::beginProgress () {

    LOGS(_log, LOG_LVL_DEBUG, "beginProgress  "
         << "status: " << WorkerReplicationRequest::status2string(_status));

    switch (_status) {

        case CompletionStatus::NONE:
            _status = CompletionStatus::IN_PROGRESS;
            break;

        default:
            throw std::logic_error("WorkerReplicationRequest::beginProgress not allowed while in status: " +
                                    WorkerReplicationRequest::status2string(_status));
    }
}

bool
WorkerReplicationRequest::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, "execute  "
         << "status: " << WorkerReplicationRequest::status2string(_status));

    // Simulate request 'processing' for some maximum duration of time (milliseconds)
    // while making a progress through increments of random duration of time.
    // Success/failure modes will be also simulated using the corresponding generator.

   switch (_status) {

        case CompletionStatus::NONE:
            _status = CompletionStatus::IN_PROGRESS;
            break;

        case CompletionStatus::IN_PROGRESS:
            break;

        case CompletionStatus::IS_CANCELLING:
            _status = CompletionStatus::CANCELLED;
            throw WorkerReplicationCancelled();

        default:
            throw std::logic_error("WorkerReplicationRequest::execute not allowed while in status: " +
                                    WorkerReplicationRequest::status2string(_status));
    }
    
    _durationMillisec += incremental ? ::incrementIvalMillisec.wait() :
                                       ::maxDurationMillisec;

    if (_durationMillisec < ::maxDurationMillisec) return false;

    _status = ::successRateGenerator.success() ? CompletionStatus::SUCCEEDED :
                                                 CompletionStatus::FAILED;
    return true;
}

void
WorkerReplicationRequest::cancel () {

    LOGS(_log, LOG_LVL_DEBUG, "cancel  "
         << "status: " << WorkerReplicationRequest::status2string(_status));

    switch (_status) {

        case CompletionStatus::NONE:
        case CompletionStatus::CANCELLED:
            _status = CompletionStatus::CANCELLED;
            break;

        case CompletionStatus::IN_PROGRESS:
        case CompletionStatus::IS_CANCELLING:
            _status = CompletionStatus::IS_CANCELLING;
            break;

        default:
            throw std::logic_error("WorkerReplicationRequest::cancel not allowed while in status: " +
                                    WorkerReplicationRequest::status2string(_status));
    }
}
void
WorkerReplicationRequest::rollback () {

    LOGS(_log, LOG_LVL_DEBUG, "rollback  "
         << "status: " << WorkerReplicationRequest::status2string(_status));

    switch (_status) {

        case CompletionStatus::NONE:
        case CompletionStatus::IN_PROGRESS:
            _status = CompletionStatus::NONE;
            _durationMillisec = 0;
            break;

        case CompletionStatus::IS_CANCELLING:
            _status = CompletionStatus::CANCELLED;
            throw WorkerReplicationCancelled();
            break;

        default:
            throw std::logic_error("WorkerReplicationRequest::rollback not allowed while in status: " +
                                    WorkerReplicationRequest::status2string(_status));
    }
}
}}} // namespace lsst::qserv::replica_core

