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

WorkerReplicationRequest::pointer
WorkerReplicationRequest::create (WorkerReplicationRequest::PriorityLevel  priorityLevel,
                                  const std::string                       &id,
                                  const std::string                       &database,
                                  unsigned int                             chunk) {

    return WorkerReplicationRequest::pointer (
        new WorkerReplicationRequest (priorityLevel,
                                      id,
                                      database,
                                      chunk)
    );
}

WorkerReplicationRequest::WorkerReplicationRequest (WorkerReplicationRequest::PriorityLevel  priorityLevel,
                                                    const std::string                       &id,
                                                    const std::string                       &database,
                                                    unsigned int                             chunk)
    :   WorkerRequest ("REPLICATE",
                       priorityLevel,
                       id),

        _database        (database),
        _chunk           (chunk),
        _durationMillisec(0)
{}

WorkerReplicationRequest::~WorkerReplicationRequest () {
}

bool
WorkerReplicationRequest::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  db: "    << _database
         << "  chunk: " << _chunk);

    // Simulate request 'processing' for some maximum duration of time (milliseconds)
    // while making a progress through increments of random duration of time.
    // Success/failure modes will be also simulated using the corresponding generator.

   switch (status()) {

        case STATUS_IN_PROGRESS:
            break;

        case STATUS_IS_CANCELLING:
            setStatus(STATUS_CANCELLED);
            throw WorkerRequestCancelled();

        default:
            throw std::logic_error("WorkerReplicationRequest::execute not allowed while in status: " +
                                    WorkerReplicationRequest::status2string(status()));
    }
    
    _durationMillisec += incremental ? ::incrementIvalMillisec.wait() :
                                       ::maxDurationMillisec;

    if (_durationMillisec < ::maxDurationMillisec) return false;

    setStatus (::successRateGenerator.success() ? STATUS_SUCCEEDED :
                                                  STATUS_FAILED);
    return true;
}


}}} // namespace lsst::qserv::replica_core

