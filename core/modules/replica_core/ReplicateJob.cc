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
#include "replica_core/ReplicateJob.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"


// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.ReplicateJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

ReplicateJob::pointer
ReplicateJob::create (unsigned int               numReplicas,
                      std::string const&         database,
                      Controller::pointer const& controller,
                      bool                       progressReport,
                      bool                       errorReport) {
    return ReplicateJob::pointer (
        new ReplicateJob (numReplicas,
                          database,
                          controller,
                          progressReport,
                          errorReport));
}

ReplicateJob::ReplicateJob (unsigned int               numReplicas,
                            std::string const&         database,
                            Controller::pointer const& controller,
                            bool                       progressReport,
                            bool                       errorReport)

    :   Job (controller,
             "REPLICATE",
             progressReport,
             errorReport),

        _numReplicas (numReplicas),
        _database    (database) {
}

ReplicateJob::~ReplicateJob () {
}

void
ReplicateJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // TODO:
    // - harvest infor on existing chunk replicas of the database
    // - prepare an execution plan and submit the one
    // - make sure all requests are registered in the controller
    // - launch another thread which will be tracking results and then,
    //   depenidng on the outcome either finishing the execution,
    //   or proceeding with another stage - the actual replication, again
    //   in the same asynchronous manner

    setState(State::IN_PROGRESS);
}

void
ReplicateJob::cancelImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

}}} // namespace lsst::qserv::replica_core