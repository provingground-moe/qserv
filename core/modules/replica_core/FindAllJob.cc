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
#include "replica_core/FindAllJob.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"


// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.FindAllJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

FindAllJob::pointer
FindAllJob::create (std::string const&         database,
                    Controller::pointer const& controller,
                    callback_type              onFinish,
                    bool                       progressReport,
                    bool                       errorReport) {
    return FindAllJob::pointer (
        new FindAllJob (database,
                        controller,
                        onFinish,
                        progressReport,
                        errorReport));
}

FindAllJob::FindAllJob (std::string const&         database,
                        Controller::pointer const& controller,
                        callback_type              onFinish,
                        bool                       progressReport,
                        bool                       errorReport)

    :   Job (controller,
             "FIND_ALL",
             progressReport,
             errorReport),

        _database (database),
        _onFinish (onFinish),

        _numLaunched (0),
        _numFinished (0),
        _numSuccess  (0) {
}

FindAllJob::~FindAllJob () {
}

FindAllJobResult const&
FindAllJob::getReplicaData () const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_status == Status::FINISHED)  return _replicaData;

    throw std::logic_error (
        "FindAllJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void
FindAllJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // TODO:
    // - launch requests against all worker nodes
    // - register all requests in the collection
    // - specify 

    setState(State::IN_PROGRESS);
}

void
FindAllJob::cancelImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    setState(State::FINISHED, ExtendedState::CANCELLED);
}
void
FindAllJob::onRequestFinish (FindAllRequest::pointer request) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onRequestFinish  database=" << request->database());

    // Do not allow the lock guard to "escape" the scope as we don't want to
    // to deadlock when notifying a subscriber on the completion of the operation

    {
        LOCK_GUARD;

        // TODO:
        // - Check the completion status of the request and increment the counters
        //   accordingly
        // - if successful completion then update member _replicaData
        // - if all requests have finished then update object state accordingly 
    }
    
    // Note that access to the job's public API shoul not be locked while
    // notifying a caller (if the callback function was povided)

    if (_status == Status::FINISHED) {
        if (_onFinish) {
            auto self = shared_from_base<FindAllJob>();
            _onFinish(self);
        }
    }
}

}}} // namespace lsst::qserv::replica_core