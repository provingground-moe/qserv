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
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/ErrorReporting.h"
#include "replica_core/ServiceProvider.h"

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
                    callback_type              onFinish) {
    return FindAllJob::pointer (
        new FindAllJob (database,
                        controller,
                        onFinish));
}

FindAllJob::FindAllJob (std::string const&         database,
                        Controller::pointer const& controller,
                        callback_type              onFinish)

    :   Job (controller,
             "FIND_ALL"),

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

    if (_state == State::FINISHED)  return _replicaData;

    throw std::logic_error (
        "FindAllJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void
FindAllJob::track (bool          progressReport,
                   bool          errorReport,
                   std::ostream& os) const {

    if (_state == State::FINISHED) return;
    
    BlockPost blockPost (1000, 2000);

    while (_numFinished < _numLaunched) {
        blockPost.wait();
        if (progressReport)
            os << "FindAllJob::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
    }
    if (progressReport)
        os << "FindAllJob::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;

    if (errorReport && _numLaunched - _numSuccess)
        replica_core::reportRequestState (_requests, os);
}

void
FindAllJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<FindAllJob>();

    for (auto const& worker: _controller->serviceProvider().config().workers()) {
        _requests.push_back (
            _controller->findAllReplicas (
                worker,
                _database,
                [self] (FindAllRequest::pointer request) {
                    self->onRequestFinish(request);
                },
                0,      /* priority */
                true,   /* keepTracking*/
                _id     /* jobId */
            )
        );
        _numLaunched++;
    }
    setState(State::IN_PROGRESS);
}

void
FindAllJob::cancelImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto const& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            _controller->stopReplicaFindAll (
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
    }
    _requests.clear();
    
    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void
FindAllJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<FindAllJob>();
        _onFinish(self);
    }
}

void
FindAllJob::onRequestFinish (FindAllRequest::pointer request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  database=" << request->database()
         << " worker=" << request->worker());

    // Ignore the callback if the job was cancelled   
    if (_state == State::FINISHED) return;

    // Update counters and object state if needed.

    {
        LOCK_GUARD;

        _numFinished++;
        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            _numSuccess++;
            _replicaData.replicas.emplace_back(request->responseData());
            _replicaData.workers[request->worker()] = true;
        } else {
            _replicaData.workers[request->worker()] = false;
        }
        if (_numFinished == _numLaunched) {
            setState (
                State::FINISHED,
                _numSuccess == _numLaunched ? ExtendedState::SUCCESS : ExtendedState::FAILED);
        }
    }
    
    // Note that access to the job's public API shoul not be locked while
    // notifying a caller (if the callback function was povided) in order to avoid
    // the circular deadlocks.

    if (_state == State::FINISHED)
        notify ();
}

}}} // namespace lsst::qserv::replica_core