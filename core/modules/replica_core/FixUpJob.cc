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
#include "replica_core/FixUpJob.h"

// System headers

#include <algorithm>
#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/BlockPost.h"
#include "replica_core/ErrorReporting.h"
#include "replica_core/ServiceProvider.h"


// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.FixUpJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

FixUpJob::pointer
FixUpJob::create (std::string const&         databaseFamily,
                  Controller::pointer const& controller,
                  callback_type              onFinish,
                  bool                       bestEffort,
                  int                        priority,
                  bool                       exclusive,
                  bool                       preemptable) {
    return FixUpJob::pointer (
        new FixUpJob (databaseFamily,
                      controller,
                      onFinish,
                      bestEffort,
                      priority,
                      exclusive,
                      preemptable));
}

FixUpJob::FixUpJob (std::string const&         databaseFamily,
                    Controller::pointer const& controller,
                    callback_type              onFinish,
                    bool                       bestEffort,
                    int                        priority,
                    bool                       exclusive,
                    bool                       preemptable)

    :   Job (controller,
             "FIXUP",
             priority,
             exclusive,
             preemptable),

        _databaseFamily (databaseFamily),
        _onFinish       (onFinish),
        _bestEffort     (bestEffort),

        _numIterations  (0),
        _numFailedLocks (0),

        _numLaunched (0),
        _numFinished (0),
        _numSuccess  (0) {
}

FixUpJob::~FixUpJob () {
    for (auto const& chunkEntry: _chunk2worker2request) {
        unsigned int chunk = chunkEntry.first;
        release (chunk);
    }
}

FixUpJobResult const&
FixUpJob::getReplicaData () const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED)  return _replicaData;

    throw std::logic_error (
        "FixUpJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void
FixUpJob::track (bool          progressReport,
                 bool          errorReport,
                 std::ostream& os) const {

    if (_state == State::FINISHED) return;

    if (_findAllJob)
        _findAllJob->track (progressReport,
                            errorReport,
                            os);
    
    BlockPost blockPost (1000, 2000);

    while (_numFinished < _numLaunched) {
        blockPost.wait();
        if (progressReport)
            os << "FixUpJob::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
    }
    if (progressReport)
        os << "FixUpJob::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;

    if (errorReport && _numLaunched - _numSuccess)
        replica_core::reportRequestState (_requests, os);
}

void
FixUpJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  _numIterations=" << _numIterations);

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<FixUpJob>();

    _findAllJob = FindAllJob::create (
        _databaseFamily,
        _controller,
        [self] (FindAllJob::pointer job) {
            self->onPrecursorJobFinish();
        }
    );
    _findAllJob->start();

    setState(State::IN_PROGRESS);
}

void
FixUpJob::cancelImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob && (_findAllJob->state() != State::FINISHED))
        _findAllJob->cancel();

    _findAllJob = nullptr;

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto const& ptr: _requests) {
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED)
            _controller->stopReplication (
                ptr->worker(),
                ptr->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
    }

    _chunk2worker2request.clear();
    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;

    setState(State::FINISHED, ExtendedState::CANCELLED);
}


void
FixUpJob::restart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");
    
    if (_findAllJob or (_numLaunched != _numFinished))
        throw std::logic_error ("FixUpJob::restart ()  not allowed in this object state");

    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;
}

void
FixUpJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<FixUpJob>();
        _onFinish(self);
    }
}

void
FixUpJob::onPrecursorJobFinish () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    // Ignore the callback if the job was cancelled   
    if (_state == State::FINISHED) return;

    LOCK_GUARD;

    ////////////////////////////////////////////////////////////////////
    // Do not proceed with the replication effort unless running the job
    // under relaxed condition.

    if (!_bestEffort && (_findAllJob->extendedState() != ExtendedState::SUCCESS)) {
        setState(State::FINISHED, ExtendedState::FAILED);
        return;
    }

    /////////////////////////////////////////////////////////////////
    // Analyse results and prepare a replication plan to fix chunk
    // co-location for under-represented chunks

    FindAllJobResult const& replicaData = _findAllJob->getReplicaData ();

    auto self = shared_from_base<FixUpJob>();

    for (auto const& chunk2workers: replicaData.isColocated) {
        unsigned int chunk = chunk2workers.first;

        for (auto const& worker2colocated: chunk2workers.second) {
            std::string const& destinationWorker = worker2colocated.first;
            bool        const  isColocated       = worker2colocated.second;

            if (isColocated) continue;

            // Chunk locking is mandatory. If it's not possible to do this now then
            // the job will need to make another attempt later.
    
            if (not _controller->serviceProvider().chunkLocker().lock({_databaseFamily, chunk}, _id)) {
                ++_numFailedLocks;
                continue;
            }

            // Iterate over all participating databases, find the ones which aren't
            // represented on the worker, find a suitable source worker which has
            // a complete chunk for the database and which (the worker) is not the same
            // as the current one and submite the replication request.

            for (auto const& database: replicaData.databases.at(chunk)) {
                if (not replicaData.chunks.at(chunk).count(database)) {
 
                    // Finding a source worker first
                    std::string sourceWorker;
                    for (auto const& worker: replicaData.complete.at(chunk).at(database)) {
                        if (worker != destinationWorker) {
                            sourceWorker = worker;
                            break;
                        }
                    }
                    if (sourceWorker.empty()) {
                        LOGS(_log, LOG_LVL_ERROR, context()
                             << "onPrecursorJobFinish  failed to find a source worker for chunk: "
                             << chunk << " and database: " << database);
                        release(chunk);
                        setState (State::FINISHED, ExtendedState::FAILED);
                    }

                    // Finally, launch the replication request and register it for further
                    // tracking (or cancellation, should the one be requested)
        
                    ReplicationRequest::pointer ptr =
                        _controller->replicate (
                            destinationWorker,
                            sourceWorker,
                            database,
                            chunk,
                            [self] (ReplicationRequest::pointer ptr) {
                                self->onRequestFinish(ptr);
                            },
                            0,      /* priority */
                            true,   /* keepTracking */
                            _id     /* jobId */
                        );
        
                    _chunk2worker2request[chunk][destinationWorker] = ptr;
                    _requests.push_back (ptr);
                    _numLaunched++;
                }
            }
        }
    }

    // Finish right away if no problematic chunks found
    if (not _requests.size()) {
        if (not _numFailedLocks) {
            setState (State::FINISHED, ExtendedState::SUCCESS);
        } else {
            // Some of the chuks were locked and yet, no sigle request was
            // lunched. Hence we should start another iteration by requesting
            // the fresh state of the chunks within the family.
            restart ();
        }
    }
}

void
FixUpJob::onRequestFinish (ReplicationRequest::pointer request) {

    std::string  const database = request->database(); 
    std::string  const worker   = request->worker(); 
    unsigned int const chunk    = request->chunk();

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish"
         << "  database=" << database
         << "  worker="   << worker
         << "  chunk="    << chunk);

    // Ignore the callback if the job was cancelled   
    if (_state == State::FINISHED) {
        release (chunk);
        return;
    }

    // Update counters and object state if needed.
    {
        LOCK_GUARD;

        _numFinished++;
        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            _numSuccess++;
            _replicaData.replicas.emplace_back(request->responseData());
            _replicaData.chunks[chunk][database][worker] = request->responseData();
            _replicaData.workers[worker] = true;
        } else {
            _replicaData.workers[worker] = false;
        }
        
        // Make sure the chunk is released if this was the last replication request
        // in its scope.
        _chunk2worker2request.at(chunk).erase(worker);
        if (not _chunk2worker2request.count(chunk)) release (chunk);

        if (_numFinished == _numLaunched) {
            if (_numSuccess == _numLaunched) {
                if (_numFailedLocks) {
                    // Make another iteration (and another one, etc. as many as needed)
                    // before it succeeds or fails.
                    restart ();
                } else {
                    setState (State::FINISHED, ExtendedState::SUCCESS);
                }
            } else {
                setState (State::FINISHED, ExtendedState::FAILED);
            }
        }
    }

    // Note that access to the job's public API should not be locked while
    // notifying a caller (if the callback function was povided) in order to avoid
    // the circular deadlocks.

    if (_state == State::FINISHED)
        notify ();
}

void
FixUpJob::release (unsigned int chunk) {
    LOGS(_log, LOG_LVL_DEBUG, context() << "release  chunk=" << chunk);
    Chunk chunkObj {_databaseFamily, chunk};
    _controller->serviceProvider().chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica_core