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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.ReplicateJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

ReplicateJob::pointer
ReplicateJob::create (unsigned int               numReplicas,
                      std::string const&         database,
                      Controller::pointer const& controller,
                      callback_type              onFinish,
                      bool                       bestEffort) {
    return ReplicateJob::pointer (
        new ReplicateJob (numReplicas,
                          database,
                          controller,
                          onFinish,
                          bestEffort));
}

ReplicateJob::ReplicateJob (unsigned int               numReplicas,
                            std::string const&         database,
                            Controller::pointer const& controller,
                            callback_type              onFinish,
                            bool                       bestEffort)

    :   Job (controller,
             "REPLICATE"),

        _numReplicas (numReplicas),
        _database    (database),
        _onFinish    (onFinish),
        _bestEffort (bestEffort),

        _numLaunched (0),
        _numFinished (0),
        _numSuccess  (0) {
}

ReplicateJob::~ReplicateJob () {
}

ReplicateJobResult const&
ReplicateJob::getReplicaData () const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED)  return _replicaData;

    throw std::logic_error (
        "ReplicateJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void
ReplicateJob::track (bool          progressReport,
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
            os << "ReplicateJob::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
    }
    if (progressReport)
        os << "ReplicateJob::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;

    if (errorReport && _numLaunched - _numSuccess)
        replica_core::reportRequestState (_requests, os);
}

void
ReplicateJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<ReplicateJob>();

    _findAllJob = FindAllJob::create (
        _database,
        _controller,
        [self] (FindAllJob::pointer job) {
            self->onPrecursorJobFinish();
        }
    );
    _findAllJob->start();

    setState(State::IN_PROGRESS);
}

void
ReplicateJob::cancelImpl () {

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
    _requests.clear();
    
    _numLaunched = 0;
    _numFinished = 0;
    _numSuccess  = 0;

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void
ReplicateJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<ReplicateJob>();
        _onFinish(self);
    }
}

void
ReplicateJob::onPrecursorJobFinish () {

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
    // Analyse results and prepare a replication plan to create extra
    // replocas for under-represented chunks 
    
    FindAllJobResult const& replicaData = _findAllJob->getReplicaData ();

    std::set<std::string> failedWorkers;    // Workers to be avoided when deciding on
                                            // locations of news replicas
    for (auto const& entry: replicaData.workers)
        if (!entry.second)
            failedWorkers.insert(entry.first);

    std::map<unsigned int, std::list<std::string>>  chunk2workers;  // All workers which have a chunk
    std::map<std::string,  std::list<unsigned int>> worker2chunks;  // All chunks hosted by a worker

    for (auto const& replicaInfoCollection: replicaData.replicas) {
        for (auto const& replica: replicaInfoCollection)
            if (replica.status() == ReplicaInfo::Status::COMPLETE) {
                chunk2workers[replica.chunk ()].push_back(replica.worker());
                worker2chunks[replica.worker()].push_back(replica.chunk ());
            }
    }

    /////////////////////////////////////////////////////////////////////
    // Check which chunks are under-represented. Then find a least loaded
    // worker and launch a replication request.

    auto self = shared_from_base<ReplicateJob>();

    // This counter will be used for optimization purposes as the upper limit for
    // the number of chunks per worker in the load balancing algorithm below.
    const size_t numUniqueChunks = chunk2workers.size();

    for (auto const& entry: chunk2workers) {

        unsigned int const chunk{entry.first};

        // Take a copy of the non-modified list of workers with chunk's replicas
        // and cache it here to know which workers are allowed to be used as reliable
        // sources vs chunk2workers[chunk] which will be modified below as new replicas
        // will get created.
        std::list<std::string> const replicas{entry.second};

        // Pick the first worker which has this chunk as the 'sourceWorker'
        // in case if we'll decide to replica the chunk within the loop below
        std::string const& sourceWorker = *(replicas.begin());

        // Note that some chunks may have more replicas than required. In that case
        // the difference would be negative.
        int const numReplicas2create = _numReplicas - replicas.size();

        for (int i = 0; i < numReplicas2create; ++i) {

            // Find a candidate worker with the least number of chunks.
            // This worker will be select as the 'destinationWorker' for the new replica.
            //
            // ATTENTION: workers which were previously found as 'failed'
            //            are going to be excluded from the search.

            std::string destinationWorker;
            size_t      numChunksPerDestinationWorker = numUniqueChunks;

            for (auto const& worker: _controller->serviceProvider().config()->workers()) {

                // Exclude failed workers
                if (failedWorkers.count(worker)) continue;

                // Exclude workers which already have this chunk, or for which
                // there is an outstanding replication requsts. Both kinds of
                // replicas are registered in chunk2workers[chunk]

                if (chunk2workers[chunk].end() == std::find(chunk2workers[chunk].begin(),
                                                            chunk2workers[chunk].end(),
                                                            worker)) {
                    if (worker2chunks[worker].size() < numChunksPerDestinationWorker) {
                        destinationWorker = worker;
                        numChunksPerDestinationWorker = worker2chunks[worker].size();
                    }
                }
            }
            if (destinationWorker.empty()) {
                LOGS(_log, LOG_LVL_ERROR,
                    context() << "failed to find the least populated worker for replicating chunk: " << chunk
                              << ", skipping this chunk");
                break;
            }
             
            // Register this chunk with the worker to bump the number of chunks per
            // the worker so that this updated stats will be accounted for later as
            // the replication process goes.
            worker2chunks[destinationWorker].push_back(chunk);

            // Also register the worker in the chunk2workers[chunk] to prevent it
            // from being select as the 'destinationWorker' for the same replica
            // in case if more than one replica needs to be created.
            chunk2workers[chunk].push_back(destinationWorker);
            
            // Finally, launch and register for further tracking the replication
            // request.

            _requests.push_back (
                _controller->replicate (
                    destinationWorker,
                    sourceWorker,
                    _database,
                    chunk,
                    [self] (ReplicationRequest::pointer ptr) {
                        self->onRequestFinish(ptr);
                    },
                    0,      /* priority */
                    true,   /* keepTracking */
                    _id     /* jobId */
                )
            );
            _numLaunched++;
        }
    }
}

void
ReplicateJob::onRequestFinish (ReplicationRequest::pointer request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  database=" << request->database()
         << " worker=" << request->worker()
         << " chunk=" << request->chunk());

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