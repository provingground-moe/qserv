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
#include "replica_core/PurgeJob.h"

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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.PurgeJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

PurgeJob::pointer
PurgeJob::create (std::string const&         databaseFamily,
                  unsigned int               numReplicas,
                  Controller::pointer const& controller,
                  callback_type              onFinish,
                  bool                       bestEffort,
                  int                        priority,
                  bool                       exclusive,
                  bool                       preemptable) {
    return PurgeJob::pointer (
        new PurgeJob (databaseFamily,
                      numReplicas,
                      controller,
                      onFinish,
                      bestEffort,
                      priority,
                      exclusive,
                      preemptable));
}

PurgeJob::PurgeJob (std::string const&         databaseFamily,
                    unsigned int               numReplicas,
                    Controller::pointer const& controller,
                    callback_type              onFinish,
                    bool                       bestEffort,
                    int                        priority,
                    bool                       exclusive,
                    bool                       preemptable)

    :   Job (controller,
             "PURGE",
             priority,
             exclusive,
             preemptable),

        _databaseFamily (databaseFamily),

        _numReplicas (numReplicas ?
                      numReplicas :
                      controller->serviceProvider().config()->replicationLevel(databaseFamily)),

        _onFinish   (onFinish),
        _bestEffort (bestEffort),

        _numIterations  (0),
        _numFailedLocks (0),

        _numLaunched (0),
        _numFinished (0),
        _numSuccess  (0) {
}

PurgeJob::~PurgeJob () {
    // Make sure all chunks are released
    for (auto const& chunkEntry: _chunk2worker2request) {
        unsigned int chunk = chunkEntry.first;
        release (chunk);
    }
}

PurgeJobResult const&
PurgeJob::getReplicaData () const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED)  return _replicaData;

    throw std::logic_error (
        "PurgeJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void
PurgeJob::track (bool          progressReport,
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
            os << "PurgeJob::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
    }
    if (progressReport)
        os << "PurgeJob::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;

    if (errorReport && _numLaunched - _numSuccess)
        replica_core::reportRequestState (_requests, os);
}

void
PurgeJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  _numIterations=" << _numIterations);

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<PurgeJob>();

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
PurgeJob::cancelImpl () {

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
            _controller->stopReplicaDelete (
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
PurgeJob::restart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");
    
    if (_findAllJob or (_numLaunched != _numFinished))
        throw std::logic_error ("PurgeJob::restart ()  not allowed in this object state");

    _requests.clear();

    _numFailedLocks = 0;

    _numLaunched = 0;
    _numFinished = 0;
}

void
PurgeJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<PurgeJob>();
        _onFinish(self);
    }
}

void
PurgeJob::onPrecursorJobFinish () {

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
    // Analyse results and prepare a deletion plan to remove extra
    // replocas for over-represented chunks
    //
    // IMPORTANT:
    //
    // 1) chunks for which the 'co-location' requirement was not met (as reported
    //    by the precursor job) will not be replicated. These chunks need to be
    //    "fixed-up" beforehand.
    //
    // 2) chunks in which any replica was found as not fully COMPLETE will not be
    //    included into the operation those would need to be repaired first.
    //
    // 3) chunks which alredy meet the replication level requirement
    //    will be ignored
    //
    // 4) chunks which were found locked by some other job will not be deleted
    //
    // 5) in case if more than one replica of the same chunk wil need to be
    //    deleted workers which have the highest number of replicas wil be chosen
    //
    // ATTENTION: the read-only workers will not be considered by
    //            the algorithm. Those workers are used by different kinds
    //            of jobs.

    FindAllJobResult const& replicaData = _findAllJob->getReplicaData ();

    // The 'occupancy' map or workers which will be used by the replica
    // removal algorithm later. The map is initialized below is based on
    // results reported by the precursor job and it will also be dynamically
    // updated by the algorithm as new replica removal requests for workers will
    // be issued.
    //
    // Note, this map includes chunks in any state.
    //
    std::map<std::string, size_t> worker2occupancy;

    // The number of replicas to be deleted for eligible chunks
    //
    std::map<unsigned int,int> chunk2numReplicas2delete;

    // Now initialize in those data structires

    for (auto const& chunkEntry: replicaData.chunks) {
        unsigned int const chunk = chunkEntry.first;

        for (auto const& databaseEntry: chunkEntry.second) {
            auto const& replicas = databaseEntry.second;

            for (auto const& workerEntry: databaseEntry.second) {
                std::string const& worker  = workerEntry.first;

                // Update worker's occupancy regardless of the chunk status
                worker2occupancy[worker]++;
    
                // Now check if this chunk meets the elegibilty criteras
    
                if (replicaData.colocation.at(chunk) and replicaData.complete.count(chunk)) {
    
                    // Check if this chunk has more replicas than required. Record the number
                    // of missing replicas to be deleted. This is going to be our candidate
                    // for purging.
                    //
                    // NOTE: Chunks which meet the 'colocaton' requirement should
                    //       have the same number of replicas in each database.

                    size_t const numReplicas = replicas.size();
                    if (numReplicas > _numReplicas) {
                        chunk2numReplicas2delete[chunk] = numReplicas - _numReplicas;
                    }
                }
            }
        }
    }











    
    //////////////////////////////////////////////////////////////
    // Analyse results and prepare a purge plan to shave off extra
    // replocas while trying to keep all nodes equally loaded
    
    FindAllJobResult const& replicaData = _findAllJob->getReplicaData ();

    std::map<unsigned int, std::list<std::string>>  chunk2workers;  // All workers which have a chunk
    std::map<std::string,  std::list<unsigned int>> worker2chunks;  // All chunks hosted by a worker

    for (auto const& replicaInfoCollection: replicaData.replicas) {
        for (auto const& replica: replicaInfoCollection)
            if (replica.status() == ReplicaInfo::Status::COMPLETE) {
                chunk2workers[replica.chunk ()].push_back(replica.worker());
                worker2chunks[replica.worker()].push_back(replica.chunk ());
            }
    }

    /////////////////////////////////////////////////////////////////////// 
    // Check which chunk replicas need to be eliminated. Then find the most
    // loaded worker holding the chunk and launch a delete request.
    //
    // TODO: this algorithm is way to simplistic as it won't take into
    //       an account other chunks. Ideally, it neees to be a two-pass
    //       scan.

    auto self = shared_from_base<PurgeJob>();

    for (auto const& entry: chunk2workers) {

        const unsigned int chunk{entry.first};

        // This collection is going to be modified
        std::list<std::string> replicas{entry.second};

        // Note that some chunks may have fewer replicas than required. In that case
        // the difference would be negative.
        const int numReplicas2delete =  replicas.size() - _numReplicas;

        for (int i = 0; i < numReplicas2delete; ++i) {

            // Find a candidate worker with the most number of chunks.
            // This worker will be select as the 'destinationWorker' for the new replica.

            std::string destinationWorker;
            size_t      numChunksPerDestinationWorker = 0;

            for (const auto &worker: replicas) {
                if (worker2chunks[worker].size() > numChunksPerDestinationWorker) {
                    destinationWorker = worker;
                    numChunksPerDestinationWorker = worker2chunks[worker].size();
                }
            }
            if (destinationWorker.empty()) {
                std::cerr << "failed to find the most populated worker for replicating chunk: " << chunk
                    << ", skipping this chunk" << std::endl;
                break;
            }
             
            // Remove this chunk with the worker to decrease the number of chunks per
            // the worker so that this updated stats will be accounted for later as
            // the replication process goes.
            std::remove(worker2chunks[destinationWorker].begin(),
                        worker2chunks[destinationWorker].end(),
                        chunk);

            // Also register the worker in the chunk2workers[chunk] to prevent it
            // from being select as the 'destinationWorker' for the same replica
            // in case if more than one replica needs to be created.
            // Also remove the worker from the local copy of the replicas, so that
            // it won't be tries again
            std::remove(replicas.begin(),
                        replicas.end(),
                        destinationWorker);
            
            // Finally, launch and register for further tracking the deletion
            // request.

            _requests.push_back (
                _controller->deleteReplica (
                    destinationWorker,
                    _database,
                    chunk,
                    [self] (DeleteRequest::pointer ptr) {
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
PurgeJob::onRequestFinish (DeleteRequest::pointer request) {

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
            _replicaData.workers[request->worker()] = true;
        } else {
            _replicaData.workers[request->worker()] = false;
        }
        // Make sure the chunk is released if this was the last deletion request
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
                    setState (State::FINISHED,
                              ExtendedState::SUCCESS);
                }
            } else {
                setState (State::FINISHED,
                          ExtendedState::FAILED);
            }
        }
    }

    // Note that access to the job's public API shoul not be locked while
    // notifying a caller (if the callback function was povided) in order to avoid
    // the circular deadlocks.

    if (_state == State::FINISHED)
        notify ();
}

void
PurgeJob::release (unsigned int chunk) {
    LOGS(_log, LOG_LVL_DEBUG, context() << "release  chunk=" << chunk);
    Chunk chunkObj {_databaseFamily, chunk};
    _controller->serviceProvider().chunkLocker().release(chunkObj);
}

}}} // namespace lsst::qserv::replica_core