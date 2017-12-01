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
#include "replica_core/RebalanceJob.h"

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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.RebalanceJob");

template <class COLLECTION>
void countJobStates (size_t&           numLaunched,
                     size_t&           numFinished,
                     size_t&           numSuccess,
                     COLLECTION const& collection) {

    using namespace lsst::qserv::replica_core;

    numLaunched = collection.size();
    numFinished = 0;
    numSuccess  = 0;

    for (auto const& ptr: collection) {
        if (ptr->state() == Job::State::FINISHED) {
            numFinished++;
            if (ptr->extendedState() == Job::ExtendedState::SUCCESS)
                numSuccess++;
        }
    }
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

RebalanceJob::pointer
RebalanceJob::create (std::string const&         databaseFamily,
                      unsigned int               startPercent,
                      unsigned int               stopPercent,
                      Controller::pointer const& controller,
                      callback_type              onFinish,
                      bool                       bestEffort,
                      int                        priority,
                      bool                       exclusive,
                      bool                       preemptable) {
    return RebalanceJob::pointer (
        new RebalanceJob (databaseFamily,
                          startPercent,
                          stopPercent,
                          controller,
                          onFinish,
                          bestEffort,
                          priority,
                          exclusive,
                          preemptable));
}

RebalanceJob::RebalanceJob (std::string const&         databaseFamily,
                            unsigned int               startPercent,
                            unsigned int               stopPercent,
                            Controller::pointer const& controller,
                            callback_type              onFinish,
                            bool                       bestEffort,
                            int                        priority,
                            bool                       exclusive,
                            bool                       preemptable)

    :   Job (controller,
             "REBALANCE",
             priority,
             exclusive,
             preemptable),

        _databaseFamily (databaseFamily),
        _startPercent   (startPercent),
        _stopPercent    (stopPercent),

        _onFinish   (onFinish),
        _bestEffort (bestEffort),

        _numIterations (0) {

    // Neither limit should be outside a range of [10,50], and the difference shouldn't
    // be less than 5%.

    if ((_startPercent < 10 or _startPercent > 50) or
        (_stopPercent  <  5 or _stopPercent  > 45) or
        (_stopPercent  > _startPercent) or (_stopPercent  - _startPercent < 5))
        throw std::invalid_argument (
                "RebalanceJob::RebalanceJob ()  invalid values of parameters 'startPercent' or 'stopPercent'");
}

RebalanceJob::~RebalanceJob () {
    // Make sure all chuks locked by this job are released
    _controller->serviceProvider().chunkLocker().release(_id);
}

RebalanceJobResult const&
RebalanceJob::getReplicaData () const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "getReplicaData");

    if (_state == State::FINISHED)  return _replicaData;

    throw std::logic_error (
        "RebalanceJob::getReplicaData  the method can't be called while the job hasn't finished");
}

void
RebalanceJob::track (bool          progressReport,
                     bool          errorReport,
                     bool          chunkLocksReport,
                     std::ostream& os) const {

    if (_state == State::FINISHED) return;

    if (_findAllJob)
        _findAllJob->track (progressReport,
                            errorReport,
                            chunkLocksReport,
                            os);
    
    BlockPost blockPost (1000, 2000);

    while (true) {

        blockPost.wait();

        size_t numLaunched;
        size_t numFinished;
        size_t numSuccess;

        ::countJobStates (numLaunched, numFinished, numSuccess,
                          _moveReplicaJobs);

        if (progressReport)
            os  << "RebalanceJob::track()  "
                << "launched: " << numLaunched << ", "
                << "finished: " << numFinished << ", "
                << "success: "  << numSuccess
                << std::endl;

        if (chunkLocksReport)
            os  << "RebalanceJob::track()  <LOCKED CHUNKS>  jobId: " << _id << "\n"
                << _controller->serviceProvider().chunkLocker().locked (_id);

        if (numLaunched == numFinished) break;
    }
}

void
RebalanceJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl  _numIterations=" << _numIterations);

    ++_numIterations;

    // Launch the chained job to get chunk disposition

    auto self = shared_from_base<RebalanceJob>();

    _findAllJob = FindAllJob::create (
        _databaseFamily,
        _controller,
        [self] (FindAllJob::pointer job) { self->onPrecursorJobFinish(); }
    );
    _findAllJob->start();

    setState(State::IN_PROGRESS);
}

void
RebalanceJob::cancelImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // The algorithm will also clear resources taken by various
    // locally created objects.

    if (_findAllJob && (_findAllJob->state() != State::FINISHED))
        _findAllJob->cancel();

    _findAllJob = nullptr;

    for (auto const& ptr: _moveReplicaJobs) {
        ptr->cancel();
    }
    _moveReplicaJobs.clear();

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void
RebalanceJob::restart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    size_t numLaunched;
    size_t numFinished;
    size_t numSuccess;

    ::countJobStates (numLaunched, numFinished, numSuccess,
                      _moveReplicaJobs);

    if (_findAllJob or (numLaunched != numFinished))
        throw std::logic_error ("RebalanceJob::restart ()  not allowed in this object state");

    _moveReplicaJobs.clear();

    // Take a fresh snapshot opf chunk disposition within the cluster
    // to see what else can be rebalanced. Note that this is going to be
    // a lengthy operation allowing other on-going activities locking chunks
    // to be finished before the current job will get another chance
    // to rebalance (if needed).

    auto self = shared_from_base<RebalanceJob>();

    _findAllJob = FindAllJob::create (
        _databaseFamily,
        _controller,
        [self] (FindAllJob::pointer job) { self->onPrecursorJobFinish(); }
    );
    _findAllJob->start();
}

void
RebalanceJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<RebalanceJob>();
        _onFinish(self);
    }
}

void
RebalanceJob::onPrecursorJobFinish () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "onPrecursorJobFinish");

    do {
        // This lock will be automatically release beyon this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;
    
        // Ignore the callback if the job was cancelled   
        if (_state == State::FINISHED) return;
    
        ////////////////////////////////////////////////////////////////////
        // Do not proceed with the replication effort unless running the job
        // under relaxed condition.
    
        if (not _bestEffort && (_findAllJob->extendedState() != ExtendedState::SUCCESS)) {
            setState(State::FINISHED, ExtendedState::FAILED);
            break;
        }

        ///////////////////////////////////////////////
        // Analyse results and prepare a rebalance plan

        // FindAllJobResult const& replicaData = _findAllJob->getReplicaData ();

        size_t numFailedLocks = 0;

        // TODO: The analysis and job submission code goes here. Chunks will
        // be attempted to be locked.

        ;
        
        // Finish right away if no jobs were submitted and no failed attempts
        // to lock chunks were encountered.

        if (not _moveReplicaJobs.size()) {
            if (not numFailedLocks) {
                setState (State::FINISHED, ExtendedState::SUCCESS);
            } else {
                // Start another iteration by requesting the fresh state of
                // chunks within the family or until it all fails.
                restart ();
            }
        }

    } while (false);
    
    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED)
        notify();
}

void
RebalanceJob::onJobFinish (MoveReplicaJob::pointer job) {

    std::string  const databaseFamily    = job->databaseFamily(); 
    unsigned int const chunk             = job->chunk();
    std::string  const sourceWorker      = job->sourceWorker(); 
    std::string  const destinationWorker = job->destinationWorker(); 

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onJobFinish"
         << "  databaseFamily="    << databaseFamily
         << "  chunk="             << chunk
         << "  sourceWorker="      << sourceWorker
         << "  destinationWorker=" << destinationWorker);

    do {
        // This lock will be automatically release beyon this scope
        // to allow client notifications (see the end of the method)
        LOCK_GUARD;

        // Make sure the chunk is released if this was the last job in
        // its scope regardless of the completion status of the job.

        _chunk2jobs.at(chunk).erase(sourceWorker);
        if (_chunk2jobs.at(chunk).empty()) {
            _chunk2jobs.erase(chunk);
            Chunk chunkObj {_databaseFamily, chunk};
            _controller->serviceProvider().chunkLocker().release(chunkObj);
        }

        // Ignore the callback if the job was cancelled   
        if (_state == State::FINISHED) return;

        // Update counters and object state if needed.

        if (job->extendedState() == Job::ExtendedState::SUCCESS) {
            
            // Copy over data from the job

            MoveReplicaJobResult const& replicaData = job->getReplicaData();

            for (auto const& replica: replicaData.createdReplicas) {
                _replicaData.createdReplicas.emplace_back(replica);
            }
            for (auto const& databaseEntry: replicaData.createdChunks.at(chunk)) {
                std::string const& database = databaseEntry.first;
                ReplicaInfo const& replica  = databaseEntry.second.at(destinationWorker);

                _replicaData.createdChunks[chunk][database][destinationWorker] = replica;
            }
            for (auto const& replica: replicaData.deletedReplicas) {
                _replicaData.deletedReplicas.emplace_back(replica);
            }
            for (auto const& databaseEntry: replicaData.deletedChunks.at(chunk)) {
                std::string const& database = databaseEntry.first;
                ReplicaInfo const& replica  = databaseEntry.second.at(sourceWorker);

                _replicaData.deletedChunks[chunk][database][sourceWorker] = replica;
            }
        }
        
        // Evaluate the status of on-going operations to see if the job
        // has finished.

        size_t numLaunched;
        size_t numFinished;
        size_t numSuccess;
    
        ::countJobStates (numLaunched, numFinished, numSuccess,
                          _moveReplicaJobs);

        if (numFinished == numLaunched) {
            if (numSuccess == numLaunched) {
                // Make another iteration (and another one, etc. as many as needed)
                // before it succeeds or fails.
                //
                // NOTE: a condition for this jobs is to succeed is evaluated in
                //       the precursor job completion code.
                restart ();
            } else {
                setState (State::FINISHED, ExtendedState::FAILED);
            }
        }

    } while (false);

    // Client notification should be made from the lock-free zone
    // to avoid possible deadlocks
    if (_state == State::FINISHED)
        notify ();
}

}}} // namespace lsst::qserv::replica_core