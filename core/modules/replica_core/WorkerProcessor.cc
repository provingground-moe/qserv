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

#include "replica_core/WorkerProcessor.h"

// System headers

#include <stdexcept>
#include <iostream>

// Qserv headers


// This macro to appear witin each block which requires thread safety

#define THREAD_SAFE_BLOCK \
std::lock_guard<std::mutex> lock(_mtx);

namespace lsst {
namespace qserv {
namespace replica_core {


WorkerProcessor::pointer
WorkerProcessor::create (ServiceProvider::pointer serviceProvider) {
    return WorkerProcessor::pointer (
        new WorkerProcessor (serviceProvider));
}

WorkerProcessor::WorkerProcessor (ServiceProvider::pointer serviceProvider)
    :   _serviceProvider (serviceProvider),
        _state           (State::STATE_IS_STOPPED)

{}

WorkerProcessor::~WorkerProcessor () {
}


void
WorkerProcessor::run () {

    THREAD_SAFE_BLOCK {

        if (_state == State::STATE_IS_STOPPED) {
            
            const size_t numThreads = _serviceProvider->config()->workerNumProcessingThreads();
            if (!numThreads) throw new std::out_of_range("the number of procesisng threads can't be 0");
        
            // Create threads if needed
        
            if (_threads.empty()) {
                WorkerProcessor::pointer self = shared_from_this();
                for (size_t i=0; i < numThreads; ++i) {
                    _threads.push_back(WorkerProcessorThread::create(self));
                }
            }
        
            // Tell each thread to run
        
            for (auto &t: _threads) {
                t->run();
            }
            _state = State::STATE_IS_RUNNING;
        }
    }
}

void
WorkerProcessor::stop () {

    THREAD_SAFE_BLOCK {

        if (_state != State::STATE_IS_RUNNING) {
            
            // Tell each thread to stop.
        
            for (auto &t: _threads) {
                t->stop();
            }
            
            // Begin transitioning to the final state via this intermediate one.
            // The transition will finish asynchronious when all threads will report
            // desired changes in their states.

            _state = State::STATE_IS_STOPPING;
        }
    }
}

void
WorkerProcessor::enqueueForReplication (const proto::ReplicationRequestReplicate &request,
                                        proto::ReplicationResponseReplicate &response) {

    THREAD_SAFE_BLOCK {

        std::cout << "WorkerProcessor::enqueueForReplication "
                  << " id="       << request.id()       << ","
                  << " database=" << request.database() << ","
                  << " chunk="    << request.chunk()    << std::endl;
    

        // TODO: run the sanity check to ensure no such request is found in any
        //       of the queue. Return 'DUPLICATE' error status if teh one is found.

        WorkerReplicationRequest::pointer workerRequest =
            WorkerReplicationRequest::create (
                WorkerReplicationRequest::Priority::LOW,
                request.id(),
                request.database(),
                request.chunk());

        _newRequests.push(workerRequest);
        
        // TODO: send a signal via a condition variable to notify
        // processing threads that there is a new request.
 
        response.set_status (proto::ReplicationStatus::QUEUED);
    }
}

void
WorkerProcessor::dequeueOrCancel (const proto::ReplicationRequestStop &request,
                                  proto::ReplicationResponseStop      &response) {

    THREAD_SAFE_BLOCK {

        std::cout << "WorkerProcessor::dequeueOrCancel "
                  << " id=" << request.id() <<  std::endl;
    
        // TODO: search for a request with this identifier in all queues.
        // Then take proper actions depending on a queue:
        // - just remove it fom the new requests queue
        // - find a processing thread which is processing request and tell it to cancel processing the request
        // - if it's already complete then return teh completion status
 
        response.set_status (proto::ReplicationStatus::BAD);
    }
}

void
WorkerProcessor::checkStatus (const proto::ReplicationRequestStatus &request,
                              proto::ReplicationResponseStatus      &response) {

    THREAD_SAFE_BLOCK {

        std::cout << "WorkerProcessor::checkStatus "
                  << " id=" << request.id() <<  std::endl;

        // TODO: search for a request with this identifier in all queues.
        // Then return its status.

        response.set_status (proto::ReplicationStatus::SUCCESS);
    }
}


WorkerReplicationRequest::pointer
WorkerProcessor::fetchNextForProcessing (const WorkerProcessorThread::pointer &processorThread,
                                         std::chrono::milliseconds             timeoutMilliseconds) {

    THREAD_SAFE_BLOCK {
    
        std::cout << "WorkerProcessor::fetchNextForProcessing ** NOT IMPLEMENTED **" <<  std::endl;
        
        return WorkerReplicationRequest::pointer();
    }
}

void
WorkerProcessor::processingRefused (const WorkerReplicationRequest::pointer &request) {
    
    THREAD_SAFE_BLOCK {
    
        std::cout << "WorkerProcessor::processingRefused ** NOT IMPLEMENTED **" <<  std::endl;
    }
}

void
WorkerProcessor::processingCancelled (const WorkerReplicationRequest::pointer &request) {
    
    THREAD_SAFE_BLOCK {
    
        std::cout << "WorkerProcessor::processingCancelled ** NOT IMPLEMENTED **" <<  std::endl;
    }
}

void
WorkerProcessor::processingFinished (const WorkerReplicationRequest::pointer    &request,
                                     WorkerReplicationRequest::CompletionStatus  completionStatus) {
    
    THREAD_SAFE_BLOCK {
    
        std::cout << "WorkerProcessor::processingFinished ** NOT IMPLEMENTED **" <<  std::endl;
    }
}

void
WorkerProcessor::processorThreadStopped (const WorkerProcessorThread::pointer &processorThread) {
    
    THREAD_SAFE_BLOCK {

        std::cout << "WorkerProcessor::processorThreadStopped ** NOT IMPLEMENTED **" <<  std::endl;

        if (_state == State::STATE_IS_STOPPING) {

            // Complete state transition if all threds are stopped

            for (auto &t: _threads) {
                if (t->isRunning()) return;
            }
            _state = State::STATE_IS_STOPPED;
        }
    }
}

    
}}} // namespace lsst::qserv::replica_core
