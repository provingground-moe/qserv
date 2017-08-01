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

#include <algorithm>
#include <iostream>
#include <iterator>
#include <stdexcept>

// Qserv headers

#include "replica_core/BlockPost.h"

// This macro to appear witin each block which requires thread safety

#define THREAD_SAFE_BLOCK \
std::lock_guard<std::mutex> lock(_mtx);

namespace lsst {
namespace qserv {
namespace replica_core {


std::string
WorkerProcessor::state2string (State state) {
    switch (state) {
        case STATE_IS_RUNNING:  return "STATE_IS_RUNNING";
        case STATE_IS_STOPPING: return "STATE_IS_STOPPING";
        case STATE_IS_STOPPED:  return "STATE_IS_STOPPED";
    }
    throw std::logic_error("unhandled state " + std::to_string(state) + " in WorkerProcessor::state2string()");
}

WorkerProcessor::pointer
WorkerProcessor::create (const ServiceProvider::pointer &serviceProvider) {
    return pointer (
        new WorkerProcessor (serviceProvider));
}

WorkerProcessor::WorkerProcessor (const ServiceProvider::pointer &serviceProvider)
    :   _serviceProvider (serviceProvider),
        _state           (State::STATE_IS_STOPPED) {
}

WorkerProcessor::~WorkerProcessor () {
}


void
WorkerProcessor::run () {

    std::cout << context() << "run" << std::endl;
    
    THREAD_SAFE_BLOCK {

        if (_state == State::STATE_IS_STOPPED) {

            const size_t numThreads = _serviceProvider->config()->workerNumProcessingThreads();
            if (!numThreads) throw std::out_of_range("the number of procesisng threads can't be 0");
        
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

    std::cout << context() << "stop" << std::endl;
    
    THREAD_SAFE_BLOCK {

        if (_state == State::STATE_IS_RUNNING) {
            
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
                                        proto::ReplicationResponseReplicate      &response) {

    std::cout << context() << "enqueueForReplication"
        << "  id: "    << request.id()
        << "  db: "    << request.database()
        << "  chunk: " << request.chunk()
        << std::endl;

    THREAD_SAFE_BLOCK {
    

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

    std::cout << context() << "dequeueOrCancel"
        << "  id: " << request.id()
        << std::endl;

    THREAD_SAFE_BLOCK {

        const std::string id = request.id(); 
     
         // Still waiting in the queue?
 
        for (auto const &ptr : _newRequests) {
            if (ptr->id() == id) {
                _newRequests.remove(id);
                response.set_status(proto::ReplicationStatus::CANCELLED);
                return;
            }
        }

        // Is it already being processed?

        for (auto const &ptr : _inProgressRequests) {
            if (ptr->id() == id) {

                // Go find a processing thread which is working on the request
                // and tell it to cancel processing the request. A client is expected
                // to come back and check the thread status later.

                WorkerProcessorThread::pointer processorThread = ptr->processorThread();
                processorThread->cancel();

                response.set_status(proto::ReplicationStatus::IN_PROGRESS);
                return;
            }
        }

        // Has it finished?

        for (auto &ptr : _finishedRequests) {
            if (ptr->id() == id) {

                // There is nothing else we can do here other than just
                // reporting the completion status of the request. It's up to a client
                // to figure out what to do about this situation.

                switch (ptr->status()) {
                    case WorkerReplicationRequest::CompletionStatus::SUCCEEDED:
                        response.set_status(proto::ReplicationStatus::SUCCESS);
                        return;
                    case WorkerReplicationRequest::CompletionStatus::FAILED:
                        response.set_status(proto::ReplicationStatus::FAILED);
                        return;
                    default:
                        break;
                }
                throw std::logic_error (
                    "unexpected request status " + WorkerReplicationRequest::status2string(ptr->status()) +
                    " at WorkerProcessor::checkStatus.");
            }
        }

        // Sorry, no such request found!

        response.set_status (proto::ReplicationStatus::BAD);
    }
}

void
WorkerProcessor::checkStatus (const proto::ReplicationRequestStatus &request,
                              proto::ReplicationResponseStatus      &response) {

    std::cout << context() << "checkStatus"
        << "  id: " << request.id()
        << std::endl;

    THREAD_SAFE_BLOCK {

        const std::string id = request.id(); 

        // Still waiting in the queue?
 
        for (auto const &ptr : _newRequests) {
            if (ptr->id() == id) {
                response.set_status(proto::ReplicationStatus::QUEUED);
                return;
            }
        }

        // Is it already being processed?

        for (auto const &ptr : _inProgressRequests) {
            if (ptr->id() == id) {
                response.set_status(proto::ReplicationStatus::IN_PROGRESS);
                return;
            }
        }

        // Has it finished?

        for (auto &ptr : _finishedRequests) {
            if (ptr->id() == id) {
                switch (ptr->status()) {
                    case WorkerReplicationRequest::CompletionStatus::SUCCEEDED:
                        response.set_status(proto::ReplicationStatus::SUCCESS);
                        return;
                    case WorkerReplicationRequest::CompletionStatus::FAILED:
                        response.set_status(proto::ReplicationStatus::FAILED);
                        return;
                    default:
                        break;
                }
                throw std::logic_error (
                    "unexpected request status " + WorkerReplicationRequest::status2string(ptr->status()) +
                    " at WorkerProcessor::checkStatus.");
            }
        }

        // Sorry, no such request found!

        response.set_status (proto::ReplicationStatus::BAD);
    }
}


WorkerReplicationRequest::pointer
WorkerProcessor::fetchNextForProcessing (const WorkerProcessorThread::pointer &processorThread,
                                         unsigned int                          timeoutMilliseconds) {

    std::cout << context() << "fetchNextForProcessing"
        << "  thread: " << processorThread->id()
        << "  timeout: " << timeoutMilliseconds
        << std::endl;

    //
    BlockPost blockPost(0, timeoutMilliseconds);

    unsigned int totalElapsedTime = 0;
    while (totalElapsedTime < timeoutMilliseconds) {

        // IMPORTANT: make sure no wait is hppening witin the same
        // scope where the thread safe block is defined. Otherwise
        // the queue will be locked for all threads for the duration of
        // the wait.

        {
            THREAD_SAFE_BLOCK {

                if (!_newRequests.empty()) {

                    WorkerReplicationRequest::pointer request = _newRequests.top();
                    _newRequests.pop();

                    request->setProcessorThread(processorThread);
                    _inProgressRequests.push_back(request);

                    return request;
                }
            }
        }
        totalElapsedTime += blockPost.wait();
    }
    
    // Return null pointer since noting has been found within the specified
    // timeout.

    return WorkerReplicationRequest::pointer();
}

void
WorkerProcessor::processingRefused (const WorkerReplicationRequest::pointer &request) {

    std::cout << context() << "processingRefused"
        << "  id: " << request->id()
        << std::endl;
    
    THREAD_SAFE_BLOCK {
        
        // Disconnect the reuest from its processing thread.
        // Then move it back into the input queue.

        request->setProcessorThread();

        _inProgressRequests.remove_if (
            [&request] (const WorkerReplicationRequest::pointer &ptr) {
                return ptr->id() == request->id();
            }
        );
        _newRequests.push(request);
    }
}

void
WorkerProcessor::processingCancelled (const WorkerReplicationRequest::pointer &request) {
    
    std::cout << context() << "processingCancelled"
        << "  id: " << request->id()
        << std::endl;

    THREAD_SAFE_BLOCK {

        // Simply get rid of the request altogether

        // Disconnect the reuest from its processing thread.

        request->setProcessorThread();

        _inProgressRequests.remove_if (
            [&request] (const WorkerReplicationRequest::pointer &ptr) {
                return ptr->id() == request->id();
            }
        );
    }
}

void
WorkerProcessor::processingFinished (const WorkerReplicationRequest::pointer    &request,
                                     WorkerReplicationRequest::CompletionStatus  completionStatus) {
    
    std::cout << context() << "processingFinished"
        << "  id: " << request->id()
        << "  status: " << WorkerReplicationRequest::status2string(completionStatus)
        << std::endl;

    THREAD_SAFE_BLOCK {

        // Disconnect the reuest from its processing thread.
        // Set the desired status.
        // Then move it forward into the fiished queue.

        request->setProcessorThread();
        request->setStatus(completionStatus);

        _inProgressRequests.remove_if (
            [&request] (const WorkerReplicationRequest::pointer &ptr) {
                return ptr->id() == request->id();
            }
        );
        _finishedRequests.push_back(request);
    }
}

void
WorkerProcessor::processorThreadStopped (const WorkerProcessorThread::pointer &processorThread) {
    
    std::cout << context() << "processorThreadStopped"
        << "  thread: " << processorThread->id()
        << std::endl;

    THREAD_SAFE_BLOCK {

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
