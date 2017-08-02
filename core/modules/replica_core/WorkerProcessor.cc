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
#include <iterator>
#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/BlockPost.h"

// This macro to appear witin each block which requires thread safety

#define THREAD_SAFE_BLOCK \
std::lock_guard<std::mutex> lock(_mtx);

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerProcessor");

} /// namespace

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "run");

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "stop");
    
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

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForReplication"
        << "  id: "    << request.id()
        << "  db: "    << request.database()
        << "  chunk: " << request.chunk());

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

    LOGS(_log, LOG_LVL_DEBUG, context() << "dequeueOrCancel"
        << "  id: " << request.id());

    THREAD_SAFE_BLOCK {

        const std::string id = request.id(); 
     
         // Still waiting in the queue?
 
        for (auto ptr : _newRequests) {
            if (ptr->id() == id) {
                
                // Cancel it and move it into the final queue in case if a client
                // won't be able to receive the desired status of the request due to
                // a protocol failure, etc.

                ptr->cancel();

                switch (ptr->status()) {

                    case WorkerReplicationRequest::CompletionStatus::CANCELLED:

                        _newRequests.remove(id);
                        _finishedRequests.push_back(ptr);

                        response.set_status(proto::ReplicationStatus::CANCELLED);
                        return;

                    default:
                        throw std::logic_error (
                            "unexpected request status " + WorkerReplicationRequest::status2string(ptr->status()) +
                            " at WorkerProcessor::dequeueOrCancel among new requests");
                }
            }
        }
 
        // Is it already being processed?

        for (auto ptr : _inProgressRequests) {
            if (ptr->id() == id) {

                // Tell the request to begin the cancelling protocol. The protocol
                // will take care of moving the request into the final queue when
                // the cancellation will finish.
                //
                // At the ment time we just notify the client about the cancelattion status
                // of the request and let it come back later to check the updated status.

                ptr->cancel();

                switch (ptr->status()) {

                    case WorkerReplicationRequest::CompletionStatus::IS_CANCELLING:
                        response.set_status(proto::ReplicationStatus::IS_CANCELLING);
                        return;

                    default:
                        throw std::logic_error (
                            "unexpected request status " + WorkerReplicationRequest::status2string(ptr->status()) +
                            " at WorkerProcessor::dequeueOrCancel among in-progress requests");
                }
            }
        }

        // Has it finished?

        for (auto ptr : _finishedRequests) {
            if (ptr->id() == id) {

                // There is nothing else we can do here other than just
                // reporting the completion status of the request. It's up to a client
                // to figure out what to do about this situation.

                switch (ptr->status()) {

                    case WorkerReplicationRequest::CompletionStatus::CANCELLED:
                        response.set_status(proto::ReplicationStatus::CANCELLED);
                        return;

                    case WorkerReplicationRequest::CompletionStatus::SUCCEEDED:
                        response.set_status(proto::ReplicationStatus::SUCCESS);
                        return;

                    case WorkerReplicationRequest::CompletionStatus::FAILED:
                        response.set_status(proto::ReplicationStatus::FAILED);
                        return;

                    default:
                        throw std::logic_error (
                            "unexpected request status " + WorkerReplicationRequest::status2string(ptr->status()) +
                            " at WorkerProcessor::checkStatus.");
                }
            }
        }

        // Sorry, no such request found!

        response.set_status (proto::ReplicationStatus::BAD);
    }
}

void
WorkerProcessor::checkStatus (const proto::ReplicationRequestStatus &request,
                              proto::ReplicationResponseStatus      &response) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "checkStatus"
        << "  id: " << request.id());

    THREAD_SAFE_BLOCK {

        const std::string id = request.id(); 

        // Still waiting in the queue?
 
        for (auto ptr : _newRequests) {
            if (ptr->id() == id) {
                switch (ptr->status()) {
                    case WorkerReplicationRequest::CompletionStatus::NONE:
                        response.set_status(proto::ReplicationStatus::QUEUED);
                        return;
                    default:
                        break;
                }
                throw std::logic_error (
                    "unexpected request status " + WorkerReplicationRequest::status2string(ptr->status()) +
                    " at WorkerProcessor::checkStatus among new requests");
            }
        }

        // Is it already being processed?

        for (auto ptr : _inProgressRequests) {
            if (ptr->id() == id) {
                switch (ptr->status()) {
                    case WorkerReplicationRequest::CompletionStatus::IS_CANCELLING:
                        response.set_status(proto::ReplicationStatus::IS_CANCELLING);
                        return;
                    case WorkerReplicationRequest::CompletionStatus::IN_PROGRESS:
                        response.set_status(proto::ReplicationStatus::IN_PROGRESS);
                        return;
                    default:
                        break;
                }
                throw std::logic_error (
                    "unexpected request status " + WorkerReplicationRequest::status2string(ptr->status()) +
                    " at WorkerProcessor::checkStatus among in-progress requests");
            }
        }

        // Has it finished?

        for (auto ptr : _finishedRequests) {
            if (ptr->id() == id) {
                switch (ptr->status()) {
                    case WorkerReplicationRequest::CompletionStatus::CANCELLED:
                        response.set_status(proto::ReplicationStatus::CANCELLED);
                        return;
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
                    " at WorkerProcessor::checkStatus among finished requests");
            }
        }

        // Sorry, no such request found!

        response.set_status (proto::ReplicationStatus::BAD);
    }
}


WorkerReplicationRequest::pointer
WorkerProcessor::fetchNextForProcessing (const WorkerProcessorThread::pointer &processorThread,
                                         unsigned int                          timeoutMilliseconds) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "fetchNextForProcessing"
        << "  thread: " << processorThread->id()
        << "  timeout: " << timeoutMilliseconds);

    // For generating random intervals witghin the maximum range of seconds
    // requested by a client.

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

                    request->beginProgress();
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

    LOGS(_log, LOG_LVL_DEBUG, context() << "processingRefused"
        << "  id: " << request->id());
    
    THREAD_SAFE_BLOCK {
        
        // Move it back into the input queue.

        _inProgressRequests.remove_if (
            [&request] (const WorkerReplicationRequest::pointer &ptr) {
                return ptr->id() == request->id();
            }
        );
        _newRequests.push(request);
    }
}

void
WorkerProcessor::processingFinished (const WorkerReplicationRequest::pointer &request) {
    
    LOGS(_log, LOG_LVL_DEBUG, context() << "processingFinished"
        << "  id: " << request->id()
        << "  status: " << WorkerReplicationRequest::status2string(request->status()));

    THREAD_SAFE_BLOCK {

        // Then move it forward into the finished queue.

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
    
    LOGS(_log, LOG_LVL_DEBUG, context() << "processorThreadStopped"
        << "  thread: " << processorThread->id());

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
