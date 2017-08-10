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
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerDeleteRequest.h"
#include "replica_core/WorkerFindRequest.h"
#include "replica_core/WorkerFindAllRequest.h"
#include "replica_core/WorkerReplicationRequest.h"
#include "replica_core/WorkerRequestFactory.h"

// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

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
    throw std::logic_error("unhandled state " + std::to_string(state) +
                           " in WorkerProcessor::state2string()");
}

proto::ReplicationStatus
WorkerProcessor::translateReplicationStatus (WorkerRequest::CompletionStatus status) {
    switch (status) {
        case WorkerRequest::STATUS_NONE:          return proto::ReplicationStatus::QUEUED;
        case WorkerRequest::STATUS_IN_PROGRESS:   return proto::ReplicationStatus::IN_PROGRESS;
        case WorkerRequest::STATUS_IS_CANCELLING: return proto::ReplicationStatus::IS_CANCELLING;
        case WorkerRequest::STATUS_CANCELLED:     return proto::ReplicationStatus::CANCELLED;
        case WorkerRequest::STATUS_SUCCEEDED:     return proto::ReplicationStatus::SUCCESS;
        case WorkerRequest::STATUS_FAILED:        return proto::ReplicationStatus::FAILED;
        default:
            throw std::logic_error (
                "unhandled status " + WorkerRequest::status2string(status) +
                " at WorkerProcessor::translateReplicationStatus()");
    }
}

WorkerProcessor::WorkerProcessor (ServiceProvider      &serviceProvider,
                                  WorkerRequestFactory &requestFactory)

    :   _serviceProvider (serviceProvider),
        _requestFactory  (requestFactory),

        _state (STATE_IS_STOPPED) {
}

WorkerProcessor::~WorkerProcessor () {
}

void
WorkerProcessor::run () {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "run");

    if (_state == STATE_IS_STOPPED) {

        const size_t numThreads = _serviceProvider.config().workerNumProcessingThreads();
        if (!numThreads) throw std::out_of_range("the number of procesisng threads can't be 0");
    
        // Create threads if needed
    
        if (_threads.empty()) {
            for (size_t i=0; i < numThreads; ++i) {
                _threads.push_back(WorkerProcessorThread::create(*this));
            }
        }
    
        // Tell each thread to run
    
        for (auto &t: _threads) {
            t->run();
        }
        _state = STATE_IS_RUNNING;
    }
}

void
WorkerProcessor::stop () {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "stop");

    if (_state == STATE_IS_RUNNING) {
        
        // Tell each thread to stop.
    
        for (auto &t: _threads) {
            t->stop();
        }
        
        // Begin transitioning to the final state via this intermediate one.
        // The transition will finish asynchronious when all threads will report
        // desired changes in their states.

        _state = STATE_IS_STOPPING;
    }
}

void
WorkerProcessor::enqueueForReplication (const proto::ReplicationRequestReplicate &request,
                                        proto::ReplicationResponseReplicate      &response) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForReplication"
        << "  id: "     << request.id()
        << "  db: "     << request.database()
        << "  chunk: "  << request.chunk()
        << "  worker: " << request.worker());    

    // TODO: run the sanity check to ensure no such request is found in any
    //       of the queue. Return 'DUPLICATE' error status if teh one is found.

    WorkerRequest::pointer workerRequest =
        _requestFactory.createReplicationRequest (
            request.id(),
            request.priority(),
            request.database(),
            request.chunk(),
            request.worker());

    _newRequests.push(workerRequest);
    
    response.set_status (proto::ReplicationStatus::QUEUED);
}

void
WorkerProcessor::enqueueForDeletion (const proto::ReplicationRequestDelete &request,
                                     proto::ReplicationResponseDelete      &response) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForDeletion"
        << "  id: "    << request.id()
        << "  db: "    << request.database()
        << "  chunk: " << request.chunk());
    
    // TODO: run the sanity check to ensure no such request is found in any
    //       of the queue. Return 'DUPLICATE' error status if teh one is found.

    WorkerRequest::pointer workerRequest =
        _requestFactory.createDeleteRequest (
            request.id(),
            request.priority(),
            request.database(),
            request.chunk());

    _newRequests.push(workerRequest);

    response.set_status (proto::ReplicationStatus::QUEUED);
}

void
WorkerProcessor::enqueueForFind (const proto::ReplicationRequestFind &request,
                                 proto::ReplicationResponseFind      &response) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForFind"
        << "  id: "    << request.id()
        << "  db: "    << request.database()
        << "  chunk: " << request.chunk());

    // TODO: run the sanity check to ensure no such request is found in any
    //       of the queue. Return 'DUPLICATE' error status if teh one is found.

    WorkerRequest::pointer workerRequest =
        _requestFactory.createFindRequest (
            request.id(),
            request.priority(),
            request.database(),
            request.chunk());

    _newRequests.push(workerRequest);

    response.set_status (proto::ReplicationStatus::QUEUED);
}


void
WorkerProcessor::enqueueForFindAll (const proto::ReplicationRequestFindAll &request,
                                    proto::ReplicationResponseFindAll      &response) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "enqueueForFindAll"
        << "  id: " << request.id()
        << "  db: " << request.database());

    // TODO: run the sanity check to ensure no such request is found in any
    //       of the queue. Return 'DUPLICATE' error status if teh one is found.

    WorkerRequest::pointer workerRequest =
        _requestFactory.createFindAllRequest (
            request.id(),
            request.priority(),
            request.database());

    _newRequests.push(workerRequest);

    response.set_status (proto::ReplicationStatus::QUEUED);
}


WorkerRequest::pointer
WorkerProcessor::dequeueOrCancelImpl (const std::string &id) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "dequeueOrCancelImpl" << "  id: " << id);
     
    // Still waiting in the queue?

    for (auto ptr : _newRequests)
        if (ptr->id() == id) {
           
            // Cancel it and move it into the final queue in case if a client
            // won't be able to receive the desired status of the request due to
            // a protocol failure, etc.

            ptr->cancel();

            switch (ptr->status()) {

                case WorkerRequest::STATUS_CANCELLED:

                    _newRequests.remove(id);
                    _finishedRequests.push_back(ptr);

                    return ptr;

                default:
                    throw std::logic_error (
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::dequeueOrCancelImpl among new requests");
            }
        }

    // Is it already being processed?

    for (auto ptr : _inProgressRequests)
        if (ptr->id() == id) {

            // Tell the request to begin the cancelling protocol. The protocol
            // will take care of moving the request into the final queue when
            // the cancellation will finish.
            //
            // At the ment time we just notify the client about the cancelattion status
            // of the request and let it come back later to check the updated status.

            ptr->cancel();

            switch (ptr->status()) {

                case WorkerRequest::STATUS_IS_CANCELLING:
                    return ptr;

                default:
                    throw std::logic_error (
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::dequeueOrCancelImpl among in-progress requests");
            }
        }

    // Has it finished?

    for (auto ptr : _finishedRequests)
        if (ptr->id() == id) {

            // There is nothing else we can do here other than just
            // reporting the completion status of the request. It's up to a client
            // to figure out what to do about this situation.

            switch (ptr->status()) {

                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;

                default:
                    throw std::logic_error (
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::dequeueOrCancelImpl.");
            }
        }

    // Sorry, no such request found!

    return WorkerRequest::pointer();
}

WorkerRequest::pointer
WorkerProcessor::checkStatusImpl (const std::string &id) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "checkStatusImpl" << "  id: " << id);

    // Still waiting in the queue?

    for (auto ptr : _newRequests)
        if (ptr->id() == id)
            switch (ptr->status()) {
                case WorkerRequest::STATUS_NONE:
                    return ptr;
                default:
                    throw std::logic_error (
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::checkStatusImpl among new requests");
            }


    // Is it already being processed?

    for (auto ptr : _inProgressRequests)
        if (ptr->id() == id)
            switch (ptr->status()) {
                case WorkerRequest::STATUS_IS_CANCELLING:
                case WorkerRequest::STATUS_IN_PROGRESS:
                    return ptr;
                default:
                    throw std::logic_error (
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::checkStatusImpl among in-progress requests");
            }


    // Has it finished?

    for (auto ptr : _finishedRequests)
        if (ptr->id() == id)
            switch (ptr->status()) {
                case WorkerRequest::STATUS_CANCELLED:
                case WorkerRequest::STATUS_SUCCEEDED:
                case WorkerRequest::STATUS_FAILED:
                    return ptr;
                default:
                    throw std::logic_error (
                        "unexpected request status " + WorkerRequest::status2string(ptr->status()) +
                        " at WorkerProcessor::checkStatusImpl among finished requests");
            }

    // Sorry, no such request found!

    return WorkerRequest::pointer();
}


WorkerRequest::pointer
WorkerProcessor::fetchNextForProcessing (const WorkerProcessorThread::pointer &processorThread,
                                         unsigned int                          timeoutMilliseconds) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "fetchNextForProcessing"
        << "  thread: " << processorThread->id()
        << "  timeout: " << timeoutMilliseconds);

    // For generating random intervals within the maximum range of seconds
    // requested by a client.

    BlockPost blockPost(0, timeoutMilliseconds);

    unsigned int totalElapsedTime = 0;
    while (totalElapsedTime < timeoutMilliseconds) {

        // IMPORTANT: make sure no wait is happening within the same
        // scope where the thread safe block is defined. Otherwise
        // the queue will be locked for all threads for the duration of
        // the wait.

        {
            LOCK_GUARD;

            if (!_newRequests.empty()) {

                WorkerRequest::pointer request = _newRequests.top();
                _newRequests.pop();

                request->setStatus(WorkerRequest::STATUS_IN_PROGRESS);
                _inProgressRequests.push_back(request);

                return request;
            }
        }
        totalElapsedTime += blockPost.wait();
    }
    
    // Return null pointer since noting has been found within the specified
    // timeout.

    return WorkerRequest::pointer();
}

void
WorkerProcessor::processingRefused (const WorkerRequest::pointer &request) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "processingRefused" << "  id: " << request->id());

    // Update request's state before moving it back into
    // the input queue.

    request->setStatus(WorkerRequest::STATUS_NONE);
    _inProgressRequests.remove_if (
        [&request] (const WorkerRequest::pointer &ptr) {
            return ptr->id() == request->id();
        }
    );
    _newRequests.push(request);
}

void
WorkerProcessor::processingFinished (const WorkerRequest::pointer &request) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "processingFinished"
        << "  id: " << request->id()
        << "  status: " << WorkerRequest::status2string(request->status()));

    // Then move it forward into the finished queue.

    _inProgressRequests.remove_if (
        [&request] (const WorkerRequest::pointer &ptr) {
            return ptr->id() == request->id();
        }
    );
    _finishedRequests.push_back(request);
}

void
WorkerProcessor::processorThreadStopped (const WorkerProcessorThread::pointer &processorThread) {

    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, context() << "processorThreadStopped" << "  thread: " << processorThread->id());

    if (_state == STATE_IS_STOPPING) {

        // Complete state transition if all threds are stopped

        for (auto &t: _threads) {
            if (t->isRunning()) return;
        }
        _state = STATE_IS_STOPPED;
    }
}

    
}}} // namespace lsst::qserv::replica_core
