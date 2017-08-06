// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERPROCESSOR_H
#define LSST_QSERV_REPLICA_CORE_WORKERPROCESSOR_H

/// WorkerProcessor.h declares:
///
/// class WorkerProcessor
/// (see individual class documentation for more information)

// System headers

#include <algorithm>
#include <list>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

// Qserv headers

#include "proto/replication.pb.h"

#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerProcessorThread.h"
#include "replica_core/WorkerRequest.h"

namespace proto = lsst::qserv::proto;

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

class WorkerRequestFactory;

/**
  * Class WorkerProcessor is a front-end interface for processing
  * requests fro connected clients.
  */
class WorkerProcessor
    :   public std::enable_shared_from_this<WorkerProcessor> {

public:

    // The thread-based processor class is allowed to access the internal API
    friend class WorkerProcessorThread;

    /// The pointer type for self
    typedef std::shared_ptr<WorkerProcessor> pointer;

    /// The priority queue for pointers to the new (unprocessed) requests.
    /// Using inheritance to get access to the protected data members 'c'
    /// representing the internal container.
    struct PriorityQueueType
        :   std::priority_queue<WorkerRequest::pointer,
                                std::vector<WorkerRequest::pointer>,
                                WorkerRequestCompare> {

        /// The beginning of the container to allow the iterator protocol
        decltype(c.begin()) begin () {
            return c.begin();
        }

        /// The end of the container to allow the iterator protocol
        decltype(c.end()) end () {
            return c.end();
        }

        /// Remove a request from the queue by its identifier
        bool remove (const std::string &id) {
            auto itr = std::find_if (
                c.begin(),
                c.end(),
                [&id] (const WorkerRequest::pointer &ptr) {
                    return ptr->id() == id;
                }
            );
            if (itr != c.end()) {
                c.erase(itr);
                std::make_heap(c.begin(), c.end(), comp);
                return true;
            }
            return false;
        }
    };

    /// Ordinary collection of pointers for requests in other (than new/unprocessed) state
    typedef std::list<WorkerRequest::pointer> CollectionType;

    /// Current state of the request processing engine
    enum State {
        STATE_IS_RUNNING,    // all threads are running
        STATE_IS_STOPPING,   // stopping all threads
        STATE_IS_STOPPED     // not started
    };

    /// Return the string representation of the status
    static std::string state2string (State state);

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (const ServiceProvider::pointer &serviceProvider,
                           WorkerRequestFactory           &requestFactory);

    // Default construction and copy semantics are proxibited

    WorkerProcessor () = delete;
    WorkerProcessor (WorkerProcessor const&) = delete;
    WorkerProcessor & operator= (WorkerProcessor const&) = delete;

    /// Destructor
    virtual ~WorkerProcessor ();

    /// @return the state of the processor
    State state () const { return _state; }

    /// Begin processing requests
    void run ();

    /// Stop processing all requests, and stop all threads
    void stop ();

    /**
     * Enqueue the replication request for processing
     *
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForReplication (const proto::ReplicationRequestReplicate &request,
                                proto::ReplicationResponseReplicate      &response);

    /**
     * Enqueue the replica deletion request for processing
     *
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForDeletion (const proto::ReplicationRequestDelete &request,
                             proto::ReplicationResponseDelete      &response);

    /**
     * Enqueue the replica lookup request for processing
     *
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForFind (const proto::ReplicationRequestFind &request,
                         proto::ReplicationResponseFind      &response);

    /**
     * Enqueue the multi-replica lookup request for processing
     *
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    void enqueueForFindAll (const proto::ReplicationRequestFindAll &request,
                            proto::ReplicationResponseFindAll      &response);

    /**
     * Dequeue replication request
     *
     * If the request is not being processed yet then it wil be simply removed
     * from the ready-to-be-processed queue. If it's being processed an attempt
     * to cancel processing will be made. If it has already processed this will
     * be reported.
     *
     * @param request  - the protobuf object representing the request
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    template <typename RESPONSE_MSG_TYPE>
    void dequeueOrCancel (const proto::ReplicationRequestStop &request
                          RESPONSE_MSG_TYPE                   &response) {

        WorkerRequest::pointer ptr = dequeueOrCancelImpl(request->id());
        if (!ptr) {
            response.set_status(proto::ReplicationStatus::BAD);
            return;
        }

        // Set the status field which is present in all response types
        response.set_status(translateReplicationStatus(ptr->status()));

        // Set request-specific fields (if any)
        
        switch (request.type()) {

            case proto::ReplicationReplicaRequestType::REPLICA_CREATE:
            case proto::ReplicationReplicaRequestType::REPLICA_DELETE:
                break;

            case proto::ReplicationReplicaRequestType::REPLICA_FIND: {
                break;
            }
            case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL : {
                break;
            }
        }
    }

    /**
     * Return the status of an on-going replication request
     *
     * @param request  - the protobuf object representing the request
     * @param response - the protobuf object to be initialized and ready
     *                   to be sent back to the client
     */
    template <typename RESPONSE_MSG_TYPE>
    void checkStatus (const proto::ReplicationRequestStatus &request
                      RESPONSE_MSG_TYPE                     &response) {

        WorkerRequest::pointer ptr = checkStatusImpl(request->id());
        if (!ptr) {
            response.set_status(proto::ReplicationStatus::BAD);
            return;
        }

        // Set the status field which is present in all response types
        response.set_status(translateReplicationStatus(ptr->status()));

        // Set request-specific fields (if any)
        
        switch (request.type()) {

            case proto::ReplicationReplicaRequestType::REPLICA_CREATE:
            case proto::ReplicationReplicaRequestType::REPLICA_DELETE:
                break;

            case proto::ReplicationReplicaRequestType::REPLICA_FIND: {
                break;
            }
            case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL : {
                break;
            }
        }
    }


    /// Number of new unprocessed requests
    size_t numNewRequests () const { return _newRequests.size(); }

    /// Number of requests which are being processed
    size_t numInProgressRequests () const { return _inProgressRequests.size(); }

    /// Number of completed (succeeded or otherwise) requests
    size_t numFinishedRequests () const { return _finishedRequests.size(); }

private:

    /**
     * Translate the completion status for replication requests and return
     * its protobuf counterpart
     *
     * @param status - a completion status of a request processing object
     * 
     * @return the matching completion status as per a protobuf definition
     */
    static proto::ReplicationStatus translateReplicationStatus (
            WorkerRequest::CompletionStatus status);

    /**
     * The constructor of the class.
     */
    WorkerProcessor (const ServiceProvider::pointer &serviceProvider,
                     WorkerRequestFactory           &requestFactory);

    /**
     * Return the next request which is ready to be pocessed
     * and if then one found assign it to the specified thread. The request
     * will be removed from the ready-to-be-processed queue.
     * 
     * If the one is available witin the specified timeout then such request
     * will be moved into the in-progress queue, assigned to the processor thread
     * and returned to a caller. Otherwise an empty pointer (pointing to nullptr)
     * will be returned.
     *
     * This method is supposed to be called by one of the processing threads
     * when it becomes available.
     * 
     * ATTENTION: this method will block for a duration of time not exceeding
     *            the client-specified timeout unless it's set to 0. In the later
     *            case the method will block indefinitevely.
     */
    WorkerRequest::pointer fetchNextForProcessing (
            const WorkerProcessorThread::pointer &processorThread,
            unsigned int                          timeoutMilliseconds=0);

    /**
     * Implement the operation for the specified identifier if such request
     * is still known to the Processor. Return a reference to the request object
     * whose state will be properly updated.
     *
     * @return - a valid reference to the request object (if found)
     *           or a reference to nullptr otherwise. 
     */
    WorkerRequest::pointer dequeueOrCancelImpl (const std::string &id);

    /** Find and return a reference to the request object.
     * 
     * @return - a valid reference to the request object (if found)
     *           or a reference to nullptr otherwise. 
     */
    WorkerRequest::pointer checkStatusImpl (const std::string &id);

    /**
     * Report a decision not to process a request
     *
     * This method ia supposed to be called by one of the processing threads
     * after it fetches the next ready-to-process request and then decided
     * not to proceed with processing. Normally this should happen when
     * the thread was asked to stop. In that case the request will be put
     * back into the ready-to-be processed request and be picked up later
     * by some other thread.
     */
    void processingRefused (const WorkerRequest::pointer &request);

    /**
     * Report a request which has been processed or cancelled.
     *
     * The method is called by a thread which was processing the request.
     * The request will be moved into the corresponding queue. A proper
     * completion status is expected be stored witin the request.
     */
    void processingFinished (const WorkerRequest::pointer &request);

    /**
     * For threads reporting their completion
     *
     * This method is used by threads to report a change in their state.
     * It's meant to be used during the gradual and asynchronous state transition
     * of this processor from the combined State::STATE_IS_STOPPING to
     * State::STATE_IS_STOPPED. The later is achieved when all threads are stopped.
     */
    void processorThreadStopped (const WorkerProcessorThread::pointer &processorThread);

    /// Return the context string
    std::string context () const { return "PROCESSOR  "; }

private:

    /// Services used by the processor
    ServiceProvider::pointer _serviceProvider;


    WorkerRequestFactory &_requestFactory;

    /// Current state of the processor
    State _state;

    /// A pool of threads for processing requests
    std::vector<WorkerProcessorThread::pointer> _threads;
    
    /// Mutex guarding the queues
    std::mutex _mtx;

    /// New unprocessed requests
    PriorityQueueType _newRequests;

    /// Requests which are being processed
    CollectionType _inProgressRequests;

    /// Completed (succeeded or otherwise) requests
    CollectionType _finishedRequests;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERPROCESSOR_H