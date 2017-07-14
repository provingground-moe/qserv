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

#include <chrono>
#include <queue>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <mutex>
#include <thread>
#include <vector>

// Qserv headers

#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerProcessorThread.h"
#include "replica_core/WorkerReplicationRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class WorkerProcessor is a front-end interface for processing
  * requests fro connected clients.
  */
class WorkerProcessor
    : public std::enable_shared_from_this<WorkerProcessor> {

public:

    typedef std::shared_ptr<WorkerProcessor> pointer;    

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (ServiceProvider::pointer serviceProvider);

    // Default construction and copy semantics are proxibited

    WorkerProcessor () = delete;
    WorkerProcessor (WorkerProcessor const&) = delete;
    WorkerProcessor & operator= (WorkerProcessor const&) = delete;

    /// Destructor
    virtual ~WorkerProcessor ();

    /// Begin processing requests
    void run();

    /**
     * Process the replication request.
     *
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void replicate (const proto::ReplicationRequestReplicate &request,
                    proto::ReplicationResponseReplicate      &response);

    /**
     * Stop an on-going replication request
     *
     * @param request - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void stop (const proto::ReplicationRequestStop &request,
               proto::ReplicationResponseStop      &response);

    /**
     * Return the status of an on-going replication request
     *
     * @param request - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void status (const proto::ReplicationRequestStatus &request,
                 proto::ReplicationResponseStatus      &response);
    
    /**
     * Return the next replication request.
     * 
     * If the one is available witin the specified timeout then such request
     * will be moved into the in-progress queue, assigned to the processor thread
     * and returned to a caller. Otherwise an empty pointer (pointing to nullptr)
     * will be returned.
     *
     * ATTENTION: this method will block for a duration of time not exceeding
     * the client-specified timeout.
     */
    WorkerReplicationRequest::pointer next (
            WorkerProcessorThread::pointer processorThread,
            std::chrono::milliseconds      timeoutMilliseconds);

    /**
     * Report a request which has been processed. The method accepts requests which
     * were previously obtained with the above defined metho mext().
     *
     * The completion status of the operation will be stored witin the request.
     */
    void finish (WorkerReplicationRequest::pointer);

private:

    /**
     * The constructor of the class.
     */
    explicit WorkerProcessor (ServiceProvider::pointer serviceProvider);

private:

    /// Services used by the processor
    ServiceProvider::pointer _serviceProvider;

    /// A pool of threads for processing requests
    std::vector<WorkerProcessorThread::pointer> _threads;
    
    /// Mutex guarding the queues
    std::mutex _mtx;

    /// The queues of new unprocessed requests
    std::priority_queue<WorkerReplicationRequest::pointer> _newRequests;

    /// The queues of requests which are being processed
    std::priority_queue<WorkerReplicationRequest::pointer> _inProgressRequests;

    /// The queues of requests which have cove been completed (succeeded or otherwise)
    std::priority_queue<WorkerReplicationRequest::pointer> _finishedRequests;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERPROCESSOR_H