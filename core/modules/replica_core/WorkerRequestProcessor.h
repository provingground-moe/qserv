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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERREQUESTPROCESSOR_H
#define LSST_QSERV_REPLICA_CORE_WORKERREQUESTPROCESSOR_H

/// WorkerRequestProcessor.h declares:
///
/// class WorkerRequestProcessor
/// (see individual class documentation for more information)

// System headers

#include <memory>       // shared_ptr, enable_shared_from_this

// Qserv headers

#include "proto/replication.pb.h"
#include "replica_core/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

namespace proto = lsst::qserv::proto;

/**
  * Class WorkerRequestProcessor is a front-end interface for processing
  * requests fro connected clients.
  */
class WorkerRequestProcessor
    : public std::enable_shared_from_this<WorkerRequestProcessor> {

public:

    typedef std::shared_ptr<WorkerRequestProcessor> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (ServiceProvider::pointer serviceProvider);

    // Default construction and copy semantics are proxibited

    WorkerRequestProcessor () = delete;
    WorkerRequestProcessor (WorkerRequestProcessor const&) = delete;
    WorkerRequestProcessor & operator= (WorkerRequestProcessor const&) = delete;

    /// Destructor
    virtual ~WorkerRequestProcessor ();

    /**
     * Process the replication request.
     *
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void replicate (const proto::ReplicationRequestReplicate &request,
                    proto::ReplicationResponseReplicate &response);

    /**
     * Stop an on-going replication request
     *
     * @param request - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void stop (const proto::ReplicationRequestStop &request,
               proto::ReplicationResponseStop &response);

    /**
     * Return the status of an on-going replication request
     *
     * @param request - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void status (const proto::ReplicationRequestStatus &request,
                 proto::ReplicationResponseStatus &response);

private:

    /**
     * The constructor of the class.
     */
    explicit WorkerRequestProcessor (ServiceProvider::pointer serviceProvider);

private:

    // Parameters of the object

    ServiceProvider::pointer _serviceProvider;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERREQUESTPROCESSOR_H