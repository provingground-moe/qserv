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
#ifndef LSST_QSERV_REPLICA_CORE_REPLICATIONREQUEST_H
#define LSST_QSERV_REPLICA_CORE_REPLICATIONREQUEST_H

/// ReplicationRequest.h declares:
///
/// class ReplicationRequest
/// (see individual class documentation for more information)

// System headers

#include <functional>   // std::function
#include <memory>       // shared_ptr
#include <string>

// Qserv headers

#include "proto/replication.pb.h"
#include "replica_core/Request.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class ReplicationRequest is a base class for a family of requests.
  * It represents a transient state of a request witi the master service.
  */
class ReplicationRequest
    :   public Request  {

public:

    /// The only class which is allowed to instantiate and manage replications
    friend class MasterServer;

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ReplicationRequest> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are proxibited

    ReplicationRequest () = delete;
    ReplicationRequest (ReplicationRequest const&) = delete;
    ReplicationRequest & operator= (ReplicationRequest const&) = delete;

    /// Destructor
    ~ReplicationRequest () final;

    
    const std::string& database     () const { return _database; }
    unsigned int       chunk        () const { return _chunk; }
    const std::string& sourceWorker () const { return  _sourceWorker; }

private:

    /**
     * Create a new request with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider   - a host of services for various communications
     * @param database          - the name of a database
     * @param chunk             - the number of a chunk to replicate (implies all relevant tables)
     * @param sourceWorker      - the identifier of a worker node at a source of the chunk
     * @param destinationWorker - the identifier of a worker node (the one to be affectd by the replication)
     *                            at a destination of the chunk
     * @param onFinish          - an optional callback function to be called upon a completion of
     *                            the request.
     */
    static pointer create (const ServiceProvider::pointer &serviceProvider,
                           const std::string              &database,
                           unsigned int                    chunk,
                           const std::string              &sourceWorker,
                           const std::string              &destinationWorker,
                           boost::asio::io_service        &io_service,
                           callback_type                   onFinish);

    /**
     * Construct the request with the pointer to the services provider.
     */
    ReplicationRequest (const ServiceProvider::pointer &serviceProvider,
                        const std::string              &database,
                        unsigned int                    chunk,
                        const std::string              &sourceWorker,
                        const std::string              &destinationWorker,
                        boost::asio::io_service        &io_service,
                        callback_type                   onFinish);

    /**
      * This method is called when a connection is established and
      * the stack is ready to begin implementing an actual protocol
      * with the worker server.
      *
      * The first step of teh protocol will be to send the replication
      * request to the destination worker.
      */
    void beginProtocol () final;
    
    /// Callback handler for the asynchronious operation
    void requestSent (const boost::system::error_code &ec,
                      size_t bytes_transferred);

    /// Start receiving the response from the destination worker
    void receiveResponse ();

    /// Callback handler for the asynchronious operation
    void responseReceived (const boost::system::error_code &ec,
                           size_t bytes_transferred);

    /// Start the timer before attempting the previously failed
    /// or successfull (if a status check is needed) step.
    void wait ();

    /// Callback handler for the asynchronious operation
    void awaken (const boost::system::error_code &ec);

    /// Start sending the status request to the destination worker
    void sendStatus ();

    /// Callback handler for the asynchronious operation
    void statusSent (const boost::system::error_code &ec,
                     size_t bytes_transferred);

    /// Start receiving the status response from the destination worker
    void receiveStatus ();

    /// Callback handler for the asynchronious operation
    void statusReceived (const boost::system::error_code &ec,
                         size_t bytes_transferred);

    /// Process the completion of the requested operation
    void analyze (lsst::qserv::proto::ReplicationStatus status);

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * bu the base class.
     */
    void endProtocol () final;

private:

    // Parameters of the object

    std::string  _database;
    unsigned int _chunk;
    std::string  _sourceWorker;
    
    // To be initialized from a configuration via the ServiceProvider
    // then cached for the lifespan of the object.

    std::shared_ptr<WorkerInfo> _sourceWorkerInfoPtr;
    
    // Registered callback to be called when the operation finishes

    callback_type _onFinish;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_REPLICATIONREQUEST_H