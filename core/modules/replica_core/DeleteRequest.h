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
#ifndef LSST_QSERV_REPLICA_CORE_DELETEREQUEST_H
#define LSST_QSERV_REPLICA_CORE_DELETEREQUEST_H

/// DeleteRequest.h declares:
///
/// class DeleteRequest
/// (see individual class documentation for more information)

// System headers

#include <functional>   // std::function
#include <memory>       // shared_ptr
#include <string>

// Qserv headers

#include "proto/replication.pb.h"
#include "replica_core/ReplicaDeleteInfo.h"
#include "replica_core/Request.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class DeleteRequest represents a transient state of the replica deletion
  * requests within the master controller for deleting replicas.
  */
class DeleteRequest
    :   public Request  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DeleteRequest> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are proxibited

    DeleteRequest () = delete;
    DeleteRequest (DeleteRequest const&) = delete;
    DeleteRequest & operator= (DeleteRequest const&) = delete;

    /// Destructor
    ~DeleteRequest () final;

    // Trivial acccessors
 
    const std::string& database () const { return _database; }
    unsigned int       chunk    () const { return _chunk; }

    /// Return request-specific extended data reported upon completion of the request
    const ReplicaDeleteInfo& responseData () const {
        return _responseData;
    }

    /**
     * Create a new request with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one where the chunk is supposed
     *                           to be located) at a destination of the chunk
     * @param database         - the name of a database
     * @param chunk            - the number of a chunk to replicate (implies all relevant tables)
     * @param onFinish         - an optional callback function to be called upon a completion of
     *                           the request.
     * @param priority         - a priority level of the request
     */
    static pointer create (ServiceProvider         &serviceProvider,
                           boost::asio::io_service &io_service,
                           const std::string       &worker,
                           const std::string       &database,
                           unsigned int             chunk,
                           callback_type            onFinish,
                           int                      priority=0);

private:

    /**
     * Construct the request with the pointer to the services provider.
     */
    DeleteRequest (ServiceProvider         &serviceProvider,
                   boost::asio::io_service &io_service,
                   const std::string       &worker,
                   const std::string       &database,
                   unsigned int             chunk,
                   callback_type            onFinish,
                   int                      priority=0);

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
                      size_t                           bytes_transferred);

    /// Start receiving the response from the destination worker
    void receiveResponse ();

    /// Callback handler for the asynchronious operation
    void responseReceived (const boost::system::error_code &ec,
                           size_t                           bytes_transferred);

    /// Start the timer before attempting the previously failed
    /// or successfull (if a status check is needed) step.
    void wait ();

    /// Callback handler for the asynchronious operation
    void awaken (const boost::system::error_code &ec);

    /// Start sending the status request to the destination worker
    void sendStatus ();

    /// Callback handler for the asynchronious operation
    void statusSent (const boost::system::error_code &ec,
                     size_t                           bytes_transferred);

    /// Start receiving the status response from the destination worker
    void receiveStatus ();

    /// Callback handler for the asynchronious operation
    void statusReceived (const boost::system::error_code &ec,
                         size_t                           bytes_transferred);

    /// Process the completion of the requested operation
    void analyze (const proto::ReplicationResponseDelete &message);

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
    
    // Registered callback to be called when the operation finishes

    callback_type _onFinish;

    /// Extended informationon on a status of the operation
    ReplicaDeleteInfo _responseData;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_DELETEREQUEST_H