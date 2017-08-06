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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERSERVERCONNECTION_H
#define LSST_QSERV_REPLICA_CORE_WORKERSERVERCONNECTION_H

/// WorkerServerConnection.h declares:
///
/// class WorkerServerConnection
/// (see individual class documentation for more information)

// System headers

#include <memory>       // shared_ptr, enable_shared_from_this

#include <boost/asio.hpp>

// Qserv headers

#include "proto/replication.pb.h"

#include "replica_core/ProtocolBuffer.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerProcessor.h"

namespace proto = lsst::qserv::proto;

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class WorkerServerConnection is used for handling connections from
  * remote clients. One instance of the class serves one client at a time.
  *
  * Objects of this class are inistantiated by WorkerServer. After that
  * the server calls this class's method beginProtocol() which startes
  * a series of asynchronous operations to communicate with remote client.
  * When all details of an incoming request are obtained from the client
  * the connection object forwards this request for actual processing
  * to an instace of the WorkerProcessor class. A response reseived from
  * the processor is serialized and sent back (asynchroniously) to
  * the client.
  */
class WorkerServerConnection
    :   public std::enable_shared_from_this<WorkerServerConnection> {

public:

    /// Shared pointer type for the class
    typedef std::shared_ptr<WorkerServerConnection> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (const ServiceProvider::pointer &serviceProvider,
                           const WorkerProcessor::pointer &processor,
                           boost::asio::io_service        &io_service);

    // Default construction and copy semantics are proxibited

    WorkerServerConnection () = delete;
    WorkerServerConnection (WorkerServerConnection const&) = delete;
    WorkerServerConnection & operator= (WorkerServerConnection const&) = delete;

    /// Destructor
    virtual ~WorkerServerConnection ();

    /**
     * Return a network socket associated with the connection.
     */
    boost::asio::ip::tcp::socket& socket () {
        return _socket;
    }

    /**
     * Begin communicating asynchroniously with a client. This is essentially
     * an RPC protocol which runs in a loop this sequence of steps:
     * 
     *   - ASYNC: read a frame header of a request
     *   -  SYNC: read the request header (request type, etc.)
     *   -  SYNC: read the request body (depends on a type of the request) 
     *   - ASYNC: write a frame header of a reply to the request
     *            then write the reply itself
     *
     * NOTES: A reason why the read phase is split into three steps is
     *        that a client is expected to send all components of the request
     *        (frame header, re uest header and reuest body) at once. This means
     *        the whole incomming message will be already available on the server's
     *        host memory when an asyncronous handler for the freame header will fire.
     *        However, due to a variable length of the request we should know its length
     *        before attempting
     *        to read the rest of the incomming message as this (the later) will require
     *        two things: 1) to ensure enough we have enough buffer space
     *        allocated, and 2) to tell the asynchrnous reader function
     *        how many bytes exactly are we going to read.
     * 
     * The chain ends when a client disconnects or when an error condition
     * is met.
     */
    void beginProtocol ();

private:

    /**
     * The constructor of the class.
     */
    explicit WorkerServerConnection (const ServiceProvider::pointer &serviceProvider,
                                     const WorkerProcessor::pointer &processor,
                                     boost::asio::io_service        &io_service);

    /**
     * Begin reading (asynchronosly) the frame header of a new request
     *
     * The frame header is presently a 32-bit unsigned integer
     * representing the length of the subsequent message.
     */
    void receive ();

    /**
     * The calback on finishing (either successfully or not) of aynchronious reads.
     */
    void received (const boost::system::error_code &ec,
                   size_t bytes_transferred);

    /**
     * Serrialize a protobuf object and send it back to a client.
     */
    template <class T>
    void reply (T &&response) {
        _bufferPtr->resize();
        _bufferPtr->serialize(response);
        send();
    }

    /**
     * Begin sending (asynchronosly) a result back to a client
     */
    void send ();

    /**
     * The calback on finishing (either successfully or not) of aynchronious writes.
     */
    void sent (const boost::system::error_code &ec,
               size_t                           bytes_transferred);

private:

    // Parameters of the object

    ServiceProvider::pointer _serviceProvider;
    WorkerProcessor::pointer _processor;

    boost::asio::ip::tcp::socket _socket;

    /// Buffer management class facilitating serialization/deserialization
    /// of data sent over the network
    std::shared_ptr<ProtocolBuffer> _bufferPtr;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERSERVERCONNECTION_H