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

#include "replica_core/WorkerServerConnection.h"

// System headers

#include <boost/bind.hpp>
#include <iostream>

// Qserv headers

#include "replica_core/ProtocolBuffer.h"

namespace proto = lsst::qserv::proto;

namespace {
    
typedef std::shared_ptr<lsst::qserv::replica_core::ProtocolBuffer> ProtocolBufferPtr;

bool isErrorCode (boost::system::error_code ec,
                  const std::string         &context) {

    if (ec) {
        if (ec == boost::asio::error::eof)
            std::cout << context << ": connection closed by peer" << std::endl;
        else
            std::cout << context << ": failed with error code " << ec << std::endl;
        return true;
    }
    return false;
}

bool readIntoBuffer (boost::asio::ip::tcp::socket &socket,
                     const ProtocolBufferPtr      &ptr,
                     size_t                        bytes) {

    ptr->resize(bytes);     // make sure the buffer has enough space to accomodate
                            // the data of the message.

    boost::system::error_code ec;
    boost::asio::read (
        socket,
        boost::asio::buffer (
            ptr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        ec
    );
    return ! ::isErrorCode (ec, "WorkerServerConnection::readIntoBuffer");
}

template <class T>
bool readMessage (boost::asio::ip::tcp::socket  &socket,
                  const ProtocolBufferPtr       &ptr,
                  size_t                         bytes,
                  T                             &message) {
    
    if (!readIntoBuffer (socket,
                         ptr,
                         bytes)) return false;

    // Parse the response to see what should be done next.

    ptr->parse(message, bytes);
    return true;
}

bool readLength (boost::asio::ip::tcp::socket  &socket,
                 const ProtocolBufferPtr       &ptr,
                 uint32_t                      &bytes) {

    if (!readIntoBuffer (socket,
                         ptr,
                         sizeof(uint32_t))) return false;
    
    bytes = ptr->parseLength();
    return true;
}

// Eliminate nested namespaces as if we were using this class within
// the namespace
typedef lsst::qserv::replica_core::WorkerProcessor WorkerProcessor;

/// Fill in processor's state and counters into a response object
// which will be sent back to a remote client.
void setServiceResponse (proto::ReplicationServiceResponse         &response,
                         proto::ReplicationServiceResponse::Status  status,
                         const WorkerProcessor::pointer            &processor) {

    response.set_status(status);

    switch (processor->state()) {

        case WorkerProcessor::State::STATE_IS_RUNNING:
            response.set_service_state (proto::ReplicationServiceResponse::RUNNING);
            break;

        case WorkerProcessor::State::STATE_IS_STOPPING:
            response.set_service_state (proto::ReplicationServiceResponse::SUSPEND_IN_PROGRESS);
            break;

        case WorkerProcessor::State::STATE_IS_STOPPED:
            response.set_service_state (proto::ReplicationServiceResponse::SUSPENDED);
            break;
    }
    response.set_num_new_requests        (processor->numNewRequests());
    response.set_num_in_progress_requests(processor->numInProgressRequests());
    response.set_num_finished_requests   (processor->numFinishedRequests());
}
                
}   // namespace

namespace lsst {
namespace qserv {
namespace replica_core {

WorkerServerConnection::pointer
WorkerServerConnection::create (const ServiceProvider::pointer &serviceProvider,
                                const WorkerProcessor::pointer &processor,
                                boost::asio::io_service        &io_service) {

    return WorkerServerConnection::pointer (
        new WorkerServerConnection (
            serviceProvider,
            processor,
            io_service));
}

WorkerServerConnection::WorkerServerConnection (const ServiceProvider::pointer &serviceProvider,
                                                const WorkerProcessor::pointer &processor,
                                                boost::asio::io_service        &io_service)

    :   _serviceProvider (serviceProvider),
        _processor       (processor),
        _socket          (io_service),

        _bufferPtr (
            std::make_shared<ProtocolBuffer>(
                serviceProvider->config()->requestBufferSizeBytes()))
{}

WorkerServerConnection::~WorkerServerConnection () {
}

void
WorkerServerConnection::beginProtocol () {
    receive();
}

void
WorkerServerConnection::receive () {

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whol emessage (its frame and
    // the message itsef) at once.

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind (
            &WorkerServerConnection::received,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
WorkerServerConnection::received (const boost::system::error_code &ec,
                                  size_t                           bytes_transferred) {

    if ( ::isErrorCode (ec, "WorkerServerConnection::received")) return;

    // Now read the request header

    proto::ReplicationRequestHeader hdr;
    if (!::readMessage (_socket, _bufferPtr, _bufferPtr->parseLength(), hdr)) return;
   
    // Now read a specific request
    //
    // ATTENTION: watch for the protocol! Some requests are fully expressed
    //            in terms of the above received & parsed header.

    switch (hdr.type()) {

        case proto::ReplicationRequestHeader::REPLICATE : {

            // Read the request length
            uint32_t bytes;
            if (!::readLength (_socket, _bufferPtr, bytes)) return;

            // Read the request body
            proto::ReplicationRequestReplicate request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseReplicate response;
            _processor->enqueueForReplication (request, response);
            reply(response);

            break;
        }
        case proto::ReplicationRequestHeader::STOP : {

            // Read the request length
            uint32_t bytes;
            if (!::readLength (_socket, _bufferPtr, bytes)) return;

            // Read the request body
            proto::ReplicationRequestStop request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseStop response;
            _processor->dequeueOrCancel (request, response);
            reply(response);

            break;
        }
        case proto::ReplicationRequestHeader::STATUS : {

            // Read the request length
            uint32_t bytes;
            if (!::readLength (_socket, _bufferPtr, bytes)) return;

            // Read the request body
            proto::ReplicationRequestStatus request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseStatus response;
            _processor->checkStatus (request, response);
            reply(response);

            break;
        }
        case proto::ReplicationRequestHeader::SERVICE_SUSPEND : {

            // This operation is allowed to be asynchronious as it may take
            // extra time for the processor's threads to finish on-going processing

            _processor->stop ();

            proto::ReplicationServiceResponse response;
            ::setServiceResponse(response,
                                 _processor->state() == WorkerProcessor::State::STATE_IS_RUNNING ?
                                    proto::ReplicationServiceResponse::FAILED :
                                    proto::ReplicationServiceResponse::SUCCESS,
                                 _processor);
            reply(response);

            break;
        }
        case proto::ReplicationRequestHeader::SERVICE_RESUME : {

        
            // This is a synchronus operation. The state transition request should happen
            // (or be denied) instantaneously.
        
            _processor->run ();

            proto::ReplicationServiceResponse response;
            ::setServiceResponse(response,
                                 _processor->state() == WorkerProcessor::State::STATE_IS_RUNNING ?
                                    proto::ReplicationServiceResponse::SUCCESS :
                                    proto::ReplicationServiceResponse::FAILED,
                                 _processor);
            reply(response);

            break;
        }
        case proto::ReplicationRequestHeader::SERVICE_STATUS : {

            proto::ReplicationServiceResponse response;
            ::setServiceResponse (response,
                                  proto::ReplicationServiceResponse::SUCCESS,
                                  _processor);
            reply(response);

            break;
        }
    }
}

void
WorkerServerConnection::send () {

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &WorkerServerConnection::sent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
WorkerServerConnection::sent (const boost::system::error_code &ec,
                              size_t                           bytes_transferred) {

    if ( ::isErrorCode (ec, "WorkerServerConnection::sent")) return;

    // Go wait for another request

    receive();
}

}}} // namespace lsst::qserv::replica_core
