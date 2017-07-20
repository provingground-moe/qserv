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

#include "replica_core/WorkerConnection.h"

// System headers

#include <boost/bind.hpp>
#include <iostream>

// Qserv headers

#include "replica_core/ProtocolBuffer.h"

namespace proto = lsst::qserv::proto;

namespace {
    
    typedef std::shared_ptr<lsst::qserv::replica_core::ProtocolBuffer> ProtocolBufferPtr;

bool
isErrorCode (boost::system::error_code ec,
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

bool
readIntoBuffer (boost::asio::ip::tcp::socket &socket,
                ProtocolBufferPtr ptr,
                size_t bytes) {

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
    return ! ::isErrorCode (ec, "WorkerConnection::readIntoBuffer");
}

template <class T>
bool
readMessage (boost::asio::ip::tcp::socket  &socket,
             ProtocolBufferPtr ptr,
             size_t bytes,
             T &message) {
    
    if (!readIntoBuffer (socket,
                         ptr,
                         bytes)) return false;

    // Parse the response to see what should be done next.

    ptr->parse(message, bytes);
    return true;
}

bool
readLength (boost::asio::ip::tcp::socket  &socket,
            ProtocolBufferPtr ptr,
            uint32_t &bytes) {

    if (!readIntoBuffer (socket,
                         ptr,
                         sizeof(uint32_t))) return false;
    
    bytes = ptr->parseLength();
    return true;
}

                       
}   // namespace

namespace lsst {
namespace qserv {
namespace replica_core {

WorkerConnection::pointer
WorkerConnection::create (const ServiceProvider::pointer &serviceProvider,
                          const WorkerProcessor::pointer &requestProcessor,
                          boost::asio::io_service        &io_service) {

    return WorkerConnection::pointer (
        new WorkerConnection (
            serviceProvider,
            requestProcessor,
            io_service));
}

WorkerConnection::WorkerConnection (const ServiceProvider::pointer &serviceProvider,
                                    const WorkerProcessor::pointer &requestProcessor,
                                    boost::asio::io_service        &io_service)

    :   _serviceProvider  (serviceProvider),
        _requestProcessor (requestProcessor),
        _socket           (io_service),

        _bufferPtr (
            std::make_shared<ProtocolBuffer>(
                serviceProvider->config()->requestBufferSizeBytes()))
{}

WorkerConnection::~WorkerConnection () {
}

void
WorkerConnection::beginProtocol () {
    receive();
}

void
WorkerConnection::receive () {

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
            &WorkerConnection::received,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
WorkerConnection::received (const boost::system::error_code& ec,
                            size_t bytes_transferred) {

    if ( ::isErrorCode (ec, "WorkerConnection::received")) return;

    // Now read the request header

    proto::ReplicationRequestHeader hdr;
    if (!::readMessage (_socket, _bufferPtr, _bufferPtr->parseLength(), hdr)) return;
   
    // Read the request length

    uint32_t bytes;
    if (!::readLength (_socket, _bufferPtr, bytes)) return;

    // Now read a specific request

    switch (hdr.type()) {

        case proto::ReplicationRequestHeader::REPLICATE : {

            proto::ReplicationRequestReplicate request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseReplicate response;
            _requestProcessor->enqueueForReplication (request, response);
            reply(response);

            break;
        }

        case proto::ReplicationRequestHeader::STOP : {

            proto::ReplicationRequestStop request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseStop response;
            _requestProcessor->dequeueOrCancel (request, response);
            reply(response);

            break;
        }

        case proto::ReplicationRequestHeader::STATUS : {

            proto::ReplicationRequestStatus request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseStatus response;
            _requestProcessor->checkStatus (request, response);
            reply(response);

            break;
        }
    }
}

void
WorkerConnection::send () {

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &WorkerConnection::sent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
WorkerConnection::sent (const boost::system::error_code &ec,
                        size_t bytes_transferred) {

    if ( ::isErrorCode (ec, "WorkerConnection::sent")) return;

    // Go wait for another request

    receive();
}

}}} // namespace lsst::qserv::replica_core
