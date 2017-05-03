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
#include "replica_core/StatusRequest.h"

// System headers

#include <arpa/inet.h>  // htonl, ntohl

#include <chrono>
#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers

#include "replica_core/ProtocolBuffer.h"

namespace proto = lsst::qserv::proto;

namespace lsst {
namespace qserv {
namespace replica_core {

StatusRequest::pointer
StatusRequest::create (ServiceProvider::pointer serviceProvider,
                       const std::string        &worker,
                       boost::asio::io_service  &io_service,
                       const std::string        &replicationRequestId,
                       callback_type            onFinish) {

    return pointer (
        new StatusRequest (
            serviceProvider,
            generateId(),
            worker,
            io_service,
            replicationRequestId,
            onFinish));
}

StatusRequest::StatusRequest (ServiceProvider::pointer serviceProvider,
                              const std::string        &id,
                              const std::string        &worker,
                              boost::asio::io_service  &io_service,
                              const std::string        &replicationRequestId,
                              callback_type            onFinish)
    :   Request(serviceProvider,
                "STATUS",
                id,
                worker,
                io_service),

        _replicationRequestId (replicationRequestId),
        _onFinish             (onFinish)
{}

StatusRequest::~StatusRequest ()
{}

std::shared_ptr<Request>
StatusRequest::final_shared_from_this () {
    return shared_from_this () ;
}

void
StatusRequest::beginProtocol () {

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_type(proto::ReplicationRequestHeader::STOP);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id(_replicationRequestId);

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &StatusRequest::requestSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
StatusRequest::requestSent (const boost::system::error_code &ec,
                            size_t bytes_transferred) {
    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveResponse();
}

void
StatusRequest::receiveResponse () {

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
            &StatusRequest::responseReceived,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
StatusRequest::responseReceived (const boost::system::error_code &ec,
                                 size_t bytes_transferred) {
    if (isAborted(ec)) return;

    if (ec) {
        restart();
        return;
    }

    // Get the length of the message and try reading the message itself
    // from the socket.
        
    const uint32_t bytes = _bufferPtr->parseLength();

    _bufferPtr->resize(bytes);      // make sure the buffer has enough space to accomodate
                                    // the data of the message.

    boost::system::error_code error_code;
    boost::asio::read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        error_code
    );
    if (error_code) restart();
    else {
    
        // Parse the response to see what should be done next.
    
        proto::ReplicationResponseStatus message;
        _bufferPtr->parse(message, bytes);
    
        analyze(message.status());
    }
}

void
StatusRequest::analyze (proto::ReplicationStatus status) {

    switch (status) {
 
        case proto::ReplicationStatus::SUCCESS:
            finish (FINISHED, SUCCESS);
            break;

        default:
            finish (FINISHED, SERVER_ERROR);
            break;
    }
}

void
StatusRequest::endProtocol () {

    if (_onFinish != nullptr) {
        _onFinish(shared_from_this());
    }
}

}}} // namespace lsst::qserv::replica_core
