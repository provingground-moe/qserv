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
#include "replica_core/ReplicationRequest.h"

// System headers

#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/ProtocolBuffer.h"
#include "replica_core/ServiceProvider.h"

namespace proto = lsst::qserv::proto;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.ReplicationRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

ReplicationRequest::pointer
ReplicationRequest::create (ServiceProvider         &serviceProvider,
                            boost::asio::io_service &io_service,
                            const std::string       &worker,
                            const std::string       &sourceWorker,
                            const std::string       &database,
                            unsigned int             chunk,
                            callback_type            onFinish,
                            int                      priority,
                            bool                     keepTracking) {

    return ReplicationRequest::pointer (
        new ReplicationRequest (
            serviceProvider,
            io_service,
            worker,
            sourceWorker,
            database,
            chunk,
            onFinish,
            priority,
            keepTracking));
}

ReplicationRequest::ReplicationRequest (ServiceProvider         &serviceProvider,
                                        boost::asio::io_service &io_service,
                                        const std::string       &worker,
                                        const std::string       &sourceWorker,
                                        const std::string       &database,
                                        unsigned int             chunk,
                                        callback_type            onFinish,
                                        int                      priority,
                                        bool                     keepTracking)
    :   RequestConnection (serviceProvider,
                           io_service,
                           "REPLICA_CREATE",
                           worker,
                           priority,
                           keepTracking),
 
        _database     (database),
        _chunk        (chunk),
        _sourceWorker (sourceWorker),
        _onFinish     (onFinish),
        _responseData () {

    _serviceProvider.assertWorkerIsValid       (sourceWorker);
    _serviceProvider.assertWorkersAreDifferent (sourceWorker, worker);
    _serviceProvider.assertDatabaseIsValid     (database);
}

ReplicationRequest::~ReplicationRequest () {
}

void
ReplicationRequest::beginProtocol () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "beginProtocol");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id          (id());
    hdr.set_type        (proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestReplicate message;
    message.set_priority(priority    ());
    message.set_database(database    ());
    message.set_chunk   (chunk       ());
    message.set_worker  (sourceWorker());

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &ReplicationRequest::requestSent,
            shared_from_base<ReplicationRequest>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
ReplicationRequest::requestSent (const boost::system::error_code &ec,
                                 size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent");

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveResponse();
}

void
ReplicationRequest::receiveResponse () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "receiveResponse");

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
            &ReplicationRequest::responseReceived,
            shared_from_base<ReplicationRequest>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
ReplicationRequest::responseReceived (const boost::system::error_code &ec,
                                      size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "responseReceived");

    if (isAborted(ec)) return;

    if (ec) {
        restart();
        return;
    }

    // All operations hereafter are synchronious because the worker
    // is supposed to send a complete multi-message response w/o
    // making any explicit handshake with the Controller.

    if (syncReadVerifyHeader (_bufferPtr->parseLength())) restart();
    
    size_t bytes;
    if (syncReadFrame (bytes)) restart ();
           
    proto::ReplicationResponseReplicate message;
    if (syncReadMessage (bytes, message)) restart();
    else                                  analyze(message);
}

void
ReplicationRequest::wait () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &ReplicationRequest::awaken,
            shared_from_base<ReplicationRequest>(),
            boost::asio::placeholders::error
        )
    );
}

void
ReplicationRequest::awaken (const boost::system::error_code &ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) return;

    // Also ignore this event if the request expired
    if (_state== State::FINISHED) return;

    sendStatus();
}

void
ReplicationRequest::sendStatus () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "sendStatus");

    // Serialize the Status message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_id             (id());
    hdr.set_type           (proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id  (id());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_CREATE);

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &ReplicationRequest::statusSent,
            shared_from_base<ReplicationRequest>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
ReplicationRequest::statusSent (const boost::system::error_code &ec,
                                size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusSent");

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveStatus();
}

void
ReplicationRequest::receiveStatus () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "receiveStatus");

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
            &ReplicationRequest::statusReceived,
            shared_from_base<ReplicationRequest>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
ReplicationRequest::statusReceived (const boost::system::error_code &ec,
                                    size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusReceived");

    if (isAborted(ec)) return;

    if (ec) {
        restart();
        return;
    }

    // All operations hereafter are synchronious because the worker
    // is supposed to send a complete multi-message response w/o
    // making any explicit handshake with the Controller.

    if (syncReadVerifyHeader (_bufferPtr->parseLength())) restart();
    
    size_t bytes;
    if (syncReadFrame (bytes)) restart ();
           
    proto::ReplicationResponseReplicate message;
    if (syncReadMessage (bytes, message)) restart();
    else                                  analyze(message);
}

void
ReplicationRequest::analyze (const proto::ReplicationResponseReplicate &message) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze  remote status: " << proto::ReplicationStatus_Name(message.status()));

    // Always get the latest status reported by the remote server
    _extendedServerStatus = replica_core::translate(message.status_ext());

    // Performance counters are updated from either of two sources,
    // depending on the availability of the 'target' performance counters
    // filled in by the 'STATUS' queries. If the later is not available
    // then fallback to the one of the current request.

    if (message.has_target_performance())
        _performance.update(message.target_performance());
    else
        _performance.update(message.performance());

    // Always extract extended data regardless of the completion status
    // reported by the worker service.

    _responseData = ReplicaCreateInfo(&(message.replication_info()));

    switch (message.status()) {
 
        case proto::ReplicationStatus::SUCCESS:
            finish (SUCCESS);
            break;

        case proto::ReplicationStatus::QUEUED:
        case proto::ReplicationStatus::IN_PROGRESS:
        case proto::ReplicationStatus::IS_CANCELLING:

            // Go wait until a definitive response from the worker is received.

            wait();
            return;

        case proto::ReplicationStatus::BAD:
            finish (SERVER_BAD);
            break;

        case proto::ReplicationStatus::FAILED:
            finish (SERVER_ERROR);
            break;

        case proto::ReplicationStatus::CANCELLED:
            finish (SERVER_CANCELLED);
            break;

        default:
            throw std::logic_error("ReplicationRequest::analyze() unknown status '" + proto::ReplicationStatus_Name(message.status()) +
                                   "' received from server");

    }
}

void
ReplicationRequest::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish != nullptr) {
        _onFinish(shared_from_base<ReplicationRequest>());
    }
}

}}} // namespace lsst::qserv::replica_core
