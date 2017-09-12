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
#include "replica_core/MessengerConnector.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/Configuration.h"
#include "replica_core/ProtocolBuffer.h"
#include "replica_core/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.MessengerConnector");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

std::string
MessengerConnector::status2string (MessengerConnector::CompletionStatus status) {
    switch (status) {
        case SUCCEEDED: return "SUCCEEDED";
        case FAILED:    return "FAILED";
        case CANCELED:  return "CANCELED";
    }
    throw std::logic_error ("incomplete implementation of method MessengerConnector::status2string()");
}


    enum State {
        STATE_INITIAL,      // no communication is happening
        STATE_CONNECTING,   // attempting to connecto to a worker service
        STATE_COMMUNICATING // sending and receiven messages
    };

std::string
MessengerConnector::state2string (MessengerConnector::State state) {
    switch (state) {
        case STATE_INITIAL:       return "STATE_INITIAL";
        case STATE_CONNECTING:    return "STATE_CONNECTING";
        case STATE_COMMUNICATING: return "STATE_COMMUNICATING";
    }
    throw std::logic_error ("incomplete implementation of method MessengerConnector::state2string()");
}



MessengerConnector::pointer
MessengerConnector::create (ServiceProvider&         serviceProvider,
                            boost::asio::io_service& io_service,
                            std::string const&       worker) {

    return MessengerConnector::pointer (
        new MessengerConnector (serviceProvider,
                                io_service,
                                worker));
}

MessengerConnector::MessengerConnector (ServiceProvider&         serviceProvider,
                                        boost::asio::io_service& io_service,
                                        std::string const&       worker)
    :   _serviceProvider (serviceProvider),
        _workerInfo      (serviceProvider.config().workerInfo(worker)),

        _bufferCapacityBytes (serviceProvider.config().requestBufferSizeBytes()),
 
        _state    (State::STATE_INITIAL),
        _resolver (io_service),
        _socket   (io_service),
        
        _outBuffer (serviceProvider.config().requestBufferSizeBytes()),
        _inBuffer  (serviceProvider.config().requestBufferSizeBytes()) {
}

MessengerConnector::~MessengerConnector () {
}

void
MessengerConnector::cancel (std::string const& id) {

    LOCK_GUARD;
    
    if (!_id2request.count(id))
        throw std::logic_error ("MessengerConnector::cancel()  unknow request id: " + id);

}

void
MessengerConnector::sendImpl (std::string const&                             id,
                              MessengerConnector::WrapperBase_pointer const& ptr) {
    LOCK_GUARD;

    if (_id2request.count(id))
        throw std::logic_error (
                "MessengerConnector::sendImpl()  the request is alrady registered for id:" + id);

    // Register the request

    _id2request[id] = ptr;
    _requests.push_back(ptr);

    // Then check the status of communication
    
    switch (_state) {

        case STATE_INITIAL:
            _state = STATE_CONNECTING;
            //resolve();
            return;

        case STATE_CONNECTING:
            return;
        
        default:
            break;
    }
    //beginSending (id);
}
    
}}} // namespace lsst::qserv::replica_core
