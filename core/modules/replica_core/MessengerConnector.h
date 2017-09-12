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
#ifndef LSST_QSERV_REPLICA_CORE_MESSENGER_CONNECTOR_H
#define LSST_QSERV_REPLICA_CORE_MESSENGER_CONNECTOR_H

/// MessengerConnector.h declares:
///
/// class MessengerConnector
/// (see individual class documentation for more information)

// System headers

#include <functional>   // std::function
#include <list>
#include <map>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <mutex>
#include <string>

#include <boost/asio.hpp>

// Qserv headers

#include "proto/replication.pb.h"
#include "replica_core/ProtocolBuffer.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

class ServiceProvider;
class WorkerInfo;

/**
 * This class provides a communication interface for sending/receiving messages
 * to and from worker services. It provides connection multiplexing and automatic
 * reconnects.
 */
class MessengerConnector
    :   public std::enable_shared_from_this<MessengerConnector> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<MessengerConnector> pointer;

    /// The type for returning results from 
    enum CompletionStatus {
        SUCCEEDED,  // in case of success (in that case the RESPONSE_TYPE should be valid)
        FAILED,     // there was a comunication or protocol failure
        CANCELED    // if a prior reqest was explicitly canceled by calling 'cancel' (see below)
    };
    
    /// Return the string representation of the status
    static std::string status2string (CompletionStatus status);

    /// The base class for request wrappers
    class WrapperBase {

    public:

        // Default construction and copy semantics are prohibited
    
        WrapperBase () = delete;
        WrapperBase (WrapperBase const&) = delete;
        WrapperBase& operator= (WrapperBase const&) = delete;
    
        /// Destructor
        virtual ~WrapperBase () {
        }

        /**
         * Parse the content of the buffer and notify a subscriber
         *
         * @param bytes - the number of meaningful bytes in the buffer
         */
        virtual void parseAndNotify (size_t bytes)=0;

    protected:

        /**
         * The constructor
         *
         * @param responseBufferCapacityBytes - the initial size of the response buffer
         */
        WrapperBase (std::shared_ptr<replica_core::ProtocolBuffer> const& requestBufferPtr_,
                     size_t                                               responseBufferCapacityBytes)

            :   status           (CompletionStatus::SUCCEEDED),
                requestBufferPtr (requestBufferPtr_),
                responseBuffer   (responseBufferCapacityBytes) {
        }

    public:

        /// The completion status to be returned to a subscriber
        CompletionStatus status;

        /// The buffer with a serialized request
        std::shared_ptr<replica_core::ProtocolBuffer> requestBufferPtr;

        /// The buffer for receiving responses from a worker server
        replica_core::ProtocolBuffer responseBuffer;
    };

    template <class RESPONSE_TYPE>
    class Wrapper
        :   public WrapperBase {

        // Default construction and copy semantics are prohibited
    
        Wrapper () = delete;
        Wrapper (Wrapper const&) = delete;
        Wrapper& operator= (Wrapper const&) = delete;
    
        /// Destructor
        ~Wrapper () override {
        }

        /**
         * The constructor
         * 
         * @param requestBufferPtr            - a request serielized into a network buffer
         * @param responseBufferCapacityBytes - the initial size of the response buffer
         * @param onFinish                    - an asynchronious callback function called upon
         *                                      a completion or failure of the operation
         */
        Wrapper (std::shared_ptr<replica_core::ProtocolBuffer> const&         requestBufferPtr,
                 size_t                                                       responseBufferCapacityBytes,
                 std::function<void(CompletionStatus, RESPONSE_TYPE const&)>& onFinish)

            :   WrapperBase (requestBufferPtr,
                             responseBufferCapacityBytes),
                _onFinish (onFinish) {
        }

        /**
         * @see WrapperBase::parseResponseAndNotify
         */
        void parseAndNotify (size_t bytes) override {
            RESPONSE_TYPE response;
            if (status == CompletionStatus::SUCCEEDED) {
                responseBuffer.parse(response, bytes);
            }
            _onFinish(status, response);
        }

    private:

        /// The collback fnction to be called upon the completion of
        /// the transaction.
        std::function<void(CompletionStatus, RESPONSE_TYPE const&)> _onFinish;
    };
    
    /// The pointer type for the base class of the request wrappers
    typedef std::shared_ptr<WrapperBase> WrapperBase_pointer;

    // Default construction and copy semantics are prohibited

    MessengerConnector () = delete;
    MessengerConnector (MessengerConnector const&) = delete;
    MessengerConnector& operator= (MessengerConnector const&) = delete;

    /// Destructor
    virtual ~MessengerConnector ();

    /**
     * Create a new connector with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param io_service       - the I/O service for communication. The lifespan of
     *                           the object must exceed the one of this instanc.
     * @param worker           - the name of a worker
     */
    static pointer create (ServiceProvider&         serviceProvider,
                           boost::asio::io_service& io_service,
                           std::string const&       worker);

    /**
     * Initiate sending a message
     *
     * The response message will be initialized only in case of successfull completion
     * of the transaction. The method may throw exception std::logic_error if
     * the MessangerConnector already has another transaction registered with the same
     * transaction 'id'.
     * 
     * @param id                - a unique identifier of a request
     * @param requestBufferPtr  - a request serielized into a network buffer
     * @param onFinish          - an asynchronious callback function called upon a completion
     *                            or failure of the operation
     */
    template <class RESPONSE_TYPE>
    void send (std::string const&                                           id,
               std::shared_ptr<replica_core::ProtocolBuffer> const&         requestBufferPtr,
               std::function<void(CompletionStatus, RESPONSE_TYPE const&)>& onFinish) {

        sendImpl (
            id,
            std::make_shared<Wrapper<RESPONSE_TYPE>> (
                requestBufferPtr,
                _bufferCapacityBytes,
                onFinish
            )
        );
    }

    /**
     * Cancel an outstanding transaction
     *
     * If this call succeeds there won't be any 'onFinish' callback made
     * as provided to the 'onFinish' method in method 'send'.
     * 
     * The method may throw std::logic_error if the Messanger doesn't have
     * a transaction registered with the specified transaction 'id'.
     *
     * @param id  - a unique identifier of a request
     */
    void cancel (std::string const& id);

private:

    /**
     * The constructor
     */
    MessengerConnector (ServiceProvider&         serviceProvider,
                        boost::asio::io_service& io_service,
                        std::string const&       worker);

    /**
     * The actual implementation of the operation 'send'.
     *
     * The method may throw the same exceptions as method 'sent'
     *
     * @param id  - a unique identifier of a request
     * @param ptr - a pointer to the request wrapper object
     */
    void sendImpl (std::string const&         id,
                   WrapperBase_pointer const& ptr);
    
    /// State transitions for the connector object
    enum State {
        STATE_INITIAL,      // no communication is happening
        STATE_CONNECTING,   // attempting to connecto to a worker service
        STATE_COMMUNICATING // sending and receiven messages
    };

    /// Return the string representation of the connector's state
    static std::string state2string (State state);

private:

    // Parameters of the object

    ServiceProvider& _serviceProvider;

    WorkerInfo const& _workerInfo;

    /// The cached parameter for the buffer sizes (pulled from
    /// the Configuration upon the construction of the object).
    size_t _bufferCapacityBytes;

    /// The internal state
    State _state;

    // Mutable state for network communication

    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::socket   _socket;

    /// This mutex is meant to avoid race conditions to the internal data
    /// structure between a thread which runs the Ntework I/O service
    /// and threads submitting requests.
    std::mutex _mtx;

    /// The queue of requests
    std::list<WrapperBase_pointer> _requests;

    /// The currently processed (being sent) request (if any, otherwise
    /// the pointer is set to nullptr)
    WrapperBase_pointer _currentRequest;

    /// Requests ordered by their unique identifiers
    std::map<std::string, WrapperBase_pointer> _id2request;

    /// The buffer for messages sent to a worker
    replica_core::ProtocolBuffer _outBuffer;

    /// The intermediate buffer for messages received from a worker
    replica_core::ProtocolBuffer _inBuffer;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_MESSENGER_CONNECTOR_H