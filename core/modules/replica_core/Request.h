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
#ifndef LSST_QSERV_REPLICA_CORE_REQUEST_H
#define LSST_QSERV_REPLICA_CORE_REQUEST_H

/// Request.h declares:
///
/// class Request
/// (see individual class documentation for more information)

// System headers

#include <chrono>
#include <memory>
#include <string>

#include <boost/asio.hpp>

// Qserv headers

#include "replica_core/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

class ProtocolBuffer;

/**
  * Class Request is a base class for a family of requests within
  * the master server.
  */
class Request {

public:

    /// Primary public state of the request
    enum State {

        /// The request has been constructed, and no attempt to execure it has
        /// been made.
        CREATED,
        
        /// The request is in a progress
        IN_PROGRESS,
        
        /// The request is finihed. See extended status for more details
        /// (the completion status, etc.)
        FINISHED
    };

    /// Return the string representation of the primary state
    static std::string state2string (State state) ;

    /// Refined public sub-state of the requiest once it's FINISHED as per
    /// the above defined primary state.
    enum ExtendedState {

        /// No extended state exists at this time        
        NONE,

        /// The request has been fully implemented
        SUCCESS,
        
        /// The request can not be implemented due to incorrect parameters, etc.
        BAD,
        
        /// The request could not be implemented due to an unrecoverable
        /// cliend-side error.
        CLIENT_ERROR,
        
        /// The request could not be implemented due to an unrecoverable
        /// server-side error.
        SERVER_ERROR,

        /// Expired due to a timeout (as per the Configuration)
        EXPIRED,
        
        /// Explicitly cancelled
        CANCELLED
    };

    /// Return the string representation of the extended state
    static std::string state2string (ExtendedState state) ;

    // Default construction and copy semantics are proxibited

    Request () = delete;
    Request (Request const&) = delete;
    Request & operator= (Request const&) = delete;

    /// Destructor
    virtual ~Request ();

    /// Return a pointer to the service provider,
    ServiceProvider::pointer serviceProvider() { return _serviceProvider; }

    /// Return a string representing a type of a request.
    const std::string& type () const {
        return _type;
    }

    /// Return a unique identifier of the request
    const std::string& id () const {
        return _id;
    }

    /// Return a unique identifier of the request
    const std::string& worker () const {
        return _worker;
    }

    /// Return the primary status of the request
    State state () const {
        return _state;
    }
    
    /// Return the extended state of the request when it finished.
    ExtendedState extendedState () const {
        return _extendedState;
    }

protected:

    /**
     * Generate a unique identifier of a request which can also be persisted.
     *
     * @return an identifier
     */
    static std::string generateId ();

    /**
     * Construct the request with the pointer to the services provider.
     *
     * @param serviceProvider - the pointer to the provider of serviceses
     * @param id              - a unique identifier of the request
     */
    Request (ServiceProvider::pointer serviceProvider,
             const std::string        &type,
             const std::string        &id,
             const std::string        &worker,
             boost::asio::io_service  &io_service);
    
    /**
     * Return a down-cust pointer onto an object of the final class.
     * This pointer is used by an implementation of this class for registering
     * asynchronous callback handlers to guarantee that the object always
     * oulive the asynchronous operations.
     *
     * The method is supposed to be implemented by final subclasses.
     */
    virtual std::shared_ptr<Request> final_shared_from_this ()=0;

    /**
     * Reset the state (if needed) and begin processing the request.
     *
     * This is supposed to be the first operation to be called upon a creation
     * of the request.
     */
    void start ();

    /**
     * Request expiration timer's handler. The expiration interval (if any)
     * is configured via the configuraton service. When the request expires
     * it finishes with completion status FINISHED::EXPIRED.
     */
    void expired (const boost::system::error_code &ec);

    /**
     * Cancel any asynchronous operation(s) and put the object into
     * the initial state
     */
    void cancel ();

    /**
     * Restart the whole operation from scratch.
     *
     * NOTE: This method is called internally when there is a doubt that
     * it's possible to do a clean recovery from a failure.
     */
    void restart ();

    /**
     * Cancel any asynchronous operation(s) if not in the initial state
     * w/o notifying a subscriber.
     */
    void reset();

    /// Start resolving the destination worker host & port
    void resolve ();

    /// Callback handler for the asynchronious operation
    void resolved (const boost::system::error_code &ec,
                   boost::asio::ip::tcp::resolver::iterator iter);

    /// Start resolving the destination worker host & port
    void connect (boost::asio::ip::tcp::resolver::iterator iter);

    /**
     * Callback handler for the asynchronious operation upon its
     * successfull completion will trigger a request-specific
     * protocol sequence.
     */
    void connected (const boost::system::error_code &ec,
                    boost::asio::ip::tcp::resolver::iterator iter);

    /**
     * Finalize request processing (as reported by subclasses)
     *
     * This is supposed to be the last operation to be called by subclasses
     * upon a completion of the request.
     */
    void finish (State         state,
                 ExtendedState extendedState);

    /**
      * This method is supposed to be provided by subclasses to begin
      * an actual protocol as required by the subclass.
      */
    virtual void beginProtocol ()=0;

    /**
     * This method is supposed to be provided by subclasses to handle
     * request completion steps, such as notifying a party which initiated
     * the request, etc.
     */
    virtual void endProtocol ()=0;

    /**
     * Return 'true' if the operation was aborted.
     *
     * USAGE NOTES:
     *
     *    Nomally this method is supposed to be called as the first action
     *    witin asynchronous handlers to figure out if an on-going aynchronous
     *    operation was cancelled for some reason. Should this be the case
     *    the caller is supposed to quit right away. It will be up to a code
     *    which initiated the abort to take care of putting the object into
     *    a proper state.
     */
    bool isAborted (const boost::system::error_code &ec) const;

    /**
     * Ensure the object is in the deseride internal state. Throw an
     * exception otherwise.
     *
     * NOTES: normally this condition should never been seen unless
     *        there is a problem with the application implementation
     *        or the underlying run-time system.
     * 
     * @throws std::logic_error
     */
    void assertState (State desiredState) const;

    /**
     * Set the desired primary and extended state.
     *
     * The change of the state is done via a method to allow extra actions
     * at this step, such as:
     *
     * - reporting change state in a debug stream
     * - verifying the correctness of the state transition
     */
    void setState (State         state,
                   ExtendedState extendedStat);

protected:

    // Parameters of the object

    ServiceProvider::pointer _serviceProvider;

    std::string _type;
    std::string _id;
    std::string _worker;

    // Primary and extended states of the request

    State         _state;
    ExtendedState _extendedState;

    // Buffers for data moved over the network

    std::auto_ptr<ProtocolBuffer> _bufferPtr;

    // To be initialized from a configuration via the ServiceProvider
    // then cached for the lifespan of the object.

    std::shared_ptr<WorkerInfo> _workerInfoPtr;

    unsigned int _timerIvalSec;

    // Mutable state for network communication

    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::socket   _socket;
    boost::asio::deadline_timer    _timer;

    // This timer is used (if configured) to limit the total run time
    // of a request. The timer (re-)starts each time method send() is called.
    // Upon a suvcessfull expiration of the timer the request would finish
    // with status: FINISHED::EXPIRED.
    
    unsigned int  _requestExpirationIvalSec;        // initialized from a configuration

    boost::asio::deadline_timer _requestExpirationTimer;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_REQUEST_H