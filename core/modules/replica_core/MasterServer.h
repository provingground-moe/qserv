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
#ifndef LSST_QSERV_REPLICA_CORE_MASTERSERVER_H
#define LSST_QSERV_REPLICA_CORE_MASTERSERVER_H

/// MasterServer.h declares:
///
/// class MasterServer
/// (see individual class documentation for more information)

// System headers

#include <boost/asio.hpp>

#include <map>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <mutex>        // std::mutex, std::lock_guard
#include <thread>
#include <vector>

// Qserv headers

#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/StatusRequest.h"
#include "replica_core/StopRequest.h"

// Forward declarations


// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
 * The base class for implementing a polymorphic collection of active requests.
 */
struct RequestWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RequestWrapper> pointer;

    /// This method will be called upon a completion of a request
    /// to notify a subscriber on the event.
    virtual void notify ()=0;

    /// Return a pointer to the stored request object
    virtual std::shared_ptr<Request> request () const=0;
};

/**
 * Request-type specific wrappers
 */
template <class  T>
struct RequestWrapperImpl
    :   RequestWrapper {

    /// The implementation of the vurtual method defined in the base class
    virtual void notify () {
        if (_onFinish == nullptr) return;
        _onFinish(_request);
    }

    RequestWrapperImpl(typename T::pointer request,
                       typename T::callback_type onFinish)
        :   _request  (request),
            _onFinish (onFinish)
    {}

    virtual std::shared_ptr<Request> request () const { return _request; }

private:

    // The context of the operation
    
    typename T::pointer       _request;
    typename T::callback_type _onFinish;
};

/**
  * Class MasterServer is used for pushing replication (etc.) requests
  * to the worker replication services. Only one instance of this class is
  * allowed per a thread.
  */
class MasterServer
    : public std::enable_shared_from_this<MasterServer>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<MasterServer> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, etc. services
     */
    static pointer create (ServiceProvider::pointer serviceProvider);

    // Default construction and copy semantics are proxibited

    MasterServer () = delete;
    MasterServer (MasterServer const&) = delete;
    MasterServer & operator= (MasterServer const&) = delete;

    /**
     * Run the server in a dedicated thread unless it's already running.
     * It's safe to call this method multiple times from any thread.
     */
    void run ();

    /**
     * Check if the service is running.
     *
     * @return true if the service is running.
     */
    bool isRunning () const;

    /**
     * Stop the server. This method will guarantee that all outstanding
     * opeations will finish and not aborted.
     *
     * This operation will also result in stopping the internal thread
     * in which the server is being run.
     */
    void stop ();

    /**
     * Join with a thread in which the service is being run (if any).
     * If the service was not started or if it's stopped the the method
     * will return immediattely.
     *
     * This method is meant to be used for integration of the service into
     * a larger multi-threaded application which may require a proper
     * synchronization between threads.
     */
    void join () {
        if (_thread) _thread->join();
    }

    /**
     * Initiate a new replication request.
     *
     * IMPORTANT: this operation requires that the server was runing.
     * Otherwise if will throw exception std::runtime_error.
     *
     * @param database              - database name
     * @param chunk                 - the chunk number
     * @param sourceWorkerName      - the name of a worker node from which to copy
     *                                the chunk
     * @param destinationWorkerName - the name of a worker node where the replica
     *                                will be created
     * @param onFinish              - an optional callback function to be called upon
     *                                the completion of the request
     *
     * @return a pointer to the replication request
     */
    ReplicationRequest::pointer replicate (const std::string                 &database,
                                           unsigned int                      chunk,
                                           const std::string                 &sourceWorkerName,
                                           const std::string                 &destinationWorkerName,
                                           ReplicationRequest::callback_type onFinish=nullptr);

    /**
     * Return a list of the on-going replication requests.
     */
    std::vector<ReplicationRequest::pointer> activeReplications () const;

    /**
     * Stop an outstanding replication request.
     *
     * IMPORTANT: this operation requires that the server was running.
     *            Otherwise it will throw std::runtime_error.
     *
     * @param workerName           - the name of a worker node where the request was launched
     * @param replicationRequestId - an identifier of a request to be stopped
     * @param onFinish             - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopRequest::pointer stopReplication (const std::string          &workerName,
                                          const std::string          &replicationRequestId,
                                          StopRequest::callback_type onFinish=nullptr);

    /**
     * Return a list of the on-going stop requests.
     */
    std::vector<StopRequest::pointer> activeStopReplications () const;

    /**
     * Check the on-going status of an outstanding replication request.
     *
     * IMPORTANT: this operation requires that the server was runinng.
     *            Otherwise it will throw std::runtime_error.
     *
     * @param workerName           - the name of a worker node where the request was launched
     * @param replicationRequestId - an identifier of a request to be inspected
     * @param onFinish             - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusRequest::pointer statusOfReplication (const std::string            &workerName,
                                                const std::string            &replicationRequestId,
                                                StatusRequest::callback_type onFinish=nullptr);

    /**
     * Return a list of the on-going status inquery requests.
     */
    std::vector<StatusRequest::pointer> activeStatusInqueries () const;

private:

    /**
     * Construct the server with the specified configuration.
     *
     * @param serviceProvider - for configuration, etc. services
     */
    explicit MasterServer (ServiceProvider::pointer serviceProvider);

    /**
     * Finalize the completion of the request. This method will notify
     * a requestor on the completion of the operation and it will also
     * remove the request from the server's registry.
     */
    void finish (const std::string &id);

    /**
     * Return a collection of requests filtered by type.
     *
     * ATTENTION: This is implementation is not thread-safe. It needs
     * to be used from the thread-safe code only.
     */
    template <class T>
    std::vector<typename T::pointer> requestsByType () const {

        std::vector<typename T::pointer> result;

        for (auto itr : (*_requestsRegistry)) {
        
            // Filter the request by leaving the ones of the desired type

            typename T::pointer request =
                std::dynamic_pointer_cast<T>(itr.second->request());

            if (request)
                result.push_back(request);
        }
        return result;
    }

    /**
     * Make sure the server is runnning. Otherwise throw std::runtime_error.
     */
    void assertIsRunning () const;

private:

    // Parameters of the object

    ServiceProvider::pointer _serviceProvider;

    // Communication and synchronization context

    boost::asio::io_service _io_service;

    std::unique_ptr<boost::asio::io_service::work> _work;

    std::unique_ptr<std::thread> _thread;   // this thread will run the asynchronous prosessing
                                            // of the requests.

    mutable std::mutex _requestPocessingMtx;    // for thread safety of the implementation of
                                                // this class's public API and internal operations.

    // The registry of the on-going requests.

    typedef std::map<std::string,RequestWrapper::pointer> RequestsRegistry;

    std::shared_ptr<RequestsRegistry> _requestsRegistry;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_MASTERSERVER_H