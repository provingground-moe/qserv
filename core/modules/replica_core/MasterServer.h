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

#include "replica_core/DeleteRequest.h"
#include "replica_core/FindRequest.h"
#include "replica_core/FindAllRequest.h"
#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceManagementRequest.h"
#include "replica_core/StatusRequest.h"
#include "replica_core/StopRequest.h"

// Forward declarations


// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

class MasterServerImpl;
class ServiceProvider;

/**
 * The base class for implementing a polymorphic collection of active requests.
 */
struct RequestWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RequestWrapper> pointer;

    /// Destructor
    virtual ~RequestWrapper() {}

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

    /// Destructor
    virtual ~RequestWrapperImpl() {}

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
  *
  * NOTES:
  * - all methods launching, stopping or checking status of requests
  *   require that the server was runinng. Otherwise it will throw
  *   std::runtime_error. The current implementation of the server
  *   doesn't support (yet?) amn operation queuing mechanism.
  */
class MasterServer
    : public std::enable_shared_from_this<MasterServer>  {

public:

    /// Friend class behind this implementation must have access to
    /// the private methods
    friend class MasterServerImpl;

    /// The pointer type for instances of the class
    typedef std::shared_ptr<MasterServer> pointer;

    /**
     * The registry of requests
     *
     * IMPLEMENTATION NOTE: this type is placed into the public area of
     * the class to allow using it outside of the class within a compilation
     * unit matching this header.
     */
    typedef std::map<std::string,RequestWrapper::pointer> Registry;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, other services
     */
    static pointer create (ServiceProvider &serviceProvider);

    // Default construction and copy semantics are proxibited

    MasterServer () = delete;
    MasterServer (MasterServer const&) = delete;
    MasterServer & operator= (MasterServer const&) = delete;
 
    /// Destructor
    virtual ~MasterServer();

    /// Return the Service Provider used by the server
    ServiceProvider& serviceProvider () { return _serviceProvider; }

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
    void join ();

    /**
     * Initiate a new replication request.
     *
     * @param database              - database name
     * @param chunk                 - the chunk number
     * @param sourceWorkerName      - the name of a worker node from which to copy the chunk
     * @param destinationWorkerName - the name of a worker node where the replica will be created
     * @param onFinish              - an optional callback function to be called upon
     *                                the completion of the request
     *
     * @return a pointer to the replication request
     */
    ReplicationRequest::pointer replicate (const std::string                 &database,
                                           unsigned int                      chunk,
                                           const std::string                 &sourceWorkerName,
                                           const std::string                 &destinationWorkerName,
                                           ReplicationRequest::callback_type  onFinish=nullptr);

    /**
     * Initiate a new replica deletion request.
     *
     * @param database   - database name
     * @param chunk      - the chunk number
     * @param workerName - the name of a worker node where the replica will be deleted
     * @param onFinish   - an optional callback function to be called upon
     *                     the completion of the request
     *
     * @return a pointer to the replication request
     */
    DeleteRequest::pointer deleteReplica (const std::string            &database,
                                          unsigned int                  chunk,
                                          const std::string            &workerName,
                                          DeleteRequest::callback_type  onFinish=nullptr);

    /**
     * Initiate a new replica lookup request.
     *
     * @param database   - database name
     * @param chunk      - the chunk number
     * @param workerName - the name of a worker node where the replica is located
     * @param onFinish   - an optional callback function to be called upon
     *                     the completion of the request
     *
     * @return a pointer to the replication request
     */
    FindRequest::pointer findReplica (const std::string          &database,
                                      unsigned int                chunk,
                                      const std::string          &workerName,
                                      FindRequest::callback_type  onFinish=nullptr);

    /**
     * Initiate a new replicas lookup request.
     *
     * @param database   - database name
     * @param chunk      - the chunk number
     * @param workerName - the name of a worker node where the replicas are located
     * @param onFinish   - an optional callback function to be called upon
     *                     the completion of the request
     *
     * @return a pointer to the replication request
     */
    FindAllRequest::pointer findAllReplicas (const std::string             &database,
                                             unsigned int                   chunk,
                                             const std::string             &workerName,
                                             FindAllRequest::callback_type  onFinish=nullptr);

    /**
     * Stop an outstanding replication request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopReplicationRequest::pointer stopReplication (const std::string                     &workerName,
                                                     const std::string                     &targetRequestId,
                                                     StopReplicationRequest::callback_type  onFinish=nullptr);

    /**
     * Stop an outstanding replica deletion request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopDeleteRequest::pointer stopReplicaDelete (const std::string                &workerName,
                                                  const std::string                &targetRequestId,
                                                  StopDeleteRequest::callback_type  onFinish=nullptr);

    /**
     * Stop an outstanding replica lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopFindRequest::pointer stopReplicaFind (const std::string              &workerName,
                                              const std::string              &targetRequestId,
                                              StopFindRequest::callback_type  onFinish=nullptr);

    /**
     * Stop an outstanding replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopFindAllRequest::pointer stopReplicaFindAll (const std::string                 &workerName,
                                                    const std::string                 &targetRequestId,
                                                    StopFindAllRequest::callback_type  onFinish=nullptr);

    /**
     * Check the on-going status of an outstanding replication request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusReplicationRequest::pointer statusOfReplication (const std::string                       &workerName,
                                                           const std::string                       &targetRequestId,
                                                           StatusReplicationRequest::callback_type  onFinish=nullptr);
 
    /**
     * Check the on-going status of an outstanding replica deletion request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusDeleteRequest::pointer statusOfDelete (const std::string                  &workerName,
                                                 const std::string                  &targetRequestId,
                                                 StatusDeleteRequest::callback_type  onFinish=nullptr);

    /**
     * Check the on-going status of an outstanding replica lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusFindRequest::pointer statusOfFind (const std::string                &workerName,
                                             const std::string                &targetRequestId,
                                             StatusFindRequest::callback_type  onFinish=nullptr);

    /**
     * Check the on-going status of an outstanding (multiple) replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusFindAllRequest::pointer statusOfFindAll (const std::string                   &workerName,
                                                   const std::string                   &targetRequestId,
                                                   StatusFindAllRequest::callback_type  onFinish=nullptr);

    /**
     * Tell the worker-side service to temporarily suspend processing requests
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the request
     */
    ServiceSuspendRequest::pointer suspendWorkerService (const std::string                    &workerName,
                                                         ServiceSuspendRequest::callback_type  onFinish=nullptr);

    /**
     * Tell the worker-side service to resume processing requests
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the request
     */
    ServiceResumeRequest::pointer resumeWorkerService (const std::string                   &workerName,
                                                       ServiceResumeRequest::callback_type  onFinish=nullptr);
    /**
     * Request the current status of the worker-side service
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the request
     */
    ServiceStatusRequest::pointer statusOfWorkerService (const std::string                   &workerName,
                                                         ServiceStatusRequest::callback_type  onFinish=nullptr);
                                                         
    // Filters for active requests

    std::vector<ReplicationRequest::pointer>       activeReplicationRequests       () const;
    std::vector<DeleteRequest::pointer>            activeDeleteRequests            () const;
    std::vector<FindRequest::pointer>              activeFindRequests              () const;
    std::vector<FindAllRequest::pointer>           activeFindAllRequests           () const;

    std::vector<StopReplicationRequest::pointer>   activeStopReplicationRequests   () const;
    std::vector<StopDeleteRequest::pointer>        activeStopDeleteRequests        () const;
    std::vector<StopFindRequest::pointer>          activeStopFindRequests          () const;
    std::vector<StopFindAllRequest::pointer>       activeStopFindAllRequests       () const;

    std::vector<StatusReplicationRequest::pointer> activeStatusReplicationRequests () const;
    std::vector<StatusDeleteRequest::pointer>      activeStatusDeleyeRequests      () const;
    std::vector<StatusFindRequest::pointer>        activeStatusFindRequests        () const;
    std::vector<StatusFindAllRequest::pointer>     activeStatusFindAllRequests     () const;

    std::vector<ServiceSuspendRequest::pointer>    activeServiceSuspendRequests    () const;
    std::vector<ServiceResumeRequest::pointer>     activeServiceResumeRequests     () const;
    std::vector<ServiceStatusRequest::pointer>     activeServiceStatusRequests     () const;

    // Counters of active requests

    size_t numActiveReplicationRequests       () const;
    size_t numActiveDeleteRequests            () const;
    size_t numActiveFindRequests              () const;
    size_t numActiveFindAllRequests           () const;

    size_t numActiveStopReplicationRequests   () const;
    size_t numActiveStopDeleteRequests        () const;
    size_t numActiveStopFindRequests          () const;
    size_t numActiveStopFindAllRequests       () const;

    size_t numActiveStatusReplicationRequests () const;
    size_t numActiveStatusDeleteRequests      () const;
    size_t numActiveStatusFindRequests        () const;
    size_t numActiveStatusFindAllRequests     () const;

    size_t numActiveServiceSuspendRequests    () const;
    size_t numActiveServiceResumeRequests     () const;
    size_t numActiveServiceStatusRequests     () const;

private:

    /**
     * Construct the server with the specified configuration.
     *
     * @param serviceProvider - for configuration, other services
     */
    explicit MasterServer (ServiceProvider &serviceProvider);

    /**
     * Finalize the completion of the request. This method will notify
     * a requestor on the completion of the operation and it will also
     * remove the request from the server's registry.
     */
    void finish (const std::string &id);

    /**
     * Make sure the server is runnning. Otherwise throw std::runtime_error.
     */
    void assertIsRunning () const;

private:

    // Parameters of the object

    ServiceProvider &_serviceProvider;

    // Communication and synchronization context

    boost::asio::io_service _io_service;

    std::unique_ptr<boost::asio::io_service::work> _work;

    std::unique_ptr<std::thread> _thread;   // this thread will run the asynchronous prosessing
                                            // of the requests.

    mutable std::mutex _requestPocessingMtx;    // for thread safety of the implementation of
                                                // this class's public API and internal operations.

    // The registry of the on-going requests.

    Registry _requestsRegistry;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_MASTERSERVER_H