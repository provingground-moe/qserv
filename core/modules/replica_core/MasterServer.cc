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
#include "replica_core/MasterServer.h"

// System headers

#include <iostream>
#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/ReplicationRequest.h"

// This macro to appear witin each block which requires thread safety

#define THREAD_SAFE_BLOCK \
std::lock_guard<std::mutex> lock(_requestPocessingMtx);

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.MasterServer");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

MasterServer::pointer
MasterServer::create (ServiceProvider::pointer serviceProvider)
{
    return pointer (
        new MasterServer (
            serviceProvider
        )
    );
}

MasterServer::MasterServer (ServiceProvider::pointer serviceProvider)
    :   _serviceProvider (serviceProvider),
        _io_service (),
        _work       (nullptr),
        _thread     (nullptr),
        _requestsRegistry (std::make_shared<RequestsRegistry>())
{}
    
void
MasterServer::run () {

    if (isRunning()) return;

    THREAD_SAFE_BLOCK {

        MasterServer::pointer server = shared_from_this();
     
        _work.reset (
            new boost::asio::io_service::work(_io_service)
        );
        _thread.reset (
            new std::thread (
                [server] () {
        
                    // This will prevent the I/O service from existing the .run()
                    // method event when it will run ourt of any requess to process.
                    // Unless the service will explicitly stopped.
    
                    server->_io_service.run();
                    
                    // Always reset the object in a preparation for its further
                    // usage.
    
                    server->_io_service.reset();
                }
            )
        );
    }
}

bool
MasterServer::isRunning () const {
    return _thread.get() != nullptr;
}

void
MasterServer::stop () {

    if (!isRunning()) return;

    // IMPORTANT:
    //
    //   Never attempt running these operations within THREAD_SAFE_BLOCK
    //   due to a possibile deadlock when asynchronous handlers will be
    //   calling the thread-safe methods. A problem is that until they finish
    //   in a clean way (as per the _work.reset()) the thread will never finish,
    //   and the application will hang on _thread->join().

    // THREAD_SAFE_BLOCK  (disabled)

    // Destoring this object will let the I/O service to (eventually) finish
    // all on-going work and shut down the thread. In that case there is no need
    // to stop the service explicitly (which is not a good idea anyway because
    // there may be outstanding synchronous requests, in which case the service
    // would get into an unpredictanle state.)

    _work.reset();

    // Join with the thread before clearning up the pointer

    _thread->join();

    _thread.reset(nullptr);
    
    // Double check that the collection of requests is empty.
    
    if (!_requestsRegistry->empty())
        throw std::logic_error ("MasterServer::stop() the collection of outstanding requests is not empty");
}

ReplicationRequest::pointer
MasterServer::replicate (const std::string                 &database,
                         unsigned int                      chunk,
                         const std::string                 &sourceWorkerName,
                         const std::string                 &destinationWorkerName,
                         ReplicationRequest::callback_type onFinish) {

    assertIsRunning();

    THREAD_SAFE_BLOCK {

        MasterServer::pointer server = shared_from_this();

        ReplicationRequest::pointer request =
            ReplicationRequest::create (
                _serviceProvider,
                database,
                chunk,
                sourceWorkerName,
                destinationWorkerName,
                _io_service,
                [server] (ReplicationRequest::pointer request) {
                    server->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (*_requestsRegistry)[request->id()] =
            std::make_shared<RequestWrapperImpl<ReplicationRequest>> (request, onFinish);
    
        // Initiate the request

        request->start ();

        return request;
    }
}

std::vector<ReplicationRequest::pointer>
MasterServer::activeReplications () const {
    THREAD_SAFE_BLOCK {
        return requestsByType<ReplicationRequest>();
    }
}


StopRequest::pointer
MasterServer::stopReplication (const std::string          &workerName,
                               const std::string          &replicationRequestId,
                               StopRequest::callback_type onFinish) {
    assertIsRunning();

    THREAD_SAFE_BLOCK {

        MasterServer::pointer server = shared_from_this();

        StopRequest::pointer request =
            StopRequest::create (
                _serviceProvider,
                workerName,
                _io_service,
                replicationRequestId,
                [server] (StopRequest::pointer request) {
                    server->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (*_requestsRegistry)[request->id()] =
            std::make_shared<RequestWrapperImpl<StopRequest>> (request, onFinish);
    
        // Initiate the request

        request->start ();

        return request;
    }
}

std::vector<StopRequest::pointer>
MasterServer::activeStopReplications () const {
    THREAD_SAFE_BLOCK {
        return requestsByType<StopRequest>();
    };       
}


StatusRequest::pointer
MasterServer::statusOfReplication (const std::string            &workerName,
                                   const std::string            &replicationRequestId,
                                   StatusRequest::callback_type onFinish) {
    assertIsRunning();

    THREAD_SAFE_BLOCK {

        LOGS(_log, LOG_LVL_DEBUG, "statusOfReplication  replicationRequestId = " << replicationRequestId);

        MasterServer::pointer server = shared_from_this();

        StatusRequest::pointer request =
            StatusRequest::create (
                _serviceProvider,
                workerName,
                _io_service,
                replicationRequestId,
                [server] (StatusRequest::pointer request) {
                    server->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (*_requestsRegistry)[request->id()] =
            std::make_shared<RequestWrapperImpl<StatusRequest>> (request, onFinish);
    
        // Initiate the request

        request->start ();

        return request;
    }
}


std::vector<StatusRequest::pointer>
MasterServer::activeStatusInqueries () const {
    THREAD_SAFE_BLOCK {
        return requestsByType<StatusRequest>();
    }
}


ServiceSuspendRequest::pointer
MasterServer::suspendWorkerService (const std::string                    &workerName,
                                    ServiceSuspendRequest::callback_type  onFinish) {
    THREAD_SAFE_BLOCK {

        LOGS(_log, LOG_LVL_DEBUG, "suspendWorkerService ");

        MasterServer::pointer server = shared_from_this();

        ServiceSuspendRequest::pointer request =
            ServiceSuspendRequest::create (
                _serviceProvider,
                workerName,
                _io_service,
                [server] (ServiceSuspendRequest::pointer request) {
                    server->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (*_requestsRegistry)[request->id()] =
            std::make_shared<RequestWrapperImpl<ServiceSuspendRequest>> (request, onFinish);
    
        // Initiate the request

        request->start ();

        return request;
    }
}


std::vector<ServiceSuspendRequest::pointer>
MasterServer::activeServiceSuspendRequests () const {
    THREAD_SAFE_BLOCK {
        return requestsByType<ServiceSuspendRequest>();
    }
}

ServiceResumeRequest::pointer
MasterServer::resumeWorkerService (const std::string                   &workerName,
                                   ServiceResumeRequest::callback_type  onFinish) {
    THREAD_SAFE_BLOCK {
        
        LOGS(_log, LOG_LVL_DEBUG, "resumeWorkerService");

        MasterServer::pointer server = shared_from_this();

        ServiceResumeRequest::pointer request =
            ServiceResumeRequest::create (
                _serviceProvider,
                workerName,
                _io_service,
                [server] (ServiceResumeRequest::pointer request) {
                    server->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (*_requestsRegistry)[request->id()] =
            std::make_shared<RequestWrapperImpl<ServiceResumeRequest>> (request, onFinish);
    
        // Initiate the request

        request->start ();

        return request;
    }
}


std::vector<ServiceResumeRequest::pointer>
MasterServer::activeServiceResumeRequests () const {
    THREAD_SAFE_BLOCK {
        return requestsByType<ServiceResumeRequest>();
    }
}


ServiceStatusRequest::pointer
MasterServer::statusOfWorkerService (const std::string                   &workerName,
                                     ServiceStatusRequest::callback_type  onFinish) {
    THREAD_SAFE_BLOCK {
        
        LOGS(_log, LOG_LVL_DEBUG, "statusOfWorkerService");

        MasterServer::pointer server = shared_from_this();

        ServiceStatusRequest::pointer request =
            ServiceStatusRequest::create (
                _serviceProvider,
                workerName,
                _io_service,
                [server] (ServiceStatusRequest::pointer request) {
                    server->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (*_requestsRegistry)[request->id()] =
            std::make_shared<RequestWrapperImpl<ServiceStatusRequest>> (request, onFinish);
    
        // Initiate the request

        request->start ();

        return request;
    }
}


std::vector<ServiceStatusRequest::pointer>
MasterServer::activeServiceStatusRequests () const {
    THREAD_SAFE_BLOCK {
        return requestsByType<ServiceStatusRequest>();
    }
}


void
MasterServer::finish (const std::string &id) {

    // IMPORTANT:
    //
    //   Make sure the notification is complete before removing
    //   the request from the registry. This has two reasons:
    //
    //   - it will avoid a possibility of deadlocking in case if
    //     the callback function to be notified will be doing
    //     any API calls of the server.
    //
    //   - it will reduce the server API dead-time due to a prolonged
    //     execution time of of the callback function.

    (*_requestsRegistry)[id]->notify();

    THREAD_SAFE_BLOCK {
        _requestsRegistry->erase(id);
    }
}

void
MasterServer::assertIsRunning () const {
    if (!isRunning())
        throw std::runtime_error("the replication service is not running");
}

}}} // namespace lsst::qserv::replica_core
