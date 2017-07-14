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

#include "replica_core/WorkerProcessor.h"

// System headers

#include <stdexcept>
#include <iostream>

// Qserv headers


// This macro to appear witin each block which requires thread safety

#define THREAD_SAFE_BLOCK \
std::lock_guard<std::mutex> lock(_mtx);

namespace lsst {
namespace qserv {
namespace replica_core {


class WorkerProcessorThread {

public:

    /// Normal constructor
    explicit WorkerProcessorThread (WorkerProcessor::pointer processor)
        :   _processor(processor) {
    }
    
    // Default construction and copy semantics are proxibited

    WorkerProcessorThread () = delete;
    WorkerProcessorThread (WorkerProcessorThread const&) = delete;
    WorkerProcessorThread & operator= (WorkerProcessorThread const&) = delete;

    /// Destructor
    virtual ~WorkerProcessorThread () {
        // TODO: detach or join(?) the thred if any.
        ;
    }

    /// Create and run the thread
    void run () {
        if (!_thread) {
            _thread = std::make_shared<std::thread>([&_processor]() {

                // TODO: in the infinite loop:
                //
                // - wait for the next availabe request
                // - fetch the one and give own hared pointer to be recorded with
                //   the request in case if the processing will need to be stopped/aborted
                // - emulate its processing using:
                //   + a variable timeout to simulate the duration of the request
                //   + and some random mechanism to emulate variable completion status
                //     of the operation
                // - when finished return the request to the completed queue
                ;
            });
        }
    }

    /// Stop the thread
    void stop () {
        if (_thread) {
            // TODO: think about how to tell the thread to stop processing.
            ;
        }
    }
    
private:

    WorkerProcessor::pointer _processor;

    std::shared_ptr<std::thread> _thread;
};

WorkerProcessor::pointer
WorkerProcessor::create (ServiceProvider::pointer serviceProvider) {
    return WorkerProcessor::pointer (
        new WorkerProcessor (serviceProvider));
}

WorkerProcessor::WorkerProcessor (ServiceProvider::pointer serviceProvider)
    :   _serviceProvider (serviceProvider)

{}

WorkerProcessor::~WorkerProcessor () {
}

void
WorkerProcessor::run () {

    if (isRunning()) return;

    THREAD_SAFE_BLOCK {
    
        const size_t numThreads = _serviceProvider->config().workerNumProcessingThreads();
        if (!numThreads) throw new std::out_of_range("the number of procesisng threads can't be 0");
        
        for (size_t i=0; i < numThreads; ++i) {
            
            std::make_shared<WorkerProcessorThread> processingThread =
                std::make_shared<WorkerProcessorThread>(shared_from_this());

            _threads.push_back(processingThread);
        }
    }
}

void
WorkerProcessor::replicate (const proto::ReplicationRequestReplicate &request,
                                   proto::ReplicationResponseReplicate &response) {

    THREAD_SAFE_BLOCK {

        std::cout << "WorkerProcessor::replicate "
                  << " id="       << request.id()       << ","
                  << " database=" << request.database() << ","
                  << " chunk="    << request.chunk()    << std::endl;
    
        WorkerReplicationRequest workerRequest;
        workerRequest.priority = WorkerReplicationRequest::LOW;
        workerRequest.id       = request.id();
        workerRequest.database = request.database();
        workerRequest.chunk    = request.chunk();
        workerRequest.status   = WorkerReplicationRequest::NONE;
    
        _newRequests.push(workerRequest);
        
        // TODO: send a signal via a condition variable to notify
        // processing threads that there is a new request.
 
        response.set_status (proto::ReplicationStatus::QUEUED);
    }
}

void
WorkerProcessor::stop (const proto::ReplicationRequestStop &request,
                              proto::ReplicationResponseStop &response) {

    THREAD_SAFE_BLOCK {

        std::cout << "WorkerProcessor::stop "
                  << " id=" << request.id() <<  std::endl;
    
        // TODO: search for a request with this identifier in all queues.
        // Then take proper actions depending on a queue:
        // - just remove it fom the new requests queue
        // - find a processing thread which is processing request and tell it to stop, then wait 
 
        response.set_status (proto::ReplicationStatus::BAD);
    }
}

void
WorkerProcessor::status (const proto::ReplicationRequestStatus &request,
                                proto::ReplicationResponseStatus &response) {

    THREAD_SAFE_BLOCK {

        std::cout << "WorkerProcessor::status "
                  << " id=" << request.id() <<  std::endl;

        // TODO: search for a request with this identifier in all queues.
        // Then return its status.

        response.set_status (proto::ReplicationStatus::SUCCESS);
    }
}


WorkerReplicationRequest::pointer
WorkerProcessor::next (WorkerProcessorThread::pointer processorThread,
                       std::chrono::milliseconds      timeoutMilliseconds) {

    THREAD_SAFE_BLOCK {
    
        std::cout << "WorkerProcessor::next ** NOT IMPLEMENTED **" <<  std::endl;
        
        return WorkerReplicationRequest::pointer();
    }
}

void
WorkerProcessor::finish (WorkerReplicationRequest::pointer) {
    
    THREAD_SAFE_BLOCK {
    
        std::cout << "WorkerProcessor::finish ** NOT IMPLEMENTED **" <<  std::endl;
    }
}

}}} // namespace lsst::qserv::replica_core
