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

#include "replica_core/WorkerProcessorThread.h"

// System headers

#include <iostream>
#include <stdexcept>

// Qserv headers

#include "replica_core/BlockPost.h"
#include "replica_core/WorkerReplicationRequest.h"

namespace lsst {
namespace qserv {
namespace replica_core {

WorkerProcessorThread::pointer
WorkerProcessorThread::create (WorkerProcessor_pointer processor) {
    return pointer (
        new WorkerProcessorThread (processor)
    );
}

WorkerProcessorThread::WorkerProcessorThread (WorkerProcessor_pointer processor)
    :   _processor (processor),
        _stop(false)

{}

WorkerProcessorThread::~WorkerProcessorThread () {
}


bool
WorkerProcessorThread::isRunning () const {
    return _thread.get() != nullptr;
}

void
WorkerProcessorThread::run () {

    if (isRunning()) return;

    WorkerProcessorThread::pointer self = shared_from_this();
    _thread = std::make_shared<std::thread>([&self]() {

        // Simulate request 'processing' for the random duration of time
        // witin this interval of milliseconds.

        BlockPost blockPost(1000, 10000);

        std::cout << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "] begin" << std::endl;

        while (!self->_stop) {
            
            // Get the next request to process if any. This operation will block
            // until either the next request is available (returned a valid pointer)
            // or if this thread need to re-evaluate the stopping condition.
            
            WorkerReplicationRequest::pointer request = self->_processor->next(self);
            if (request) {
                std::cout << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "] begin processing id=" request->id() << ", database=" << request->database() << ", chunk=" << request->chunk() << std::endl;
                blockPost.wait();
                
                // Cancel the request and (possibly cleanup) if the thread was told
                // to stop while processing the request.

                if (self->_stop) {
                    // TODO: finish
                    ;
                }
                
                std::cout << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "] end   processing id=" request->id() << ", database=" << request->database() << ", chunk=" << request->chunk() << std::endl;
            }
        }
        
        // TODO: consider using std::lock_guard<std::mutex> 
        self->_stop = false;

        std::cout << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "] end" << std::endl;
    });
}

void
WorkerProcessorThread::stopAndJoin () {

    if (!isRunning()) return;

    // TODO: consider using std::lock_guard<std::mutex> 
    _stop = true;

    _thread->join();
}


}}} // namespace lsst::qserv::replica_core
