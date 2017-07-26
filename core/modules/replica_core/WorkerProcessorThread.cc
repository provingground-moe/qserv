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
#include "replica_core/SuccessRateGenerator.h"
#include "replica_core/WorkerProcessor.h"
#include "replica_core/WorkerReplicationRequest.h"

namespace lsst {
namespace qserv {
namespace replica_core {

WorkerProcessorThread::pointer
WorkerProcessorThread::create (const WorkerProcessor_pointer &processor) {
    return pointer (
        new WorkerProcessorThread (processor)
    );
}

WorkerProcessorThread::WorkerProcessorThread (const WorkerProcessor_pointer &processor)
    :   _processor (processor),
        _stop      (false),
        _cancel    (false)

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
    _thread = std::make_shared<std::thread> ( [self] () {

        // Simulate request 'processing' for the random duration of time
        // witin this interval of milliseconds. Success/failure modes will
        // be also simulated using the corresponding generator.

        BlockPost            blockPost(1000, 10000);
        SuccessRateGenerator successRateGenerator(0.9);

        std::cout << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "] begin" << std::endl;

        while (!self->_stop) {
            
            // Get the next request to process if any. This operation will block
            // until either the next request is available (returned a valid pointer)
            // or the specified timeout expires. In either case this thread has a chance
            // to re-evaluate the stopping condition.

            WorkerReplicationRequest::pointer request = self->_processor->fetchNextForProcessing(self,1000);

            if (self->_cancel) {
                self->cancelled(request);
                continue;
            }
            if (self->_stop) {
                if (request) self->_processor->processingRefused(request);
                continue;
            }
            if (request) {

                std::cout
                    << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "]"
                    << " begin  processing id=" << request->id()
                    << ", database=" << request->database()
                    << ", chunk=" << request->chunk() << std::endl;

                // Simulate processing the request

                blockPost.wait();
                
                // Verify if the request should be cancelled, and if so then (if possibly) cleanup
                // before finishing this thread.

                if (self->_cancel) {
                    std::cout
                        << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "]"
                        << " cancel processing id=" << request->id()
                        << ", database=" << request->database()
                        << ", chunk=" << request->chunk() << std::endl;

                    self->cancelled(request);
                    continue;
                }

                // Simulate random failures and set proper completion status values

                const WorkerReplicationRequest::CompletionStatus completionStatus =
                    successRateGenerator.success() ? WorkerReplicationRequest::CompletionStatus::SUCCEEDED :
                                                     WorkerReplicationRequest::CompletionStatus::FAILED;

                std::cout
                    << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "]"
                    << " end    processing id=" << request->id()
                    << ", database=" << request->database()
                    << ", chunk=" << request->chunk()
                    << ", completionStatus=" << WorkerReplicationRequest::status2string(completionStatus) << std::endl;

                self->_processor->processingFinished (
                    request,
                    completionStatus);
            }
        }
        std::cout << "WorkerProcessorThread::run [" << std::this_thread::get_id() << "] end" << std::endl;

        self->stopped();
    });
}

void
WorkerProcessorThread::stop () {
    if (!isRunning()) return;
    _stop = true;
}

void
WorkerProcessorThread::stopped () {
    _stop = false;
    _thread->detach();
    _thread = nullptr;
    _processor->processorThreadStopped(shared_from_this());
}


void
WorkerProcessorThread::cancel () {
    if (!isRunning()) return;
    _cancel = true;
}

void
WorkerProcessorThread::cancelled (const WorkerReplicationRequest::pointer &request) {
    _cancel= false;
    if (request) _processor->processingCancelled(request);
}

}}} // namespace lsst::qserv::replica_core
