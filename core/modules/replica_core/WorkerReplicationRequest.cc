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

#include "replica_core/WorkerReplicationRequest.h"

// System headers

#include <iostream>
#include <stdexcept>

// Qserv headers

namespace lsst {
namespace qserv {
namespace replica_core {

WorkerReplicationRequest::pointer
WorkerReplicationRequest::create (WorkerReplicationRequest::Priority priority,
                                  const std::string&                 id,
                                  const std::string&                 database,
                                  unsigned int                       chunk) {
    return pointer (
        new WorkerReplicationRequest (priority,
                                      id,
                                      database,
                                      chunk)
    );
}

WorkerReplicationRequest::WorkerReplicationRequest (WorkerReplicationRequest::Priority priority,
                                                    const std::string&                 id,
                                                    const std::string&                 database,
                                                    unsigned int                       chunk)
    :   _priority(priority),
        _id      (id),
        _database(database),
        _chunk   (chunk),
        _status  (CompletionStatus::NONE)
{}

WorkerReplicationRequest::~WorkerReplicationRequest () {
}

void
WorkerReplicationRequest::setStatus (WorkerReplicationRequest::CompletionStatus status) {
    _status = status;
}
 
void
WorkerReplicationRequest::setProcessorThread (WorkerProcessorThread_pointer processorThread) {
    _processorThread = processorThread;
}


}}} // namespace lsst::qserv::replica_core

