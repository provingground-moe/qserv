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

#include "replica_core/WorkerRequestProcessor.h"

// System headers

#include <iostream>

// Qserv headers

namespace lsst {
namespace qserv {
namespace replica_core {

WorkerRequestProcessor::pointer
WorkerRequestProcessor::create (ServiceProvider::pointer serviceProvider) {
    return WorkerRequestProcessor::pointer (
        new WorkerRequestProcessor (serviceProvider));
}

WorkerRequestProcessor::WorkerRequestProcessor (ServiceProvider::pointer serviceProvider)
    :   _serviceProvider (serviceProvider)

{}

WorkerRequestProcessor::~WorkerRequestProcessor () {
}

void
WorkerRequestProcessor::replicate (const proto::ReplicationRequestReplicate &request,
                                   proto::ReplicationResponseReplicate &response) {

    std::cout << "request <REPLICATE> : \n"
              << "  database : " << request.database() << "\n"
              << "  chunk    : " << request.chunk()    << "\n"
              << "  id       : " << request.id()       << std::endl;

    response.set_status (proto::ReplicationStatus::QUEUED);
}

void
WorkerRequestProcessor::stop (const proto::ReplicationRequestStop &request,
                              proto::ReplicationResponseStop &response) {

    std::cout << "request <STOP> : \n"
              << "  id       : " << request.id() << std::endl;

    response.set_status (proto::ReplicationStatus::BAD);
}

void
WorkerRequestProcessor::status (const proto::ReplicationRequestStatus &request,
                                proto::ReplicationResponseStatus &response) {

    std::cout << "request <STATUS> : \n"
              << "  id       : " << request.id() << std::endl;

    response.set_status (proto::ReplicationStatus::SUCCESS);
}

}}} // namespace lsst::qserv::replica_core
