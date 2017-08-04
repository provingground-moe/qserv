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

#include "replica_core/WorkerRequest.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

std::string
WorkerRequest::status2string (CompletionStatus status) {
    switch (status) {
        case STATUS_NONE:          return "STATUS_NONE";
        case STATUS_IN_PROGRESS:   return "STATUS_IN_PROGRESS";
        case STATUS_IS_CANCELLING: return "STATUS_IS_CANCELLING";
        case STATUS_CANCELLED:     return "STATUS_CANCELLED";
        case STATUS_SUCCEEDED:     return "STATUS_SUCCEEDED";
        case STATUS_FAILED:        return "STATUS_FAILED";
    }
    throw std::logic_error("WorkerRequest::status2string - unhandled status: " + std::to_string(status));
}

std::string
WorkerRequest::priorityLevel2string (PriorityLevel level) {
    switch (level) {
        case PRIORITY_LEVEL_LOW:      return "PRIORITY_LEVEL_LOW";
        case PRIORITY_LEVEL_MEDIUM:   return "PRIORITY_LEVEL_MEDIUM";
        case PRIORITY_LEVEL_HIGH:     return "PRIORITY_LEVEL_HIGH";
        case PRIORITY_LEVEL_CRITICAL: return "PRIORITY_LEVEL_CRITICAL";
    }
    throw std::logic_error("WorkerRequest::priorityLevel2string - unhandled level: " + std::to_string(level));
}

WorkerRequest::WorkerRequest (const std::string            &type,
                              WorkerRequest::PriorityLevel  priorityLevel,
                              const std::string            &id)
    :   _type         (type),
        _priorityLevel(priorityLevel),
        _id          (id),
        _status      (STATUS_NONE)
{}

WorkerRequest::~WorkerRequest () {
}

void
WorkerRequest::setStatus (CompletionStatus status) {
    LOGS(_log, LOG_LVL_DEBUG, context() << "setStatus  "
         << WorkerRequest::status2string(_status) << " -> "
         << WorkerRequest::status2string(status));
    _status = status;
}

void
WorkerRequest::beginProgress () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "beginProgress");

    switch (status()) {

        case STATUS_NONE:
            setStatus(STATUS_IN_PROGRESS);
            break;

        default:
            throw std::logic_error("WorkerRequest::beginProgress not allowed while in status: " +
                                    WorkerRequest::status2string(status()));
    }
}

void
WorkerRequest::cancel () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancel");

    switch (status()) {

        case STATUS_NONE:
        case STATUS_CANCELLED:
            setStatus(STATUS_CANCELLED);
            break;

        case STATUS_IN_PROGRESS:
        case STATUS_IS_CANCELLING:
            setStatus(STATUS_IS_CANCELLING);
            break;

        default:
            throw std::logic_error("WorkerRequest::cancel not allowed while in status: " +
                                    WorkerRequest::status2string(status()));
    }
}

void
WorkerRequest::rollback () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "rollback");

    switch (status()) {

        case STATUS_NONE:
        case STATUS_IN_PROGRESS:
            setStatus(STATUS_NONE);
            break;

        case STATUS_IS_CANCELLING:
            setStatus(STATUS_CANCELLED);
            throw WorkerRequestCancelled();
            break;

        default:
            throw std::logic_error("WorkerRequest::rollback not allowed while in status: " +
                                    WorkerRequest::status2string(status()));
    }
}
}}} // namespace lsst::qserv::replica_core
