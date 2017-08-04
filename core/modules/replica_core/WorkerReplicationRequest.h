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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERREPLICATIONREQUEST_H
#define LSST_QSERV_REPLICA_CORE_WORKERREPLICATIONREQUEST_H

/// WorkerReplicationRequest.h declares:
///
/// class WorkerReplicationRequest
/// (see individual class documentation for more information)

// System headers

#include <string>

// Qserv headers

#include "replica_core/WorkerRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {


/**
  * Class WorkerReplicationRequest represents a context and a state of replication
  * requsts within the worker servers.
  */
class WorkerReplicationRequest
    :   public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerReplicationRequest> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (PriorityLevel      priorityLevel,
                           const std::string &id,
                           const std::string &database,
                           unsigned int       chunk);

    // Default construction and copy semantics are proxibited

    WorkerReplicationRequest () = delete;
    WorkerReplicationRequest (WorkerReplicationRequest const&) = delete;
    WorkerReplicationRequest & operator= (WorkerReplicationRequest const&) = delete;

    /// Destructor
    virtual ~WorkerReplicationRequest ();

    // Trivial accessors

    const std::string& database () const { return _database; }
    unsigned int       chunk    () const { return _chunk; }

    /**
     * This method implements the pure virtual method of the base class
     *
     * @see WorkerRequest::execute
     */
    bool execute (bool incremental=true) final;

private:

    /**
     * The normal constructor of the class.
     */
    WorkerReplicationRequest (PriorityLevel      priorityLevel,
                              const std::string &id,
                              const std::string &database,
                              unsigned int       chunk);
private:

    std::string  _database;
    unsigned int _chunk;

    /// The number of milliseconds since the beginning of the request processing
    unsigned int _durationMillisec;
};


}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERREPLICATIONREQUEST_H