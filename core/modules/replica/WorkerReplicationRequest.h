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

#include <memory>       // shared_ptr, enable_shared_from_this
#include <string>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {


/// Forward declaration
class WorkerProcessorThread;

/**
  * Class WorkerReplicationRequest represents a context and a state of replication
  * requsts within the worker servers.
  */
class WorkerReplicationRequest
    : public std::enable_shared_from_this<WorkerReplicationRequest> {

public:

    typedef std::shared_ptr<WorkerReplicationRequest> pointer;
    typedef std::shared_ptr<WorkerProcessorThread>    WorkerProcessorThread_pointer;

    /// Priority levels
    enum Priority {
        LOW      = 0,
        MEDIUM   = 1,
        HIGH     = 2,
        CRITICAL = 3
    };
    
    /// Completion status of the request processing operation
    enum CompletionStatus {
        NONE,           /// no processing has been attempted
        SUCCEEDED,
        FAILED
    };
    
    /// Comparision operator is needed to store requests in the priority queue
    bool operator < (const WorkerReplicationRequest& rhs) const {
        return priority < rhs.priority;
    }
    
    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (Priority           priority,
                           const std::string& id,
                           const std::string& database,
                           unsigned int       chunk);

    // Default construction and copy semantics are proxibited

    WorkerReplicationRequest () = delete;
    WorkerReplicationRequest (WorkerReplicationRequest const&) = delete;
    WorkerReplicationRequest & operator= (WorkerReplicationRequest const&) = delete;

    /// Destructor
    virtual ~WorkerReplicationRequest ();

    // Trivial accessors

    Priority priority () const { return _priority; }

    const std::string& id       () const { return _id; }
    const std::string& database () const { return _database; }
    unsigned int       chunk    () const { return _chunk; }
    
    CompletionStatus status () const { return _status; }

    WorkerProcessorThread_pointer processorThread () const { return _processorThread; }

    /// Set the new completion status
    void setStatus (CompletionStatus newStatus);
 
    /// Set (or reset) the new thread
    void setProcessorThread (WorkerProcessorThread_pointer newProcessorThread = WorkerProcessorThread_pointer());

private:

    /**
     * The constructor of the class.
     */
    WorkerReplicationRequest (Priority           priority,
                              const std::string& id,
                              const std::string& database,
                              unsigned int       chunk);

private:

    Priority _priority;
        
    std::string  _id;
    std::string  _database;
    unsigned int _chunk;
    
    CompletionStatus _status;

    /// The processor thread (set only when the request is being processed)
    WorkerProcessorThread_pointer _processorThread;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERREPLICATIONREQUEST_H