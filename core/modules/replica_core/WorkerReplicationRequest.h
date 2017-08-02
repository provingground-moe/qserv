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

#include <exception>    // std::exception
#include <memory>       // shared_ptr, enable_shared_from_this
#include <string>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/// Exception thrown when a replication request is cancelled
struct WorkerReplicationCancelled
    :   std::exception {

    /// Return the short description of the exception
    virtual const char* what () const noexcept {
        return "cancelled";
    }
};

/**
  * Class WorkerReplicationRequest represents a context and a state of replication
  * requsts within the worker servers.
  */
class WorkerReplicationRequest
    : public std::enable_shared_from_this<WorkerReplicationRequest> {

public:

    typedef std::shared_ptr<WorkerReplicationRequest> pointer;

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
        IN_PROGRESS,
        IS_CANCELLING,
        CANCELLED,
        SUCCEEDED,
        FAILED
    };

    /// Return the string representation of the status
    static std::string status2string (CompletionStatus status);

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

    /**
     * This method is called to indicate a transtion of the request into
     * the IN_PROGRESS state.
     */
    void beginProgress ();

    /**
     * This method should be invoked (repeatedly) to execute the request until
     * it returns 'true' or throws an exception. Note that returning 'true'
     * may mean both success or failure, depeniding on the completion status
     * of the request.
     *
     * The default (and preferred) mode of operation (see parameter 'incremental') is
     * to let the method do its work in progressive steps returning 'false' after
     * each increment while the work is still being done. This prevents a calling thread
     * from being blocked for the whole duration of the request execution and be gracefully
     * stopped if needed.
     *
     * The method will throw custom exception WorkerReplicationCancelled when
     * it detects a cancellation request.
     * 
     * @param incremental - setting it to 'false' will disable the incremental mode
     */
    bool execute (bool incremental=true);

    /**
     * Cancel execution of the request.
     *
     * The effect of the operation varies depending on the current state of
     * the request:
     *   NONE or CANCELLED             - transition to state CANCELLED
     *   IN_PROGRESS or IS_CANCELLING  - transition to state IS_CANCELLING
     *   other                         - throwing std::logic_error

     */
    void cancel ();

    /**
     * Roll back the request into its initial state and cleanup partial results
     * if possible.
     *
     * The effect of the operation varies depending on the current state of
     * the request:
     *   NONE or IN_PROGRESS - transition to state NONE
     *   IS_CANCELLING       - transition to CANCELLED and throwing WorkerReplicationCancelled
     *   other               - throwing std::logic_error
     */
    void rollback ();

private:

    /**
     * The normal constructor of the class.
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

    /// The number of milliseconds since the beginning of the request processing
    unsigned int _durationMillisec;
};


/// Comparision type for strict weak ordering reaquired by std::priority_queue
struct WorkerReplicationRequestCompare {

    /// Order requests by their priorities
    bool operator() (const WorkerReplicationRequest::pointer& lhs,
                     const WorkerReplicationRequest::pointer& rhs) const {

        return lhs->priority() < rhs->priority();
    }
};


}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERREPLICATIONREQUEST_H