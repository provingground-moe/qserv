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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERREQUEST_H
#define LSST_QSERV_REPLICA_CORE_WORKERREQUEST_H

/// WorkerRequest.h declares:
///
/// class WorkerRequest
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
struct WorkerRequestCancelled
    :   std::exception {

    /// Return the short description of the exception
    virtual const char* what () const noexcept {
        return "cancelled";
    }
};

/**
  * Class WorkerRequest is the base class for a family of the worker-side
  * requsts which require non-deterministic interactions with the server's
  * environment (network, disk I/O, etc.). Generally speaking, all requests
  * which can't be implemented instanteneously fall into this category.
  */
class WorkerRequest
    :   public std::enable_shared_from_this<WorkerRequest> {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerRequest> pointer;

    /// Priority levels
    enum PriorityLevel {
        PRIORITY_LEVEL_LOW      = 0,
        PRIORITY_LEVEL_MEDIUM   = 1,
        PRIORITY_LEVEL_HIGH     = 2,
        PRIORITY_LEVEL_CRITICAL = 3
    };

    /// Return the string representation of the status
    static std::string priorityLevel2string (PriorityLevel status);    

    /// Completion status of the request processing operation
    enum CompletionStatus {
        STATUS_NONE,           /// no processing has been attempted
        STATUS_IN_PROGRESS,
        STATUS_IS_CANCELLING,
        STATUS_CANCELLED,
        STATUS_SUCCEEDED,
        STATUS_FAILED
    };

    /// Return the string representation of the status
    static std::string status2string (CompletionStatus status);

    // Default construction and copy semantics are proxibited

    WorkerRequest () = delete;
    WorkerRequest (WorkerRequest const&) = delete;
    WorkerRequest & operator= (WorkerRequest const&) = delete;

    /// Destructor
    virtual ~WorkerRequest ();

    // Trivial accessors

    const std::string& type          () const { return _type; }
    PriorityLevel      priorityLevel () const { return _priorityLevel; }
    const std::string& id            () const { return _id; }
    CompletionStatus   status        () const { return _status; }

    /**
     * This method is called to indicate a transtion of the request
     * into STATUS_IN_PROGRESS.
     *
     * HOW TO OVERRIDE THIS METHOD: if a subclass chooses to override this method
     * the subclass should call this (the base class's) method as well.
     */
    virtual void beginProgress ();

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
     * The method will throw custom exception WorkerRequestCancelled when
     * it detects a cancellation request.
     * 
     * @param incremental - setting it to 'false' will disable the incremental mode
     */
    virtual bool execute (bool incremental=true) = 0;

    /**
     * Cancel execution of the request.
     *
     * The effect of the operation varies depending on the current state of
     * the request. The default (the base class's implementation) assumes
     * the following transitions:
     * 
     *   STATUS_NONE or STATUS_CANCELLED             - transition to state STATUS_CANCELLED
     *   STATUS_IN_PROGRESS or STATUS_IS_CANCELLING  - transition to state STATUS_IS_CANCELLING
     *   other                                       - throwing std::logic_error

     */
    virtual void cancel ();

    /**
     * Roll back the request into its initial state and cleanup partial results
     * if possible.
     *
     * The effect of the operation varies depending on the current state of
     * the request. The default (the base class's implementation) assumes
     * the following transitions:
     * 
     *   STATUS_NONE or STATUS_IN_PROGRESS - transition to STATUS_NONE
     *   STATUS_IS_CANCELLING              - transition to STATUS_CANCELLED and throwing WorkerRequestCancelled
     *   other                             - throwing std::logic_error
     */
    virtual void rollback ();

    /// Return the context string
    std::string context () const {
        return id() + "  " + type() + "  " + status2string(status()) + "  ";
    }

protected:

    /**
     * The normal constructor of the class.
     */
    WorkerRequest (const std::string &type,
                   PriorityLevel      priorityLevel,
                   const std::string &id);

    /// Set the status
    void setStatus (CompletionStatus status);

private:

    std::string   _type;
    PriorityLevel _priorityLevel;
    std::string   _id;
    
    CompletionStatus _status;
};


/// Comparision type for strict weak ordering reaquired by std::priority_queue
struct WorkerRequestCompare {

    /// Order requests by their priorities
    bool operator() (const WorkerRequest::pointer& lhs,
                     const WorkerRequest::pointer& rhs) const {

        return lhs->priorityLevel() < rhs->priorityLevel();
    }
};


}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERREQUEST_H