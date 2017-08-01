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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERPROCESSORTHREAD_H
#define LSST_QSERV_REPLICA_CORE_WORKERPROCESSORTHREAD_H

/// WorkerProcessorThread.h declares:
///
/// class WorkerProcessorThread
/// (see individual class documentation for more information)

// System headers

#include <memory>       // shared_ptr, enable_shared_from_this
#include <thread>

// Qserv headers

#include "replica_core/WorkerReplicationRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {


/// Forward declaration for the class
class WorkerProcessor;

/**
  * Class WorkerProcessorThread is a thread-based request processing engine
  * for replication requests.
  */
class WorkerProcessorThread
    : public std::enable_shared_from_this<WorkerProcessorThread> {

public:

    typedef std::shared_ptr<WorkerProcessorThread> pointer;
    typedef std::shared_ptr<WorkerProcessor>       WorkerProcessor_pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * 
     * @param processor - a pointer to the repository of requests to be processed
     */
    static pointer create (const WorkerProcessor_pointer &processor);

    // Default construction and copy semantics are proxibited

    WorkerProcessorThread () = delete;
    WorkerProcessorThread (WorkerProcessorThread const&) = delete;
    WorkerProcessorThread & operator= (WorkerProcessorThread const&) = delete;

    /// Destructor
    virtual ~WorkerProcessorThread ();

    /// Return an ientifier of this thread object
    unsigned int id () const { return _id; }

    /// Return true if the processing thread is still running
    bool isRunning () const;

    /**
     * Create and run the thread (if none is still running) fetching
     * and processing requests.
     */
    void run ();

    /**
     * Tell the running thread to finish proccessing the current
     * request (if any), stop fetching new requests and stop.
     *
     * NOTE: This is an asynchronous operation.
     */
    void stop ();

    /**
     * Tell the running thread to cancel proccessing the current
     * request (if any).
     * 
     * NOTE: This is an asynchronous operation.
     */
    void cancel ();

    /// Return the context string
    std::string context () const { return "THREAD [" + std::to_string(_id) + "]  "; }

private:

    /**
     * The constructor of the class.
     *
     * @param processor - a pointer to the repository of requests to be processed
     * @param id - a unique identifier of this object
     */
    explicit WorkerProcessorThread (const WorkerProcessor_pointer &processor,
                                    unsigned int                   id);

    /**
     * Event handler called by the thread when it's about to stop
     */
    void stopped ();

    /**
     * Event handler called by the thread when a request is cancelled
     */
    void cancelled (const WorkerReplicationRequest::pointer &request);
 
private:

    /// The processor
    WorkerProcessor_pointer _processor;

    /// The identifier of this thread object   
    unsigned int _id;

    /// The processing thread is created on demand when calling method run()
    std::shared_ptr<std::thread> _thread;
    
    /// The flag to be raised to tell the running thread to stop.
    /// The thread will reset this flag when it finishes.
    bool _stop;

    /// The flag to be raised to tell the running thread to cancel.
    /// The thread will reset this flag when it finishes.
    bool _cancel;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERPROCESSORTHREAD_H