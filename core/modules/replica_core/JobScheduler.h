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
#ifndef LSST_QSERV_REPLICA_CORE_JOB_SCHEDULER_H
#define LSST_QSERV_REPLICA_CORE_JOB_SCHEDULER_H

/// JobScheduler.h declares:
///
/// class JobScheduler
/// (see individual class documentation for more information)

// System headers

#include <algorithm>
#include <atomic>
#include <list>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <mutex>        // std::mutex, std::lock_guard
#include <queue>
#include <thread>
#include <vector>

// Qserv headers

#include "replica_core/Controller.h"
#include "replica_core/Job.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

class FindAllJob;
class PurgeJob;
class ReplicateJob;
class ServiceProvider;

/**
 * Base class ExclusiveMultiMasterLockI is an abstraction for operations with
 * the distributed multi-master lock.
 */
class ExclusiveMultiMasterLockI {

public:

    // Default construction and copy semantics are prohibited

    ExclusiveMultiMasterLockI () = delete;
    ExclusiveMultiMasterLockI (ExclusiveMultiMasterLockI const&) = delete;
    ExclusiveMultiMasterLockI &operator= (ExclusiveMultiMasterLockI const&) = delete;

    /// Destructor
    virtual ~ExclusiveMultiMasterLockI () {}

    /**
     * Request the lock and block on it until it's obtained
     */
    virtual void request ()=0;

    /**
     * Release the previously request lock.
     *
     * @throws std::logic_error - if no locking attempt was previously made
     */
    virtual void release ()=0;

    /**
     * Ensure the connection is still alive (and the previously requested
     * lock is still being held on behalf of the current session.)
     *
     * @throw std::runtime_error - if the connection was lost and no exclusive lock
     *                             is available for the calling context.
     */
    virtual void test ()=0;

protected:
    
    /**
     * Normal constructor
     * 
     * @param serviceProvider - for configuration, other services
     * @param controllerId    - a unique identifier of a Controller
     */
    ExclusiveMultiMasterLockI (ServiceProvider&   serviceProvider,
                               std::string const& controllerId)
        :   _serviceProvider (serviceProvider),
            _controllerId    (controllerId) {
    }

protected:

    // Prameters of the object
    
    ServiceProvider& _serviceProvider;
    std::string      _controllerId;
};

/**
  * Class JobScheduler is a front-end interface for processing
  * jobs fro connected clients.
  */
class JobScheduler
    :   public std::enable_shared_from_this<JobScheduler> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<JobScheduler> pointer;

    // Forward declarations for class-specific pointers

    typedef std::shared_ptr<FindAllJob>   FindAllJob_pointer;
    typedef std::shared_ptr<PurgeJob>     PurgeJob_pointer;
    typedef std::shared_ptr<ReplicateJob> ReplicateJob_pointer;

    typedef std::function<void(FindAllJob_pointer)>   FindAllJob_callback_type;
    typedef std::function<void(PurgeJob_pointer)>     PurgeJob_callback_type;
    typedef std::function<void(ReplicateJob_pointer)> ReplicateJob_callback_type;

    /// The priority queue for pointers to the new (unprocessed) jobs.
    /// Using inheritance to get access to the protected data members 'c'
    /// representing the internal container.
    struct PriorityQueueType
        :   std::priority_queue<Job::pointer,
                                std::vector<Job::pointer>,
                                JobCompare> {

        /// The beginning of the container to allow the iterator protocol
        decltype(c.begin()) begin () {
            return c.begin();
        }

        /// The end of the container to allow the iterator protocol
        decltype(c.end()) end () {
            return c.end();
        }

        /// Remove an entry from the queue by its identifier
        bool remove (std::string const& id) {
            auto itr = std::find_if (
                c.begin(),
                c.end(),
                [&id] (Job::pointer const& ptr) {
                    return ptr->id() == id;
                }
            );
            if (itr != c.end()) {
                c.erase(itr);
                std::make_heap(c.begin(), c.end(), comp);
                return true;
            }
            return false;
        }
    };

    /// Ordinary collection of pointers for jobs in other (than new/unprocessed)
    /// states
    typedef std::list<Job::pointer> CollectionType;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, other services
     * @param exclusive       - if set 'true' then the Scheduler at its start ime
     *                          (@see method JobScheduler::start()) will always
     *                          acquire an exclusive lock to guarantee that only
     *                          one instance of the Schedule runs at a time.
     *                          This mode should be used in the fault-tolerant
     *                          setups when multiple instances of the Schedulers
     *                          might be launched.
     */
    static pointer create (ServiceProvider &serviceProvider,
                           bool             exclusive=false);

    // Default construction and copy semantics are prohibited

    JobScheduler () = delete;
    JobScheduler (JobScheduler const&) = delete;
    JobScheduler &operator= (JobScheduler const&) = delete;

    /// Destructor
    virtual ~JobScheduler ();

    /**
     * Run the scheduler in a dedicated thread unless it's already running.
     * It's safe to call this method multiple times from any thread.
     */
    void run ();

    /**
     * Check if the service is running.
     *
     * @return true if the scheduler is running.
     */
    bool isRunning () const;

    /**
     * Stop the scheduler. This method will guarantee that all outstanding
     * opeations will finish and not aborted.
     *
     * This operation will also result in stopping the internal thread
     * in which the scheduler is being run.
     */
    void stop ();

    /**
     * Join with a thread in which the scheduler is being run (if any).
     * If the scheduler was not started or if it's stopped the the method
     * will return immediattely.
     *
     * This method is meant to be used for integration of the scheduler into
     * a larger multi-threaded application which may require a proper
     * synchronization between threads.
     */
    void join ();
    
    /**
     * Submit a job for finding all replicas and updating replica status
     * in the database.
     *
     * @param numReplicas - the maximum number of replicas allowed for each chunk
     * @param database    - the name of a database
     * @param onFinish    - a callback function to be called upon a completion of the job
     * @param priority    - set the desired job priority (larger values
     *                      mean higher priorities). A job with the highest
     *                      priority will be select from an input queue by
     *                      the JobScheduler.
     * @param exclusive   - set to 'true' to indicate that the job can't be
     *                      running simultaneously alongside other jobs.
     * @param preemptable - set to 'true' to indicate that this job can be
     *                      interrupted to give a way to some other job of
     *                      high importancy.
     */
    FindAllJob_pointer findAll (std::string const&       database,
                                FindAllJob_callback_type onFinish    = nullptr,
                                int                      priority    = 0,
                                bool                     exclusive   = false,
                                bool                     preemptable = true);

    /**
     * Submit a job for bringing the number of each chunk's replicas down
     * to a desired level.
     * 
     * @param numReplicas - the maximum number of replicas allowed for each chunk
     * @param database    - the name of a database
     * @param onFinish    - a callback function to be called upon a completion of the job
     * @param priority    - set the desired job priority (larger values
     *                      mean higher priorities). A job with the highest
     *                      priority will be select from an input queue by
     *                      the JobScheduler.
     * @param exclusive   - set to 'true' to indicate that the job can't be
     *                      running simultaneously alongside other jobs.
     * @param preemptable - set to 'true' to indicate that this job can be
     *                      interrupted to give a way to some other job of
     *                      high importancy.
     */
    PurgeJob_pointer purge (unsigned int           numReplicas,
                            std::string const&     database,
                            PurgeJob_callback_type onFinish    = nullptr,
                            int                    priority    = -1,
                            bool                   exclusive   = false,
                            bool                   preemptable = true);

    /**
     * Submit a job for bringing the number of each chunk's replicas up
     * to a desired level.
     * 
     * @param numReplicas - the minimum number of replicas required for each chunk
     * @param database    - the name of a database
     * @param onFinish    - a callback function to be called upon a completion of the job
     * @param priority    - set the desired job priority (larger values
     *                      mean higher priorities). A job with the highest
     *                      priority will be select from an input queue by
     *                      the JobScheduler.
     * @param exclusive   - set to 'true' to indicate that the job can't be
     *                      running simultaneously alongside other jobs.
     * @param preemptable - set to 'true' to indicate that this job can be
     *                      interrupted to give a way to some other job of
     *                      high importancy.
     */
    ReplicateJob_pointer replicate (unsigned int               numReplicas,
                                    std::string const&         database,
                                    ReplicateJob_callback_type onFinish    = nullptr,
                                    int                        priority    = 1,
                                    bool                       exclusive   = true,
                                    bool                       preemptable = true);


    // TODO: add job inspection methods

private:   

    /**
     * The constructor of the class.
     *
     * @see JobScheduler::create()
     */
    JobScheduler (ServiceProvider& serviceProvider,
                  bool             exclusive);

    /**
     * Check is there are any jobs in the input queue which are eligible
     * to be run immediatelly based on their scheduling attributes, such
     * as: 'priority', 'exclusive' or 'preemptable' modes. If so then launch
     * them.
     */
    void runQueued ();

    /**
     * Check is there are any time-based jobs which are supposed to run on
     * the periodic basis. If so then launch them.
     *
     * The jobs of this type will be pulled from the database each time
     * the metghod is called. If there are the ones which are ready to run
     * the jobs will be put into the input queue and the previously
     * defined method JobScheduler::runQueuedJobs() will be invoked.
     */
    void runScheduled ();

    /**
     * Stop all in-progress jobs and do *NOT* start the new ones.
     */
    void cancelAll ();

    /**
     * The callback method to be called upon a completion of a job.
     * This may also invoke method JobScheduler::runQueuedJobs()
     *
     * @param job - a reference to the job
     */
    void onFinish (Job::pointer const& job);

    /**
     * Request an exclusive distribured lock in the multi-master environment
     * if the corresponding option ws passed into the constructor of the class.
     */
    void requestMultiMasterLock ();

    /**
     * Release the previously obtained exclusive distribured lock in the multi-master
     * environment if the corresponding option ws passed into the constructor of
     * the class.
     */
    void releaseMultiMasterLock ();

   /**
     * Make sure the poreviously requested exclusive distribured lock which
     * is needed to run run this Scheduler in the multi-master environment is
     * still available.
     *
     * @throw std::runtime_error - if the connection was lost and no exclusive lock
     *                             is available for the calling context.
     */
    void testMultiMasterLock ();

private:

    /// Services used by the processor
    ServiceProvider& _serviceProvider;

    /// The multi-master synchronization option
    bool _exclusive;

    /// A dediated instance of the Controller for executing requests
    Controller::pointer _controller;

    std::unique_ptr<ExclusiveMultiMasterLockI> _multiMasterLock;

    /// This thread will run the asynchronous prosessing of the jobs
    std::unique_ptr<std::thread> _thread;

    /// Mutex guarding the queues
    mutable std::mutex _mtx;

    /// The flag to be raised to tell the running thread to stop.
    /// The thread will reset this flag when it finishes.
    std::atomic<bool> _stop;
 
    /// New unprocessed jobs
    PriorityQueueType _newJobs;

    /// Jobs which are being processed
    CollectionType _inProgressJobs;

    /// Completed (succeeded or otherwise) jobs
    CollectionType _finishedJobs;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_JOB_SCHEDULER_H