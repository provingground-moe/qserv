/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICATIONTHREAD_H
#define LSST_QSERV_REPLICATIONTHREAD_H

// Qserv headers
#include "replica/ControlThread.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class 
 */
class ReplicationThread
    :   public ControlThread {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ReplicationThread> Ptr;

    // Default construction and copy semantics are prohibited

    ReplicationThread() = delete;
    ReplicationThread(ReplicationThread const&) = delete;
    ReplicationThread& operator=(ReplicationThread const&) = delete;

    ~ReplicationThread() final = default;

    /**
     * Create a new thread with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param controller
     *   a reference to the Controller for launching requests, jobs, etc.
     *
     * @param onTerminated
     *   callback function to be called upon abnormal termination
     *   of the thread. Set it to 'nullptr' if no call back should be made.
     *
     * @param replicationIntervalSec
     *   the number of seconds to wait in the end of each iteration loop before
     *   to begin the new one.
     *
     * @param numReplicas
     *   the desired number of replicas
     *
     * @param numIter
     *   the limit for the maximum number of iteration of the replication loop
     *
     * @param purge
     *   purge excess replicas if 'true'
     *
     * @return
     *   the smart pointer to a new object
     */
    static Ptr create(Controller::Ptr const& controller,
                      ControlThread::CallbackType const& onTerminated,
                      unsigned int qservSyncTimeoutSec,
                      unsigned int replicationIntervalSec,
                      unsigned int numReplicas,
                      unsigned int numIter,
                      bool purge);

protected:

    /**
     * @see ControlThread::run()
     */
    void run() final;

private:

    /**
     * The constructor is available to the class's factory method
     *
     * @see ReplicationThread::create()
     */
    ReplicationThread(Controller::Ptr const& controller,
                      CallbackType const& onTerminated,
                      unsigned int qservSyncTimeoutSec,
                      unsigned int replicationIntervalSec,
                      unsigned int numReplicas,
                      unsigned int numIter,
                      bool purge);

private:

    /// The maximum number of seconds to be waited before giving up
    /// on the Qserv synchronization requests.
    unsigned int _qservSyncTimeoutSec;

    /// The number of seconds to wait in the end of each iteration loop before
    /// to begin the new one.
    unsigned int _replicationIntervalSec;

    /// The desired number of replicas
    unsigned int _numReplicas;

    /// The limit for the maximum number of iteration of the replication loop
    unsigned int _numIter;

    /// Purge excess replicas if 'true'
    bool _purge;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICATIONTHREAD_H
