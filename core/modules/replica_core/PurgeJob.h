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
#ifndef LSST_QSERV_REPLICA_CORE_PURGE_JOB_H
#define LSST_QSERV_REPLICA_CORE_PURGE_JOB_H

/// PurgeJob.h declares:
///
/// struct PurgeJobResult
/// class  PurgeJob
///
/// (see individual class documentation for more information)

// System headers

#include <atomic>
#include <functional>   // std::function
#include <list>
#include <map>
#include <string>

// Qserv headers

#include "replica_core/Job.h"
#include "replica_core/FindAllJob.h"
#include "replica_core/ReplicaInfo.h"
#include "replica_core/DeleteRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
 * The structure PurgeJobResult represents a combined result received
 * from worker services upon a completion of the job.
 */
struct PurgeJobResult {

    /// Results reported by workers upon the successfull completion
    /// of the corresponidng requests
    std::list<ReplicaDeleteInfo> replicas;

    /// Per-worker flags indicating if the corresponidng replica retreival
    /// request succeeded.
    std::map<std::string, bool> workers;
};

/**
  * Class PurgeJob represents a tool which will increase the minimum
  * number of each chunk's replica up to the requested level.
  */
class PurgeJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<PurgeJob> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param numReplicas - the maximum number of replicas allowed for each chunk
     * @param database    - the name of a database
     * @param controller  - for launching requests
     * @param onFinish    - a callback function to be called upon a completion of the job
     * @param bestEffort  - the flag (if set) allowing to proceed with the replication effort
     *                      when some workers fail to report their cunk disposition.
     *                      ATTENTION: do *NOT* use this in production!
     */
    static pointer create (unsigned int               numReplicas,
                           std::string const&         database,
                           Controller::pointer const& controller,
                           callback_type              onFinish,
                           bool                       bestEffort=false);

    // Default construction and copy semantics are prohibited

    PurgeJob () = delete;
    PurgeJob (PurgeJob const&) = delete;
    PurgeJob& operator= (PurgeJob const&) = delete;

    /// Destructor
    ~PurgeJob () override;

    /// Return the maximum number of each chunk's replicas to be reached when
    /// the job successfully finishes.
    unsigned int numReplicas () const { return _numReplicas; }

    /// Return the name of a database defining a scope of the operation
    std::string const& database () const { return _database; }

    /**
     * Return the result of the operation.
     *
     * IMPORTANT NOTES:
     * - the method should be invoked only after the job has finished (primary
     *   status is set to Job::Status::FINISHED). Otherwise exception
     *   std::logic_error will be thrown
     * 
     * - the result will be extracted from requests which have successfully
     *   finished. Please, verify the primary and extended status of the object
     *   to ensure that all requests have finished.
     *
     * @return the data structure to be filled upon the completin of the job.
     *
     * @throws std::logic_error - if the job dodn't finished at a time
     *                            when the method was called
     */
    PurgeJobResult const& getReplicaData () const;

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::track()
      */
    void track (bool          progressReport,
                bool          errorReport,
                std::ostream& os) const override;

protected:

    /**
     * Construct the job with the pointer to the services provider.
     *
     * @param numReplicas - the minimum number of replicas for each chunk
     * @param database    - the name of a database
     * @param controller  - for launching requests
     * @param onFinish    - a callback function to be called upon a completion of the job
     * @param bestEffort  - the flag (if set) allowing to proceed with the replication effort
     *                      when some workers fail to report their cunk disposition.
     *                      ATTENTION: do *NOT* use this in production!
     */
    PurgeJob (unsigned int               numReplicas,
              std::string const&         database,
              Controller::pointer const& controller,
              callback_type              onFinish,
              bool                       bestEffort);

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::startImpl()
      */
    void startImpl () override;

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::startImpl()
      */
    void cancelImpl () override;

    /**
      * Implement the corresponding method of the base class.
      *
      * @see Job::notify()
      */
    void notify () override;

    /**
     * The calback function to be invoked on a completion of the precursor job
     * which harvests chunk disposition accross relevant worker nodes.
     */
    void onPrecursorJobFinish ();

    /**
     * The calback function to be invoked on a completion of each request.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish (DeleteRequest::pointer request);

protected:

    /// The minimum number of replicas for each chunk
    unsigned int _numReplicas;

    /// The name of the database
    std::string _database;

    /// Client-defined function to be called upon the completion of the job
    callback_type _onFinish;

    /// The flag (if set) allowing to proceed with the effort even after
    /// not getting response on chunk disposition from all workers.
    bool _bestEffort;

    /// The chained job to be completed first in order to figure out
    /// replica disposition.
    FindAllJob::pointer _findAllJob;

    /// A collection of requests implementing the operation
    std::list<DeleteRequest::pointer> _requests;

    // The counter of requests which will be updated. They need to be atomic
    // to avoid race condition between the onFinish() callbacks executed within
    // the Controller's thread and this thread.

    std::atomic<size_t> _numLaunched;   ///< the total number of requests launched
    std::atomic<size_t> _numFinished;   ///< the total number of finished requests
    std::atomic<size_t> _numSuccess;    ///< the number of successfully completed requests

    /// The result of the operation (gets updated as requests are finishing)
    PurgeJobResult _replicaData;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_PURGE_JOB_H