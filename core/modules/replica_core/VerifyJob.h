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
#ifndef LSST_QSERV_REPLICA_CORE_VERIFY_JOB_H
#define LSST_QSERV_REPLICA_CORE_VERIFY_JOB_H

/// VerifyJob.h declares:
///
/// class  VerifyJob
///
/// (see individual class documentation for more information)

// System headers

#include <functional>   // std::function
#include <string>

// Qserv headers

#include "replica_core/Job.h"
#include "replica_core/FindRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {


/**
 * Class ReplicaDiff represents a difference between two replica information
 * objects which are stored here.
 *
 * NOTE: a reason why a special class (versus an overloaded version of
 * operator==) is needed to differrenciate between replicas is due to
 * greater flexibily of the special class which not only encapsulates both
 * replicas witin a single object, but it also allows compare ovejcts
 * in a specific context of the replica verification job. Specific aspects
 * of the replica diffences could be also reported.
 */
class ReplicaDiff {
    
public:

    /// Default constructor create an object which exhibits "no difference"
    /// behavior.
    ReplicaDiff ();

    /**
     * The normal constructor
     *
     * @param replica1 - a reference to the the 'older' replica object
     * @param replica2 - a reference to the the 'newer' replica object
     */
    ReplicaDiff (ReplicaInfo const& replica1,
                 ReplicaInfo const& replica2);
    
    /// Copy constructor
    ReplicaDiff (ReplicaDiff const&);

    /// Assignment operator
    ReplicaDiff& operator= (ReplicaDiff const&);

    /// Destructor
    virtual ~ReplicaDiff () {}

    /// Return a reference to the the 'older' replica object
    ReplicaInfo const& replica1 () const { return _replica1; }

    /// Return a reference to the the 'newer' replica object
    ReplicaInfo const& replica2 () const { return _replica2; }

    /**
     * The comporision operator returns 'true' in case if there are diffences
     * between replicas. Specific aspects of the difference can be explored
     * by comparing the replica objects.
     */
    bool operator() () const { return _notEqual; }

    // Specific tests

    bool statusMismatch    () const { return _statusMismatch; }
    bool numFilesMismatch  () const { return _numFilesMismatch; }
    bool fileNamesMismatch () const { return _fileNamesMismatch; }
    bool fileSizeMismatch  () const { return _fileSizeMismatch; }
    bool fileCsMismatch    () const { return _fileCsMismatch; }
    bool fileMtimeMismatch () const { return _fileMtimeMismatch; }

private:

    ReplicaInfo _replica1; // older replia
    ReplicaInfo _replica2; // newer replica

    bool _notEqual;
    bool _statusMismatch;
    bool _numFilesMismatch;
    bool _fileNamesMismatch;
    bool _fileSizeMismatch;
    bool _fileCsMismatch;
    bool _fileMtimeMismatch;
};

/// Overloaded streaming operator for type ReplicaDiff
std::ostream& operator<< (std::ostream& os, ReplicaDiff const& ri);

/**
  * Class VerifyJob represents a tool which will find go over all replicas
  * of all chunks and databases on all worker nodes, check if replicas still
  * exist, then verify a status of each replica. The new status will be compared
  * against the one which exists in the database. This will include:
  *   - file sizes
  *   - modification timestamps of files
  *   - control/check sums of files constituiting the replicas
  * 
  * Any differences will get reported to a subscriber via a specific callback
  * function. The new status of a replica will be also recorded witin the database.
  */
class VerifyJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<VerifyJob> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer,ReplicaDiff const&)> callback_type_on_diff;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param controller  - for launching requests
     * @param onFinish    - a callback function to be called upon a completion of the job
     @ @param onReplicaDifference - a callback function to be called when two replicas won't match
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
    static pointer create (Controller::pointer const& controller,
                           callback_type              onFinish,
                           callback_type_on_diff      onReplicaDifference,
                           int                        priority    = 0,
                           bool                       exclusive   = false,
                           bool                       preemptable = true);

    // Default construction and copy semantics are prohibited

    VerifyJob () = delete;
    VerifyJob (VerifyJob const&) = delete;
    VerifyJob& operator= (VerifyJob const&) = delete;

    /// Destructor
    ~VerifyJob () override;

    /**
      * Implement the corresponding method of the base class.
      *
      * NOTE: this method has a dummy implementation which will just
      * block a caller before the job will be finished.
      *
      * @see Job::track()
      */
    void track (bool          progressReport,
                bool          errorReport,
                bool          chunkLocksReport,
                std::ostream& os) const override;

protected:

    /**
     * Construct the job with the pointer to the services provider.
     *
     * @see VerifyJob::create()
     */
    VerifyJob (Controller::pointer const& controller,
               callback_type              onFinish,
               callback_type_on_diff      onReplicaDifference,
               int                        priority,
               bool                       exclusive,
               bool                       preemptable);

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
     * The calback function to be invoked on a completion of each request.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish (FindRequest::pointer request);

    /**
     * Find the next replica to be inspected and return 'true' if the one
     * is found. Normally the method should never return 'false' unless
     * no single replica exists in the system or there was a failure to find
     * a replica info in the database.
     */
    bool nextReplica ();
    
protected:

    /// Client-defined function to be called upon the completion of the job
    callback_type _onFinish;

    /// Client-defined function to be called when two replicas won't match
    callback_type_on_diff _onReplicaDifference;

    // The current (last) replica which is inspected
    ReplicaInfo _replica;
 
    /// The current (last) request
    FindRequest::pointer _request;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_VERIFY_JOB_H