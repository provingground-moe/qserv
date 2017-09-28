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
#ifndef LSST_QSERV_REPLICA_CORE_REPLICATE_JOB_H
#define LSST_QSERV_REPLICA_CORE_REPLICATE_JOB_H

/// ReplicateJob.h declares:
///
/// class ReplicateJob
/// (see individual class documentation for more information)

// System headers

#include <string>

// Qserv headers

#include "replica_core/Job.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class ReplicateJob represents a tool which will increase the minimum
  * number of each chunk's replica up to the requested level.
  */
class ReplicateJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ReplicateJob> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param numReplicas    - the minimum number of replicas for each chunk
     * @param database       - the name of a database
     * @param controller     - for launching requests
     * @param progressReport - triggers periodic printout into the log stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     */
    static pointer create (unsigned int               numReplicas,
                           std::string const&         database,
                           Controller::pointer const& controller,
                           bool                       progressReport=true,
                           bool                       errorReport=false);

    // Default construction and copy semantics are prohibited

    ReplicateJob () = delete;
    ReplicateJob (ReplicateJob const&) = delete;
    ReplicateJob& operator= (ReplicateJob const&) = delete;

    /// Destructor
    ~ReplicateJob () override;

    /// Return the minimum number of each chunk's replicas to be reached when
    /// the job successfully finishes.
    unsigned int numReplicas () const { return _numReplicas; }

    /// Return the name of a database defining a scope of the operation
    std::string const& database () const { _database; }

protected:

    /**
     * Construct the job with the pointer to the services provider.
     *
     * @param numReplicas    - the minimum number of replicas for each chunk
     * @param database       - the name of a database
     * @param controller     - for launching requests
     * @param progressReport - triggers periodic printout into the log stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     */
    ReplicateJob (unsigned int               numReplicas,
                  std::string const&         database,
                  Controller::pointer const& controller,
                  bool                       progressReport,
                  bool                       errorReport);

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
    
protected:

    /// The minimum number of replicas for each chunk
    unsigned int _numReplicas;

    /// The name of the database
    std::string _database;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_REPLICATE_JOB_H