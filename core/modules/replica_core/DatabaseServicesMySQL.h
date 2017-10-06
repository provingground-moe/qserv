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
#ifndef LSST_QSERV_REPLICA_CORE_DATABASE_SERVICES_MYSQL_H
#define LSST_QSERV_REPLICA_CORE_DATABASE_SERVICES_MYSQL_H

/// DatabaseServices.h declares:
///
/// class DatabaseServices
/// (see individual class documentation for more information)

// System headers

// Qserv headers

#include "replica_core/DatabaseMySQL.h"
#include "replica_core/DatabaseServices.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class DatabaseServicesMySQL is a MySQL_specific implementation of the database
  * services for replication entities: Controller, Job and Request.
  *
  * @see class DatabaseServices
  */
class DatabaseServicesMySQL
    :   public DatabaseServices {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DatabaseServicesMySQL> pointer;

    // Default construction and copy semantics are prohibited

    DatabaseServicesMySQL () = delete;
    DatabaseServicesMySQL (DatabaseServicesMySQL const&) = delete;
    DatabaseServicesMySQL& operator= (DatabaseServicesMySQL const&) = delete;

    /**
     * Construct the object.
     *
     * @param configuration - the configuration service
     */
    explicit DatabaseServicesMySQL (Configuration& configuration);

    /// Destructor
    virtual ~DatabaseServicesMySQL ();

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveControllerState()
     */
    void saveControllerState (ControllerIdentity const& identity,
                              uint64_t                  startTime) override;

    /**
     * Implement the corresponding method defined in the base class
     *
     * @see DatabaseServices::saveJobState()
     */
    void saveJobState (Job_pointer const& job) override;

protected:

    /// Databse connection
    database::mysql::Connection::pointer _connection;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_DATABASE_SERVICES_MYSQL_H