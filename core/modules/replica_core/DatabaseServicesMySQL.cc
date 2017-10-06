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

// Class header

#include "replica_core/DatabaseServicesMySQL.h"

// System headers

#include <stdexcept>
#include <sstream>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/Configuration.h"
#include "replica_core/Controller.h"
#include "replica_core/Job.h"

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.DatabaseServicesMySQL");

} /// namespace


namespace lsst {
namespace qserv {
namespace replica_core {

DatabaseServicesMySQL::DatabaseServicesMySQL (Configuration& configuration)
    :   DatabaseServices (configuration) {

    // Pull database info from the configuration and prepare
    // the connection object.

    database::mysql::ConnectionParams params;

    params.host     = configuration.databaseHost     ();
    params.port     = configuration.databasePort     ();
    params.user     = configuration.databaseUser     ();
    params.password = configuration.databasePassword ();
    params.database = configuration.databaseName     ();

    _connection = database::mysql::Connection::open (params);
}

DatabaseServicesMySQL::~DatabaseServicesMySQL () {
}

void
DatabaseServicesMySQL::saveControllerState (ControllerIdentity const& identity,
                                            uint64_t                  startTime) {

    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServicesMySQL::saveControllerState");

    LOCK_GUARD;

    std::string const query =
        "INSERT INTO `controller` VALUES ("
        "'"   + _connection->escape (identity.id) +
        "','" + _connection->escape (identity.host) +
        "',"  +      std::to_string (identity.pid) +
        ","   +      std::to_string (startTime) +
        ")";

    _connection->begin   ();
    _connection->execute (query);
    _connection->commit  ();

    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServicesMySQL::saveControllerState  ** DONE **");
}

void
DatabaseServicesMySQL::saveJobState (Job_pointer const& job) {

    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServicesMySQL::saveJobState");

    LOCK_GUARD;

    // Prepare two SQL statements for any jobs

    std::string const jobTable = _connection->strId ("job");

    std::string insertQuery;
    {
        std::ostringstream ss;
        ss  << "INSERT INTO " << _connection->strId ("job") << " VALUES ( "
            << _connection->strVal                    (job->id())                        << ","
            << _connection->strVal                    (job->controller()->identity().id) << ","
            << _connection->strVal                    (job->type())                      << ","
            << _connection->strVal (Job::state2string (job->state()))                    << ","
            << _connection->strVal (Job::state2string (job->extendedState()))            << ","
            <<                                         job->beginTime()                  << ","
            <<                                         job->endTime()                    << " )";
        insertQuery = ss.str();
    }
    std::string updateQuery;
    {
        std::ostringstream ss;
        ss  << "UPDATE " << _connection->strId ("job")
            << " SET "   << _connection->strId ("state")      << " = " << _connection->strVal (Job::state2string (job->state()))         << ","
            <<              _connection->strId ("ext_state")  << " = " << _connection->strVal (Job::state2string (job->extendedState())) << ","
            <<              _connection->strId ("begin_time") << " = " <<                                         job->beginTime()       << ","
            <<              _connection->strId ("end_time")   << " = " <<                                         job->endTime()         
            << " WHERE " << _connection->strId ("id")         << " = " << _connection->strVal                    (job->id());
        updateQuery = ss.str();
    }
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServicesMySQL::saveJobState  insertQuery: " << insertQuery);
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServicesMySQL::saveJobState  updateQuery: " << updateQuery);
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServicesMySQL::saveJobState  ** DONE **");
}



}}} // namespace lsst::qserv::replica_core