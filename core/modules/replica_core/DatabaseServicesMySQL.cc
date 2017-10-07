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

    _conn = database::mysql::Connection::open (params);
}

DatabaseServicesMySQL::~DatabaseServicesMySQL () {
}

void
DatabaseServicesMySQL::saveControllerState (ControllerIdentity const& identity,
                                            uint64_t                  startTime) {

    static std::string const context = "DatabaseServicesMySQL::saveControllerState  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK_GUARD;

    try {
        _conn->begin ();
        _conn->execute (
            _conn->sqlInsertQuery (
                "controller",
                identity.id,
                identity.host,
                identity.pid,
                startTime));
        _conn->commit ();

    } catch (database::mysql::DuplicateKeyError const&) {
        _conn->rollback ();
        throw std::logic_error (
                context + "the controller state is already "
                "in the database");
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void
DatabaseServicesMySQL::saveJobState (Job_pointer const& job) {

    static std::string const context = "DatabaseServicesMySQL::saveJobState  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK_GUARD;

    // The algorithm will first try the INSERT query. If a row with the same
    // primary key (Job id) already exists in the table then the UPDATE
    // query will be executed.
    
    try {
        std::string const jobInsertQuery =
            _conn->sqlInsertQuery (
                "job",
                job->id(),
                job->controller()->identity().id,
                job->type(),
                Job::state2string (job->state()),
                Job::state2string (job->extendedState()),
                                   job->beginTime(),
                                   job->endTime());
        _conn->begin   ();
        _conn->execute (jobInsertQuery);
        _conn->commit  ();

    } catch (database::mysql::DuplicateKeyError const&) {

        try {
            std::string const jobUpdateQuery =
                _conn->sqlSimpleUpdateQuery (
                    "job",
                    _conn->sqlEqual ("id",
                                     job->id()),
        
                    std::make_pair ("state",      Job::state2string (job->state())),
                    std::make_pair ("ext_state",  Job::state2string (job->extendedState())),
                    std::make_pair ("begin_time",                    job->beginTime()),
                    std::make_pair ("end_time",                      job->endTime()));

            _conn->rollback ();
            _conn->begin    ();
            _conn->execute  (jobUpdateQuery);
            _conn->commit   ();

        } catch (database::mysql::Error const& ex) {
            throw std::runtime_error (
                    context + "failed to save state into the database, exception: " + ex.what());
        }
    }

    // TODO: Add Job-specific query generation for the second set
    // of tables.

    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}



}}} // namespace lsst::qserv::replica_core