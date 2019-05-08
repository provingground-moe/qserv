/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_HTTPPROCESSOR_H
#define LSST_QSERV_HTTPPROCESSOR_H

// System headers
#include <functional>
#include <memory>
#include <set>

// Qserv headers
#include "qhttp/Server.h"
#include "replica/DeleteWorkerTask.h"
#include "replica/HealthMonitorTask.h"
#include "replica/ReplicationTask.h"
#include "util/Mutex.h"

// LSST headers
#include "lsst/log/Log.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpProcessor processes requests from the built-in HTTP server.
 * The constructor of the class will register requests handlers an start
 * the server.
 */
class HttpProcessor : public std::enable_shared_from_this<HttpProcessor> {

public:

    typedef std::shared_ptr<HttpProcessor> Ptr;

    HttpProcessor() = delete;
    HttpProcessor(HttpProcessor const&) = delete;
    HttpProcessor& operator=(HttpProcessor const&) = delete;

    ~HttpProcessor();

    static Ptr create(Controller::Ptr const& controller,
                      HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                      unsigned int workerResponseTimeoutSec,
                      HealthMonitorTask::Ptr const& healthMonitorTask,
                      ReplicationTask::Ptr const& replicationTask,
                      DeleteWorkerTask::Ptr const& deleteWorkerTask);

    Controller::Ptr const controller() const { return _controller; }

private:

    HttpProcessor(Controller::Ptr const& controller,
                  HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                  unsigned int workerResponseTimeoutSec,
                  HealthMonitorTask::Ptr const& healthMonitorTask,
                  ReplicationTask::Ptr const& replicationTask,
                  DeleteWorkerTask::Ptr const& deleteWorkerTask);

    void _initialize();

    std::string _context() const;

    /**
     * Log a message into the Logger's LOG_LVL_INFO stream
     */
    void _info(std::string const& msg) const;
    void _info(std::string const& context, std::string const& msg) const { _info(context + "  " + msg); }

    /**
     * Log a message into the Logger's LOG_LVL_DEBUG stream
     */
    void _debug(std::string const& msg) const;
    void _debug(std::string const& context, std::string const& msg) const { _debug(context + "  " + msg); }

    /**
     * Log a message into the Logger's LOG_LVL_ERROR stream
     */
    void _error(std::string const& msg) const;
    void _error(std::string const& context, std::string const& msg) const { _error(context + "  " + msg); }

    /**
     * Process a request which return status of one worker.
     */
    void _getWorkerStatus(qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return the status of the replicas.
     */
    void _getReplicationLevel(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return status of all workers.
     */
    void _listWorkerStatuses(qhttp::Request::Ptr const& req,
                             qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return info on known Replication Controllers
     */
    void _listControllers(qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return info on the specified Replication Controller
     */
    void _getControllerInfo(qhttp::Request::Ptr const& req,
                            qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return info on known Replication Requests
     */
    void _listRequests(qhttp::Request::Ptr const& req,
                       qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return info on the specified Replication Request
     */
    void _getRequestInfo(qhttp::Request::Ptr const& req,
                         qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return info on known Replication Jobs
     */
    void _listJobs(qhttp::Request::Ptr const& req,
                   qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return info on the specified Replication Job
     */
    void _getJobInfo(qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp);

    /**
     * Process a request which return the Configuration of the Replication system
     */
    void _getConfig(qhttp::Request::Ptr const& req,
                    qhttp::Response::Ptr const& resp);

    /**
     * Process a request which updates the Configuration of the Replication system
     * and reports back its new state.
     */
    void _updateGeneralConfig(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp);

    /**
     * Process a request which updates parameters of an existing worker in the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _updateWorkerConfig(qhttp::Request::Ptr const& req,
                             qhttp::Response::Ptr const& resp);

    /**
     * Process a request which removes an existing worker from the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _deleteWorkerConfig(qhttp::Request::Ptr const& req,
                             qhttp::Response::Ptr const& resp);

    /**
     * Process a request which adds a new worker into the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _addWorkerConfig(qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    /**
     * Process a request which removes an existing database family from the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _deleteFamilyConfig(qhttp::Request::Ptr const& req,
                             qhttp::Response::Ptr const& resp);

    /**
     * Process a request which adds a new database family into the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _addFamilyConfig(qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    /**
     * Process a request which removes an existing database from the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _deleteDatabaseConfig(qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp);

    /**
     * Process a request which adds a new database into the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _addDatabaseConfig(qhttp::Request::Ptr const& req,
                            qhttp::Response::Ptr const& resp);

    /**
     * Process a request which removes an existing table from the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _deleteTableConfig(qhttp::Request::Ptr const& req,
                            qhttp::Response::Ptr const& resp);

    /**
     * Process a request which adds a new database table into the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _addTableConfig(qhttp::Request::Ptr const& req,
                         qhttp::Response::Ptr const& resp);

    /**
     * Process a request for executing a query against a worker database.
     * A result set of the query will be returned for those query types which
     * have the one upon a successful completion of a request.
     */
    void _sqlQuery(qhttp::Request::Ptr const& req,
                   qhttp::Response::Ptr const& resp);

    /**
     * Process a request for extracting various status info from Qserv workers
     * (all of them or a subset of those as per parameters of a request).
     */
    void _getQservManyWorkersStatus(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp);

    /**
     * Process a request for extracting various status info from one Qserv worker.
     */
    void _getQservWorkerStatus(qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp);

    /**
     * Process a request for extracting a status on user queries launched to Qserv
     */
    void _getQservManyUserQuery(qhttp::Request::Ptr const& req,
                                qhttp::Response::Ptr const& resp);

    /**
     * Process a request for extracting a status on a specific user query launched to Qserv
     */
    void _getQservUserQuery(qhttp::Request::Ptr const& req,
                            qhttp::Response::Ptr const& resp);

    /**
     * Get info on super-transactions
     */
    void _getTransactions(qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    /**
     * Get info on the current/latest super-transaction
     */
    void _getTransaction(qhttp::Request::Ptr const& req,
                         qhttp::Response::Ptr const& resp);

    /**
     * Crate and start a super-transaction
     */
    void _beginTransaction(qhttp::Request::Ptr const& req,
                           qhttp::Response::Ptr const& resp);

    /**
     * Commit or rollback a super-transaction
     */
    void _endTransaction(qhttp::Request::Ptr const& req,
                         qhttp::Response::Ptr const& resp);

    /**
     * Register a database for an ingest
     */
    void _addDatabase(qhttp::Request::Ptr const& req,
                      qhttp::Response::Ptr const& resp);

    /**
     * Publish a database whose data were ingested earlier
     */
    void _publishDatabase(qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    /**
     * Register a database table for an ingest
     */
    void _addTable(qhttp::Request::Ptr const& req,
                   qhttp::Response::Ptr const& resp);

    /**
     * Register (if it's not register yet) a chunk for ingest.
     * Return connection parameters to an end-point service where chunk
     * data will need to be ingested.
     */
    void _addChunk(qhttp::Request::Ptr const& req,
                   qhttp::Response::Ptr const& resp);

    /**
     * Find descriptions of queries
     *
     * @param workerInfo  worker info object to be inspected to extract identifier)s of queries
     * @return descriptions of the queries
     */
    nlohmann::json _getQueries(nlohmann::json& workerInfo) const;

    Controller::Ptr const _controller;

    HealthMonitorTask::WorkerEvictCallbackType const _onWorkerEvict;

    unsigned int const _workerResponseTimeoutSec;

                      // References(!) to smart pointers to the tasks which can be managed
    // by this class. References to the pointers are used to avoid increasing
    // the reference counters to the pointed objects.

    HealthMonitorTask::Ptr const& _healthMonitorTask;

    std::string _replicationLevelReport; /// The cached state of the last replication levels report
    
    uint64_t _replicationLevelReportTimeMs = 0; /// The time of the last cached report

    util::Mutex _replicationLevelMtx; /// Protects the replication level cache

    util::Mutex _ingestManagementMtx;  /// Synchronized access to the Ingest management operations

    LOG_LOGGER _log;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPPROCESSOR_H
