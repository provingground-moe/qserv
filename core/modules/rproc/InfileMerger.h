// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_RPROC_INFILEMERGER_H
#define LSST_QSERV_RPROC_INFILEMERGER_H
/// InfileMerger.h declares:
///
/// struct InfileMergerError
/// class InfileMergerConfig
/// class InfileMerger
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <mutex>
#include <set>
#include <string>

// Qserv headers
#include "mysql/LocalInfile.h"
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"
#include "sql/SqlConnection.h"
#include "util/Error.h"
#include "util/EventThread.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace mysql {
    class MySqlConfig;
}
namespace proto {
    class ProtoHeader;
    class Result;
    struct WorkerResponse;
}
namespace qdisp {
    class MessageStore;
}
namespace query {
    class ColumnRef;
    class SelectStmt;
}
namespace sql {
    class Schema;
    class SqlConnection;
    class SqlResults;
}
}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace rproc {

/** \typedef InfileMergerError Store InfileMerger error code.
 *
 * \note:
 * Keep this indirection to util::Error in case
 * InfileMergerError::resultTooBig() method might is needed in the future
 *
 * */
typedef util::Error InfileMergerError;

/// class InfileMergerConfig - value class for configuring a InfileMerger
class InfileMergerConfig {
public:
    InfileMergerConfig() {}
    InfileMergerConfig(mysql::MySqlConfig const& mySqlConfig)
        :  mySqlConfig(mySqlConfig)
    {
    }
    // for final result, and imported result
    mysql::MySqlConfig const mySqlConfig;
    std::string targetTable;
    std::shared_ptr<query::SelectStmt> mergeStmt;
};


/// This class is used to remove invalid rows from cancelled job attempts.
/// Removing the invalid rows from the result table can be very expensive,
/// so steps are taken to only do it when rows are known to exist in the
/// result table.
///
/// The rows can only be safely deleted from the result table when
/// nothing is writing to the table. To minimize the time locking the mutex
/// and allow multiple entities to write to the table concurrently, the
/// number of task writing to the table is tracked with _concurrentMergeCount.
/// Deletes are only to be allowed when _concurrentMergeCount is 0.
class InvalidJobAttemptMgr {
public:
    using jASetType = std::set<int>;
    using deleteFuncType = std::function<bool(jASetType const&)>;

    InvalidJobAttemptMgr() {}
    void setDeleteFunc(deleteFuncType func) {_deleteFunc = func; }

    /// @return true if jobIdAttempt is invalid.
    /// Wait if rows need to be deleted.
    /// Then, add job-attempt to _jobIdAttemptsHaveRows and increment
    /// _concurrentMergeCount to keep rows from being deleted before
    /// decrConcurrentMergeCount is called.
    bool incrConcurrentMergeCount(int jobIdAttempt);
    void decrConcurrentMergeCount();


    /// @return true if query results are valid. If it returns false, the query results are invalid.
    /// This function will stop all merging to the result table and delete all invalid
    /// rows in the table. If it returns false, invalid rows remain in the result table,
    /// and the query should probably be cancelled.
    bool holdMergingForRowDelete(std::string const& msg="");

    /// @return true if jobIdAttempt is in the invalid set.
    bool isJobAttemptInvalid(int jobIdAttempt);

    bool prepScrub(int jobIdAttempt);

private:
    /// Precondition: must hold _iJAMtx before calling.
    /// @return true if jobIdAttempt is in the invalid set.
    bool _isJobAttemptInvalid(int jobIdAttempt);
    void _cleanupIJA(); ///< Helper to send notice to all waiting on _cv.

    std::mutex _iJAMtx;
    jASetType _invalidJobAttempts; ///< Set of job-attempts that failed.
    jASetType _invalidJAWithRows;  ///< Set of job-attempts that failed and have rows in result table.
    jASetType _jobIdAttemptsHaveRows; ///< Set of job-attempts that have rows in result table.
    int _concurrentMergeCount{0};
    bool _waitFlag{false};
    std::condition_variable  _cv;
    deleteFuncType _deleteFunc;
};

/// InfileMerger is a row-based merger that imports rows from result messages
/// and inserts them into a MySQL table, as specified during construction by
/// InfileMergerConfig.
///
/// To use, construct a configured instance, then call merge() to kick off the
/// merging process, and finalize() to wait for outstanding merging processes
/// and perform the appropriate post-processing before returning.  merge() right
/// now expects an entire message buffer, where a message buffer consists of:
/// Byte 0: unsigned char size of ProtoHeader message
/// Bytes 1 - size_ph : ProtoHeader message (containing size of result message)
/// Bytes size_ph - size_ph + size_rm : Result message
/// At present, Result messages are not chained.
class InfileMerger {
public:
    explicit InfileMerger(InfileMergerConfig const& c);
    ~InfileMerger();

    /// Create the shared thread pool and/or change its size.
    // @return the size of the large result thread pool.
    static int setLargeResultPoolSize(int size);

    /// Merge a worker response, which contains:
    /// Size of ProtoHeader message
    /// ProtoHeader message
    /// Result message
    /// @return true if merge was successfully imported (queued)
    bool merge(std::shared_ptr<proto::WorkerResponse> response);

    /// @return error details if finalize() returns false
    InfileMergerError const& getError() const { return _error; }
    /// @return final target table name  storing results after post processing
    std::string getTargetTable() const {return _config.targetTable; }
    /// Finalize a "merge" and perform postprocessing
    bool finalize();
    /// Check if the object has completed all processing.
    bool isFinished() const;

    bool prepScrub(int jobId, int attempt);
    bool scrubResults(int jobId, int attempt);
    int makeJobIdAttempt(int jobId, int attemptCount);

    /// Make a schema that matches the results of the given query.
    sql::Schema getSchemaForQueryResults(query::SelectStmt const& stmt, std::string& errMsg);

    /// Make the results table for the given query.
    bool makeResultsTableForQuery(query::SelectStmt const& stmt, std::string& errMsg);

private:
    bool _applyMysql(std::string const& query);
    bool _merge(std::shared_ptr<proto::WorkerResponse>& response);
    int _readHeader(proto::ProtoHeader& header, char const* buffer, int length);
    int _readResult(proto::Result& result, char const* buffer, int length);
    bool _verifySession(int sessionId);
    void _setupRow();
    bool _applySql(std::string const& sql);
    bool _applySqlLocal(std::string const& sql, std::string const& logMsg, sql::SqlResults& results);
    bool _applySqlLocal(std::string const& sql, std::string const& logMsg);
    bool _applySqlLocal(std::string const& sql, sql::SqlResults& results);
    bool _applySqlLocal(std::string const& sql, sql::SqlResults& results, sql::SqlErrorObject& errObj);
    bool _sqlConnect(sql::SqlErrorObject& errObj);
    std::string _getQueryIdStr();
    void _setQueryIdStr(std::string const& qIdStr);
    void _fixupTargetName();

    bool _setupConnection() {
        if (_mysqlConn.connect()) {
            _infileMgr.attach(_mysqlConn.getMySql());
            return true;
        }
        return false;
    }

    InfileMergerConfig _config; ///< Configuration
    std::shared_ptr<sql::SqlConnection> _sqlConn; ///< SQL connection
    std::string _mergeTable; ///< Table for result loading
    InfileMergerError _error; ///< Error state
    bool _isFinished{false}; ///< Completed?
    std::mutex _sqlMutex; ///< Protection for SQL connection
    size_t _getResultTableSizeMB(); ///< Return the size of the result table in MB.

    /**
     * @brief Put a "jobId" column first in the provided schema.
     *
     * The jobId column is used to keep track of what job number and attempt number each row in the results
     * table came from.
     *
     * The schema must match the schema of the results returned by workers (and workers add the JobId column
     * first in the schema).
     *
     * @note This will change _jobIdColName if it conflicts with a column name in the user query.
     *
     * @param schema The schema to be modified.
     */
    void _addJobIdColumnToSchema(sql::Schema& schema);

    mysql::MySqlConnection _mysqlConn;

    std::mutex _mysqlMutex;
    lsst::qserv::mysql::LocalInfile::Mgr _infileMgr;

    std::mutex _queryIdStrMtx; ///< protects _queryIdStr
    std::atomic<bool> _queryIdStrSet{false};
    std::string _queryIdStr{"QI=?"}; ///< Unknown until results start coming back from workers.

    std::string _jobIdColName; ///< Name of the jobId column in the result table.
    int const _jobIdMysqlType{MYSQL_TYPE_LONG}; ///< 4 byte integer.
    std::string const _jobIdSqlType{"INT(9)"}; ///< The 9 only affects '0' padding with ZEROFILL.

    InvalidJobAttemptMgr _invalidJobAttemptMgr;
    bool _deleteInvalidRows(std::set<int> const& jobIdAttempts);


    int _sizeCheckRowCount{0}; ///< Number of rows read since last size check.
    int _checkSizeEveryXRows{1000}; ///< Check the size of the result table after every x number of rows.
    size_t _maxResultTableSizeMB{5000}; ///< Max result table size.
};

}}} // namespace lsst::qserv::rproc

#endif // LSST_QSERV_RPROC_INFILEMERGER_H
