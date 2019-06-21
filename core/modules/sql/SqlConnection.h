// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
// SqlConnection.h - The SqlConnection class provides a convenience
// layer on top of an underlying mysqlclient. Historically, the
// SqlConnection class abstracted every interaction with the database
// and provided some convenience functions (e.g., show tables, show
// databases) that went beyond providing a C++ wrapper to mysql. Some
// of the more raw mysql code has been moved to MySqlConnection, but
// not all.
// It is uncertain of how this usage conflicts with db usage via the
// python MySQLdb api, but no problems have been detected so far.

#ifndef LSST_QSERV_SQL_SQLCONNECTION_H
#define LSST_QSERV_SQL_SQLCONNECTION_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Local headers
#include "global/stringTypes.h"
#include "mysql/MySqlConfig.h"
#include "sql/SqlErrorObject.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace mysql {
    class MySqlConnection;
}
namespace sql {
    class SqlResults;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace sql {

class SqlResultIter {
public:
    SqlResultIter() : _columnCount(0) {}
    SqlResultIter(mysql::MySqlConfig const& sc, std::string const& query);
    virtual ~SqlResultIter() {}
    virtual SqlErrorObject& getErrorObject() { return _errObj; }

    virtual StringVector const& operator*() const { return _current; }
    virtual SqlResultIter& operator++(); // pre-increment iterator advance.
    virtual bool done() const; // Would like to relax LSST standard 3-4 for iterator classes

private:
    bool _setup(mysql::MySqlConfig const& sqlConfig, std::string const& query);

    std::shared_ptr<mysql::MySqlConnection> _connection;
    StringVector _current;
    SqlErrorObject _errObj;
    int _columnCount;
};

/// class SqlConnection : Class for interacting with a MySQL database.
class SqlConnection {
public:

    // Make a new SqlConnection object based on the passed-in config.
    static std::shared_ptr<SqlConnection> create(mysql::MySqlConfig const& mysqlConfig);

    SqlConnection();
    SqlConnection(mysql::MySqlConfig const& sc, bool useThreadMgmt=false);
    virtual ~SqlConnection();
    virtual void reset(mysql::MySqlConfig const& sc, bool useThreadMgmt=false);
    virtual bool connectToDb(SqlErrorObject&);
    virtual bool selectDb(std::string const& dbName, SqlErrorObject&);
    virtual bool runQuery(char const* query, int qSize,
                          SqlResults& results, SqlErrorObject&);
    virtual bool runQuery(char const* query, int qSize, SqlErrorObject&);
    virtual bool runQuery(std::string const query, SqlResults&,
                          SqlErrorObject&);
    /// with runQueryIter SqlConnection is busy until SqlResultIter is closed
    virtual std::shared_ptr<SqlResultIter> getQueryIter(std::string const& query);
    virtual bool runQuery(std::string const query, SqlErrorObject&);
    virtual bool dbExists(std::string const& dbName, SqlErrorObject&);
    virtual bool createDb(std::string const& dbName, SqlErrorObject&,
                          bool failIfExists=true);
    virtual bool createDbAndSelect(std::string const& dbName,
                                   SqlErrorObject&,
                                   bool failIfExists=true);
    virtual bool dropDb(std::string const& dbName, SqlErrorObject&,
                        bool failIfDoesNotExist=true);
    virtual bool tableExists(std::string const& tableName,
                             SqlErrorObject&,
                             std::string const& dbName="");
    virtual bool dropTable(std::string const& tableName,
                           SqlErrorObject&,
                           bool failIfDoesNotExist=true,
                           std::string const& dbName="");
    virtual bool listTables(std::vector<std::string>&,
                            SqlErrorObject&,
                            std::string const& prefixed="",
                            std::string const& dbName="");
    virtual void listColumns(std::vector<std::string>&,
                            SqlErrorObject&,
                            std::string const& dbName,
                            std::string const& tableName);

    virtual std::string getActiveDbName() const;

    /**
     *  Returns the value generated for an AUTO_INCREMENT column
     *  by the previous INSERT or UPDATE statement.
     */
    unsigned long long getInsertId() const;

    /**
     *  Escape string for use inside SQL statements.
     *  @return an escaped string, or an empty string if the connection can not be established
     *  @note the connection MUST be connected before using this method
     */
    std::string escapeString(std::string const& rawString) const;

    /**
     * Escape string for use inside SQL statements.
     * @return true if the escaped string could be created.
     * @note this method will attempt to connect if the connection is not already estabilshed.
     */
    bool escapeString(std::string const& rawString, std::string& escapedString,
            SqlErrorObject& errObj);

private:
    friend class SqlResultIter;
    bool _init(SqlErrorObject&);
    bool _connect(SqlErrorObject&);
    bool _setErrorObject(SqlErrorObject&,
                         std::string const& details=std::string(""));
    std::shared_ptr<mysql::MySqlConnection> _connection;
}; // class SqlConnection

}}} // namespace lsst::qserv::sql

// Local Variables:
// mode:c++
// comment-column:0
// End:

#endif // LSST_QSERV_SQL_SQLCONNECTION_H
