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
#ifndef LSST_QSERV_SQL_MOCKSQL_H
#define LSST_QSERV_SQL_MOCKSQL_H

// Local headers
#include "sql/SqlConnection.h"

namespace lsst {
namespace qserv {
namespace sql {
class MockSql : public SqlConnection {
public:
    MockSql() {}
    ~MockSql() {}

    typedef std::map<std::string, std::vector<std::string>> DbColumns;
    MockSql(DbColumns dbColumns) : _dbColumns(dbColumns) {}

    virtual void reset(mysql::MySqlConfig const& sc, bool useThreadMgmt=false) {}
    virtual bool connectToDb(SqlErrorObject&) { return false; }
    virtual bool selectDb(std::string const& dbName, SqlErrorObject&) {
        return false; }
    virtual bool runQuery(char const* query, int qSize,
                          SqlResults& results, SqlErrorObject&) {
        return false; }
    virtual bool runQuery(char const* query, int qSize, SqlErrorObject&) {
        return false; }
    virtual bool runQuery(std::string const query, SqlResults&,
                          SqlErrorObject&) {
        return false; }
    virtual std::shared_ptr<SqlResultIter> getQueryIter(std::string const& query);
    virtual bool runQuery(std::string const query, SqlErrorObject&) {
        return false; }
    virtual bool dbExists(std::string const& dbName, SqlErrorObject&) {
        return false; }
    virtual bool createDb(std::string const& dbName, SqlErrorObject&,
                          bool failIfExists=true) {
        return false; }
    virtual bool createDbAndSelect(std::string const& dbName,
                                   SqlErrorObject&,
                                   bool failIfExists=true) {
        return false; }
    virtual bool dropDb(std::string const& dbName, SqlErrorObject&,
                        bool failIfDoesNotExist=true) {
        return false; }
    virtual bool tableExists(std::string const& tableName,
                             SqlErrorObject&,
                             std::string const& dbName="") {
        return false; }
    virtual bool dropTable(std::string const& tableName,
                           SqlErrorObject&,
                           bool failIfDoesNotExist=true,
                           std::string const& dbName="") {
        return false; }
    virtual bool listTables(std::vector<std::string>&,
                            SqlErrorObject&,
                            std::string const& prefixed="",
                            std::string const& dbName="") {
        return false; }
    virtual bool listColumns(std::vector<std::string>& columns,
                            SqlErrorObject&,
                            std::string const& dbName,
                            std::string const& tableName) {
        // The QueryContext gets all the columns in each table used by the query and stores this information
        // for lookup later. Here we return a list of column names for a table.
        auto dbColumnsItr = _dbColumns.find(tableName);
        if (dbColumnsItr == _dbColumns.end()) {
            return false;
        }
        for (auto const& column : dbColumnsItr->second) {
            columns.push_back(column);
        }
        return true;
    }

    virtual std::string getActiveDbName() const { return std::string(); }

    template <class TupleListIter>
    struct Iter : public SqlResultIter {
        Iter() {}
        Iter(TupleListIter begin, TupleListIter end) {
            _cursor = begin;
            _end = end;
        }
        virtual ~Iter() {}
        virtual SqlErrorObject& getErrorObject() { return _errObj; }
        virtual StringVector const& operator*() const { return *_cursor; }
        virtual SqlResultIter& operator++() { ++_cursor; return *this; }
        virtual bool done() const { return _cursor == _end; }

        SqlErrorObject _errObj;
        TupleListIter _cursor;
        TupleListIter _end;
    };

private:
    DbColumns _dbColumns;
}; // class MockSql

}}} // lsst::qserv::sql
// Local Variables:
// mode:c++
// comment-column:0
// End:
#endif // LSST_QSERV_SQL_MOCKSQL_H
