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
#include "replica_core/DatabaseMySQL.h"

// System headers

#include <boost/lexical_cast.hpp>
#include <mysql/mysqld_error.h>
#include <sstream>

// Qserv headers

namespace {

/**
 * The function is used to comply with the MySQL convention for
 * the default values of the connection parameters.
 */
char const* stringOrNull (std::string const& str) {
    if (str.empty()) return 0;
    return str.c_str();
}

using Row              = lsst::qserv::replica_core::database::mysql::Row;
using InvalidTypeError = lsst::qserv::replica_core::database::mysql::InvalidTypeError;

class RowImpl {

public:

    template <class T>
    static bool getNumber (Row const&         row,
                           std::string const& columnName,
                           T&                 value) {
        try {
            Row::Cell const& cell = row.getDataCell (columnName);
            if (cell.first) {
                value = boost::lexical_cast<uint64_t>(cell.first, cell.second);
                return true;
            }
            return false;
        } catch (boost::bad_lexical_cast const& ex) {
            throw InvalidTypeError ("RowImpl::getNumber<T>()  type conversion failed");
        }
    }
};

}   // namespace


namespace lsst {
namespace qserv {
namespace replica_core {
namespace database {
namespace mysql {


/////////////////////////////////////////////////////
//                ConnectionParams                 //
/////////////////////////////////////////////////////

std::string
ConnectionParams::toString () const {
    std::ostringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<< (std::ostream& os, ConnectionParams const& params) {
    os  << "DatabaseMySQL::ConnectionParams "
        << "(host=" << params.host
        << " port=" << params.port
        << " db="   << params.db
        << " user=" << params.user
        << " password=*****)";
    return os;
}


///////////////////////////////////////
//                Row                //
///////////////////////////////////////

Row::Row ()
    :   _isValid (false) {
}

Row::Row (Row const& rhs) {
    _isValid = rhs._isValid;
    _cells   = rhs._cells;
}

Row&
Row::operator= (Row const& rhs) {
    if (this != &rhs) {
        _isValid = rhs._isValid;
        _cells   = rhs._cells;   
    }
    return *this;
}

Row::~Row () {
}


size_t
Row::numColumns () const {
    if (!_isValid)
        throw std::logic_error (":Row::numColumns()  the object is not valid");
    return  _cells.size();
}

bool
Row::get (std::string const& columnName,
          std::string&       value) const {

    Cell const& cell = getDataCell (columnName);
    if (cell.first) {
        value = std::string(cell.first, cell.second);
        return true;
    }
    return false;
}


bool
Row::get (std::string const& columnName, uint64_t& value) const {
    return ::RowImpl::getNumber<uint64_t> (*this, columnName, value);
}

bool
Row::get (std::string const& columnName, uint32_t& value) const {
    return ::RowImpl::getNumber<uint32_t> (*this, columnName, value);
}

bool
Row::get (std::string const& columnName, uint16_t& value) const {
    return ::RowImpl::getNumber<uint16_t> (*this, columnName, value);
}

bool
Row::get (std::string const& columnName, uint8_t& value) const {
    return ::RowImpl::getNumber<uint8_t> (*this, columnName, value);
}


bool
Row::get (std::string const& columnName, int64_t& value) const {
    return ::RowImpl::getNumber<int64_t> (*this, columnName, value);
}

bool
Row::get (std::string const& columnName, int32_t& value) const {
    return ::RowImpl::getNumber<int32_t> (*this, columnName, value);
}

bool
Row::get (std::string const& columnName, int16_t& value) const {
    return ::RowImpl::getNumber<int16_t> (*this, columnName, value);
}

bool
Row::get (std::string const& columnName, int8_t& value) const {
    return ::RowImpl::getNumber<int8_t> (*this, columnName, value);
}

bool
Row::get (std::string const& columnName, float& value) const {
    return ::RowImpl::getNumber<float> (*this, columnName, value);
}

bool
Row::get (std::string const& columnName, double& value) const {
    return ::RowImpl::getNumber<double> (*this, columnName, value);
}



Row::Cell const&
Row::getDataCell (std::string const& columnName) const {
    if (!_isValid)
        throw std::logic_error ("Row::getDataCell()  the object is not valid");

    if (!_cells.count(columnName))
        throw std::invalid_argument ("Row::getDataCell()  the column '" + columnName + "'is not in the result set");
    
    return _cells.at(columnName);
}


///////////////////////////////////////////////
//                Connection                 //
///////////////////////////////////////////////

Connection::pointer
Connection::connect (ConnectionParams const& connectionParams,
                     bool                    autoReconnect) {

    Connection::pointer ptr (new Connection(connectionParams,
                                            autoReconnect));
    ptr->connectImpl();
    return ptr;
}

Connection::Connection (ConnectionParams const& connectionParams,
                        bool                    autoReconnect)
    :   _connectionParams (connectionParams),
        _autoReconnect    (autoReconnect),

        _inTransaction (false),

        _mysql  (nullptr),
        _res    (nullptr),
        _fields (nullptr),

        _numFields (0),
        _numRows   (0) {
}


Connection::~Connection () {
    if (_res) mysql_free_result (_res) ;
}

std::string
Connection::escape (std::string const& inStr) const {

    if (!_mysql)
        throw Error ("Connection::escape()  not connected to the MySQL service");

    size_t const inLen = inStr.length ();

    // Allocate at least that number of bytes to cover the worst case scenario
    // of each input character to be escaped plus the end of string terminator.
    // See: https://dev.mysql.com/doc/refman/5.7/en/mysql-real-escape-string.html

    size_t const outLenMax = 2*inLen + 1;

    // The temporary storage will get automatically deconstructed in the end
    // of this block.

    std::unique_ptr<char> outStr(new char[outLenMax]);
    size_t const outLen =
        mysql_real_escape_string (
            _mysql,
            outStr.get(),
            inStr.c_str(),
            inLen);

    return std::string (outStr.get(), outLen) ;
}

void
Connection::begin () {
    assertTransaction (false);
    execute ("BEGIN");
    _inTransaction = true;
}


void
Connection::commit () {
    assertTransaction (true);
    execute ("COMMIT");
    _inTransaction = false;
}


void
Connection::rollback () {
    assertTransaction (true);
    execute ("ROLLBACK");
    _inTransaction = false;
}

void
Connection::execute (std::string const& query) {

    assertTransaction (true);

    if (query.empty())
        throw std::invalid_argument (
                "Connection::execute()  empty query string passed into the object");

    // Reset/initialize the query context before attempting to execute
    // the new  query.

    _lastQuery = query;
    _res       = nullptr;
    _fields    = nullptr;
    _numFields = 0;
    _numRows   = 0;
     
    if (mysql_real_query (_mysql,
                          _lastQuery.c_str(),
                          _lastQuery.size())) {

        std::string const msg =
            "Connection::execute()  query: '" + _lastQuery + "', error: " +
            std::string(mysql_error(_mysql));

        switch (mysql_errno(_mysql)) {
            case ER_DUP_KEY: throw DuplicateKeyError (msg);
            default:         throw Error             (msg);
        }
    }
    
    // Fetch result set if any

    _res = mysql_store_result (_mysql);
    if (!_res)
        throw Error ("Connection::execute()  mysql_store_result failed, error: " +
                     std::string(mysql_error(_mysql)) +
                     ", query: '" + _lastQuery + "'");

    _fields    = mysql_fetch_fields (_res);
    _numFields = mysql_num_fields   (_res);
    _numRows   = mysql_num_rows     (_res);
}


size_t
Connection::numRows () const {
    assertQueryContext ();
    return _numRows;
}

bool
Connection::next (Row& row) {

    assertQueryContext ();

    // Check if the result set is empty or there are no more rows
    // in the set to process.
    if (!_numRows) return false;
    --_numRows;

    _row = mysql_fetch_row (_res);
    if (!_row)
        throw Error ("Connection::next()  mysql_fetch_row failed, error: " +
                     std::string(mysql_error(_mysql)) +
                     ", query: '" + _lastQuery + "'");

    size_t const* lengths = mysql_fetch_lengths (_res);

    // Transfer the data pointers for each field and their lengths into
    // the provided Row object.

    row._isValid = true;
    for (size_t i = 0; i < _numFields; i++)
        row._cells[_fields[i].name] =
            Row::Cell{_row   [i],
                      lengths[i]};

    return true;
}

void
Connection::connectImpl () {

    // Prepare the connection object
    if (!(_mysql = mysql_init (_mysql)))
        throw Error("Connection::connectImpl()  mysql_init failed");
    
    // Allow automatic reconnect if requested
    if (_autoReconnect) {
        my_bool reconnect = 0;
        mysql_options(_mysql, MYSQL_OPT_RECONNECT, &reconnect);
    }

    // Connect now
    if (!mysql_real_connect (
        _mysql,
        ::stringOrNull (_connectionParams.host),
        ::stringOrNull (_connectionParams.user),
        ::stringOrNull (_connectionParams.password),
        ::stringOrNull (_connectionParams.db),
        _connectionParams.port,
        0,  /* no defaulkt UNIX socket */
        0)) /* no default client flag */
        throw Error ("Connection::connectImpl()  mysql_real_connect() failed, error: " +
                     std::string(mysql_error(_mysql)));

    // Set session attributes
    if (mysql_query (_mysql, "SET SESSION SQL_MODE='ANSI'") ||
        mysql_query (_mysql, "SET SESSION AUTOCOMMIT=0"))
        throw Error ("Connection::connectImpl()  mysql_query() failed, error: " +
                     std::string(mysql_error(_mysql)));
}

void
Connection::assertQueryContext () const {
    if (!_mysql) throw Error ("Connection::assertQueryContext()  not connected to the MySQL service");
    if (!_res)   throw Error ("Connection::assertQueryContext()  no prior query made");
}

void
Connection::assertTransaction (bool inTransaction) const {
    if (inTransaction != _inTransaction)
        throw std::logic_error (
                "Connection::assertTransaction()  the transaction is" +
                std::string( _inTransaction ? " " : " not") + " active");
}

}}}}} // namespace lsst::qserv::replica_core::database::mysql
