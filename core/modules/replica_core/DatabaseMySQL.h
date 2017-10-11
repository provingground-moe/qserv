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
#ifndef LSST_QSERV_REPLICA_CORE_DATABASE_MYSQL_H
#define LSST_QSERV_REPLICA_CORE_DATABASE_MYSQL_H

/// DatabaseMySQL.h declares:
///
/// class ConfigurationMySQL
/// (see individual class documentation for more information)

// System headers

#include <cstddef>
#include <map>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <mysql/mysql.h>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {
namespace database {
namespace mysql {

/**
 * Class Error represents a family of exceptions which are specific
 * to the implementation of this API.
 */
struct Error
    :   std::runtime_error {

    /// Constructor   
    explicit Error (std::string const& what)
        :   std::runtime_error (what) {
    }
};


/**
 * Instances of this exception class are thrown on attempts to insert
 * rows with duplicate keys.
 */
struct DuplicateKeyError
    :   Error {

    /// Constructor   
    explicit DuplicateKeyError (std::string const& what)
        :   Error (what) {
    }
};

/**
 * Instances of this exception class are thrown on failed attempts 
 * to interpret the contents of the result rest rows.
 */
struct InvalidTypeError
    :   Error {

    /// Constructor   
    explicit InvalidTypeError (std::string const& what)
        :   Error (what) {
    }
};

/**
 * This structure encapsulates connection parameters to a MySQL server
 */
struct ConnectionParams {
    /// The DNS name or IP address of a machine where the database
    /// server runs
    std::string host;

    /// The port number of the MySQL service
    uint16_t port;

    /// The name of a database user
    std::string user;

    /// The database password
    std::string password;

    /// The name of a database to be set upon the connection
    std::string database;  

    /// Return a string representation of all (but the password) parameters
    std::string toString () const;
};

/// Overloaded operator for serializing ConnectionParams instances
std::ostream& operator<< (std::ostream&, ConnectionParams const&);


/**
 * Class Row represens the current row obtained from the last result set.
 * It provides an interface for obtainig values if fields and translating
 * them from the internal MySQL representation into the corresponding C++ type
 * system.
 *
 * All type-specific 'get' methods defined in this class will return 'true' and
 * set the value returned for the specified column if the value was not 'NULL'.
 * They will return 'false' otherwise. All methods have two parameyters:
 *
 *   columnName - the name of a column
 *   value      - the value (of a type which depends on the method signature)
 *                to be initialized upon the succesful completion of a method
 *
 * Methods may also throw the following exceptions:
 *
 *   std::invalid_argument - for unknown column names
 *   InvalidTypeError      - when the conversion of row data into a value of
 *                           the requested type is not possible.
 */
class Row {
    
public:

    /// Class Connection is allowed to initialize the valid content
    /// of rows
    friend class Connection;

    /**
     * The class encapsulate a raw data pointer and the number of bytes
     * in each column.
     */
    typedef std::pair<char const*, size_t> Cell;

    /**
     * The default constructor will initialize invalid instances of the class.
     *
     * Most (but 'isValid', copy constructor, assignment operator an ddestrctor)
     * methods of this class may throw the following exceptions:
     *
     * @throws std::logic_error - if the object is not valid
     */
    Row ();

    /// Copy constructor
    Row (Row const& rhs);

    /// The Assignment operator
    Row& operator= (Row const& rhs);
 
    /// Destructor
    virtual ~Row ();

    /// Return 'true' of the object has meaningful content
    bool isValid () const { return _isValid; }

    /**
     * Return the width of the row
     */
    size_t numColumns () const;

    // These methods will return 'true' if the specified field is NULL

    bool isNull (size_t             columnIdx)  const;
    bool isNull (std::string const& columnName) const;

    // Type-specific data extractors/converters for values at the specified column
    //
    // @see class Row

    bool get (size_t      columnIdx,         std::string& value) const;
    bool get (std::string const& columnName, std::string& value) const;

    // Unsigned integer types

    bool get (size_t columnIdx, uint64_t& value) const;
    bool get (size_t columnIdx, uint32_t& value) const;
    bool get (size_t columnIdx, uint16_t& value) const;
    bool get (size_t columnIdx, uint8_t&  value) const;

    bool get (std::string const& columnName, uint64_t& value) const;
    bool get (std::string const& columnName, uint32_t& value) const;
    bool get (std::string const& columnName, uint16_t& value) const;
    bool get (std::string const& columnName, uint8_t&  value) const;

    // Signed integer types

    bool get (size_t columnIdx, int64_t& value) const;
    bool get (size_t columnIdx, int32_t& value) const;
    bool get (size_t columnIdx, int16_t& value) const;
    bool get (size_t columnIdx, int8_t&  value) const;

    bool get (std::string const& columnName, int64_t& value) const;
    bool get (std::string const& columnName, int32_t& value) const;
    bool get (std::string const& columnName, int16_t& value) const;
    bool get (std::string const& columnName, int8_t&  value) const;

    // Floating point types

    bool get (size_t columnIdx, float&  value) const;
    bool get (size_t columnIdx, double& value) const;

    bool get (std::string const& columnName, float&  value) const;
    bool get (std::string const& columnName, double& value) const;

    /**
     * Return a reference to the data cell for the column
     *
     * @param columnIdx - the index of a column
     */
    Cell const& getDataCell (size_t columnIdx) const;

    /**
     * Return a reference to the data cell for the column
     *
     * @param columnName - the name of a column
     */
    Cell const& getDataCell (std::string const& columnName) const;

private:

    /// The status of the object
    bool _isValid;

    /// Mapping column names to the indexes
    std::map<std::string, size_t> _name2index;

    /// Mapping column indexes to the raw data cells
    std::vector<Cell> _index2cell;
};

/**
 * Class Connection provides the main API to the database.
 */
class Connection
    :   public std::enable_shared_from_this<Connection> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Connection> pointer;

    /**
     * Connect to the MySQL service with the specified parameters and return
     * a pointer to the Connection object.
     * 
     * @param connectionParams - the connection parameters
     * @param autoReconnect    - automaticalluy reconnect to the service
     *                           if the dropped connection was discovered.
     *                           This option is useful when the application is inactive
     *                           for a prologed period of time causing the server to kick out
     *                           the client. Note that only one reconnection
     *                           attempt is made each time the dropped conneciton is detected.
     *
     * @return a valid object if the connection attempt succeeded
     * @throws Error - if the connection failed
     */
    static pointer open (ConnectionParams const& connectionParams,
                         bool                    autoReconnect=true);

    // Default construction and copy semantics are prohibited

    Connection () = delete;
    Connection (Connection const&) = delete;
    Connection& operator= (Connection const&) = delete;
 
    /// Destructor
    virtual ~Connection ();

    /**
      * A front-end to mysql_real_escape_string()
      *
      * @param str - a string to be processed
      *
      * @return the processed string
      */
    std::string escape (std::string const& str) const;

    // -------------------------------------------------
    // Helper methods for simplifying query preparation
    // -------------------------------------------------
    
    template <typename T>
    T           sqlValue (T const&           val) const { return val; }
    std::string sqlValue (std::string const& val) const { return "'" + escape (val) + "'"; }
    std::string sqlValue (char const*        val) const { return sqlValue (std::string(val)); }

    // Generator: ([value [, value [, ... ]]])
    // Where values of the string types will be surrounded with single quotes

    /// The end of variadic recursion
    void sqlValues (std::string& sql) const { sql += ")"; }

    /// The next step in the variadic recursion when at least one value is
    /// still available
    template <typename T,
              typename ...Targs>
    void sqlValues (std::string& sql,
                    T            val,
                    Targs...     Fargs) const {

        bool const last = sizeof...(Fargs) - 1 < 0;
        std::ostringstream ss;
        ss << (sql.empty() ? "(" : (last ? "" : ",")) << sqlValue (val);
        sql += ss.str();

        // Recursively keep drilling down the list of arguments with one
        // argument less
        sqlValues (sql, Fargs...);
    }
    
    /**
     * Turn values of variadic argumenst into a valid SQL representing a set of
     * values to be insert into a table row. Values of string types 'std::string const&'
     * and 'char const*' will be also escaped and surrounded by single quote.
     *
     * For example, the following call:
     *   @code
     *     sqlPackValues ("st'r", std::string("c"), 123, 24.5);
     *   @code
     * will produce the following output:
     *   @code
     *     ('st\'r','c',123,24.5)
     *   @code
     */
    template <typename...Targs>
    std::string sqlPackValues (Targs... Fargs) const {
        std::string sql;
        sqlValues (sql, Fargs...);
        return sql;
    }

    /**
     * Generate an SQL statement for inserting a single row into the specified
     * table based on a variadic list of values to be inserted. The method allows
     * any number of arguments and any types of argument values. rguments of
     * types 'std::sting' and 'char*' will be additionally escaped and surrounded by
     * single quotes as required by the SQL standard.
     *
     * @param tableName - the name of a table
     * @param Fargs     - the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlInsertQuery (std::string const& tableName,
                                Targs...           Fargs) const {
        std::ostringstream qs;
        qs  << "INSERT INTO " << sqlId (tableName) << " "
            << "VALUES "      << sqlPackValues (Fargs...);
        return qs.str();
    }

    // ----------------------------------------------------------------------
    // Generator: [`column` = value [, `column` = value [, ... ]]]
    // Where values of the string types will be surrounded with single quotes

    /// Return a non-escaped and back-tick-quoted string which is meant
    /// to be an SQL identifier.
    std::string sqlId (std::string const& str) const { return "`" + str + "`"; }

    /**
     * Return:
     *
     *   `col` = <value>
     *
     * Where:
     * - the column name will be surrounded by back ticks
     * - values of string types will be escped and surrounded by single quotes
     */
    template <typename T>
    std::string sqlEqual (std::string const& col,
                          T const&           val) const {
        std::ostringstream ss;
        ss << sqlId (col) << "=" << sqlValue (val);
        return ss.str();
    }

    /// The base (the final function) to be called
    void sqlPackPair (std::string&) const {}

    /// Recursive variadic function (overloaded for column names gived as std::string)
    template <typename T, typename...Targs>
    void sqlPackPair (std::string&             sql,
                      std::pair<std::string,T> colVal,
                      Targs...                 Fargs) const {
    
        std::string const& col = colVal.first;
        T const&           val = colVal.second;
    
        std::ostringstream ss;
        ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlEqual (col, val);
        sql += ss.str();
        sqlPackPair (sql, Fargs...);
    }
    

    /// Recursive variadic function (overloaded for column names gived as char const*)
    template <typename T, typename...Targs>
    void sqlPackPair (std::string&             sql,
                      std::pair<char const*,T> colVal,
                      Targs...                 Fargs) const {
    
        std::string const  col = colVal.first;
        T const&           val = colVal.second;
    
        std::ostringstream ss;
        ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlEqual (col, val);
        sql += ss.str();
        sqlPackPair (sql, Fargs...);
    }
    
    /**
     * Pack pairs of column names and their new values into a string which can be
     * further used to form SQL statements of the following kind:
     *
     *   UPDATE <table> SET <packed-pairs>
     *
     * NOTES:
     * - The method allows any number of arguments and any types of value types.
     * - Values types 'std::sting' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * - The column names will be surrounded with back-tick quotes.
     *
     * For example, the following call:
     *   @code
     *     sqlPackPairs (
     *       std::make_pair ("col1", "st'r"),
     *       std::make_pair ("col2", std::string("c")),
     *       std:;make_pair ("col3", 123));
     *   @code
     * will produce the following output:
     *   @code
     *     `col1`='st\'r',`col2`="c",`col3`=123
     *   @code
     *
     * @param tableName      - the name of a table
     * @param whereCondition - the optional condition for selecting rows to be updated 
     * @param Fargs          - the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlPackPairs (Targs... Fargs) const {
        std::string sql;
        sqlPackPair (sql, Fargs...);
        return sql;
    }
    
    /**
     * Generate an SQL statement for updating select values of table rows
     * where the optional condition is met. Fields to be updated and their new
     * values are passed into the method as variadic list of std::pair objects.
     *
     *   UPDATE <table> SET <packed-pairs> [WHERE <condition>]
     * 
     * NOTES:
     * - The method allows any number of arguments and any types of value types.
     * - Values types 'std::sting' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * - The column names will be surrounded with back-tick quotes.
     *
     * @param tableName      - the name of a table
     * @param whereCondition - the optional condition for selecting rows to be updated 
     * @param Fargs          - the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlSimpleUpdateQuery (std::string const& tableName,
                                      std::string const& condition,
                                      Targs...           Fargs) const {
        std::ostringstream qs;
        qs  << "UPDATE " << sqlId (tableName)       << " "
            << "SET "    << sqlPackPairs (Fargs...) << " "
            << (condition.empty() ? "" : "WHERE " + condition);
        return qs.str();
    }

    /**
     * Return the status of the transaction
     */
    bool inTransaction () const { return _inTransaction; }

    /**
     * Start the transaction
     *
     * @return                  - a smart pointer to self to allow chaned calles.
     * @throws std::logic_error - if the transaction was already been started
     */
    Connection::pointer begin ();

    /**
     * Commit the transaction
     *
     * @return                  - a smart pointer to self to allow chaned calles.
     * @throws std::logic_error - if the transaction was not started
     */
    Connection::pointer commit ();

    /**
     * Rollback the transaction
     *
     * @return                  - a smart pointer to self to allow chaned calles.
     * @throws std::logic_error - if the transaction was not started
     */
    Connection::pointer rollback ();

    /**
     * Execute the specified query and initialize object context to allow
     * a result set extraction.
     *
     * @param  query                 - a query to be execured
     * @return                       - the smart pointer to self to allow chaned calles.
     * @throws std::invalid_argument - for empty query strings
     * @throws DuplicateKeyError     - for attempts to insert rows with duplicate keys
     * @throws Error                 - for any other MySQL specific errors
     */
    Connection::pointer execute (std::string const& query);

    /**
     * Execute an SQL statement for inserting a new row into a table based
     * on a variadic list of values to be inserted. The method allows
     * any number of arguments and any types of argument values. Arguments of
     * types 'std::sting' and 'char*' will be additionally escaped and surrounded by
     * single quotes as required by the SQL standard.
     *
     * The effect:
     *
     *   INSERT INTO <table> VALUES (<packed-values>)
     * 
     * ATTENTION: the method will *NOT* start a transaction, neither it will
     * commit the one in the end. Transaction management is a responsibility
     * of a caller of the method.
     *
     * @see Connection::sqlInsertQuery()
     *
     * @param tableName - the name of a table
     * @param Fargs     - the variadic list of values to be inserted
     * 
     * @return - the smart pointer to self to allow chaned calles.
     * 
     * @throws std::invalid_argument - for empty query strings
     * @throws DuplicateKeyError     - for attempts to insert rows with duplicate keys
     * @throws Error                 - for any other MySQL specific errors
     */
    template <typename...Targs>
    Connection::pointer executeInsertQuery (std::string const& tableName,
                                            Targs...           Fargs) {

        return execute (sqlInsertQuery (tableName,
                                        Fargs...));
    }

    /**
     * Execute an SQL statement for updating select values of table rows
     * where the optional condition is met. Fields to be updated and their new
     * values are passed into the method as variadic list of std::pair objects.
     *
     * The effect:
     *
     *   UPDATE <table> SET <packed-pairs> [WHERE <condition>]
     *
     * ATTENTION: the method will *NOT* start a transaction, neither it will
     * commit the one in the end. Transaction management is a responsibility
     * of a caller of teh method.
     *
     * @see Connection::sqlSimpleUpdateQuery()
     *
     * @param tableName      - the name of a table
     * @param whereCondition - the optional condition for selecting rows to be updated 
     * @param Fargs          - the variadic list of column-value pairs to be updated
     * 
     * @return - the smart pointer to self to allow chaned calles.
     * 
     * @throws std::invalid_argument - for empty query strings
     * @throws Error                 - for any MySQL specific errors
     */
    template <typename...Targs>
    Connection::pointer executeSimpleUpdateQuery (std::string const& tableName,
                                                  std::string const& condition,
                                                  Targs...           Fargs) {

        return execute (sqlSimpleUpdateQuery (tableName,
                                              condition,
                                              Fargs...));
    }

    /**
     * Returns 'true' if the last successfull query returned a result set
     * (even though it may be empty)
     */
    bool hasResult () const;

    /**
     * Return the names of the columns from the current result set.
     *
     * NOTE: the columns are returned exactly in the same order they were
     *       requested in the corresponding query.
     *
     * @throws std::logic_error - if no SQL statement has ever been executed, or
     *                            if the last query failed.
     */
    std::vector<std::string> const& columnNames () const;

    /**
     * Move the iterator to the next (first) row of the current result set
     * and if the iterator is not beyond the last row then nnitialize an object
     * passed as a parameter.
     *
     * ATTENTION: objects initialized upon the successful completion
     * of the method are valid until the next call to the method or before
     * the next query. Hence the safe practice for using this method to iterate
     * over a result set would be:
     *
     *   @code
     *     Connection::pointer conn = Connection::connect(...);
     *     conn->execute ("SELECT ...");
     *
     *     Row row;
     *     while (conn->nextRow(row)) {
     *         // Extract data from 'row' within this block
     *         // before proceeding to teh next row, etc.
     *     }
     *   @code
     * 
     * @throws std::logic_error - if no SQL statement has ever been executed, or
     *                            if the last query failed.
     *
     * @return 'true' if the row was initialized or 'false' if past the last row
     *          in the result set.
     */
    bool next (Row& row);

private:

    /**
     * Construct an object
     *
     * @param connectionParams - the connection parameters
     * @param autoReconnect    - automaticalluy reconnect to the service
     *                           if the dropped connection was discovered.
     */
    explicit Connection (ConnectionParams const& connectionParams,
                         bool                    autoReconnect);

    /**
     * Establish a connection
     *
     * @throws Error - if the connection is not possible
     */
    void connect ();

    /**
     * The method is to ensure that the transaction is in teh desired state.
     * 
     * @param inTransaction - the desired state of the transaction
     */
    void assertTransaction (bool inTransaction) const;

    /**
     * The method is to ensure that a proper query context is set and
     * its result set can be explored.
     *
     * @throw Error - if the connection is not established or no prior query was made
     */
    void assertQueryContext () const;

private:

    /// Parameters of the connection
    ConnectionParams const _connectionParams;

    /// Auto-reconnect policy
    bool _autoReconnect;

    /// The last SQL statement
    std::string _lastQuery;

    /// Transaction status
    bool _inTransaction;

    /// Connection
    MYSQL* _mysql;

    // Last result set

    MYSQL_RES*   _res;
    MYSQL_FIELD* _fields;

    size_t _numFields;

    std::vector<std::string> _columnNames;

    // Get updated after fetching each row of the result set

    MYSQL_ROW _row;     // must be cahed here to ensure its lifespan
                        // while a client will be processing its content.
};

}}}}} // namespace lsst::qserv::replica_core::database::mysql

#endif // LSST_QSERV_REPLICA_CORE_DATABASE_MYSQL_H