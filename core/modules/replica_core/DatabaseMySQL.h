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

    /*
     * Return an escaped and single-quoted string ready to be used in
     * an SQL statement.
     */
    std::string strVal (std::string const& str) const;

    /*
     * Return a non-escaped and back-tick-quoted string ready to be used in
     * an SQL statement.
     */
    std::string strId (std::string const& str) const;

    /**
     * Return the status of the transaction
     */
    bool inTransaction () const { return _inTransaction; }

    /**
     * Start the transaction
     *
     * @throws std::logic_error - if the transaction was already been started
     */
    void begin ();

    /**
     * Commit the transaction
     *
     * @throws std::logic_error - if the transaction was not started
     */
    void commit ();

    /**
     * Rollback the transaction
     *
     * @throws std::logic_error - if the transaction was not started
     */
    void rollback ();

    /**
     * Execute the specified query and initialize object context to allow
     * a result set extraction.
     *
     * @throws std::invalid_argument - for empty query strings
     * @throws DuplicateKeyError     - for attempts to insert rows with duplicate keys
     * @throws Error                 - for any other MySQL specific errors
     */
    void execute (std::string const& query);

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