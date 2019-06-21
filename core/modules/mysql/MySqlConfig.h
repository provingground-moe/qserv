// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2010-2014 LSST Corporation.
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

#ifndef LSST_QSERV_MYSQL_MYSQLCONFIG_H
#define LSST_QSERV_MYSQL_MYSQLCONFIG_H

// System headers
#include <memory>
#include <string>

namespace lsst {
namespace qserv {
namespace sql {
    class SqlConnection;
}}}


namespace lsst {
namespace qserv {
namespace mysql {

/**
 *  Value class for configuring the MySQL connection
 *
 *  Instance can be created with network socket and/or file socket
 *  or only file socket. Parameters validity and MySQL insance connection can be
 *  both ckecked.
 *
 */
class MySqlConfig {
public:
    MySqlConfig() : port(0), maxTableSizeMB(0) {}

    /**
     *  Create MySqlConfig instance
     *
     *  @param username:     MySQL username
     *  @param password:     MySQL password
     *  @param hostname:     MySQL hostname
     *  @param port:         MySQL port
     *  @param socket:       MySQL socket
     *  @param db:           MySQL database
     *  @param maxtablesize: maximum table size.
     *
     * @throws std::runtime_error: if checkValid is true and some parameters are invalid
     */
    MySqlConfig(std::string const& username, std::string const& password,
                std::string const& hostname,
                unsigned int const port,
                std::string const& socket,
                std::string const& db = "",
                size_t maxresultsize = 0);

    /**
     *  Create MySqlConfig instance
     *
     *  @param username:     MySQL username
     *  @param password:     MySQL password
     *  @param socket:       MySQL socket
     *  @param db:           MySQL database
     *  @param maxtablesize: maximum table size.
     *
     * @throws std::runtime_error: if some parameters are invalid
     */
    MySqlConfig(std::string const& username, std::string const& password,
                std::string const& socket, std::string const& db = "",
                size_t maxtablesize = 0);

    /**
     * Create MySqlConfig instance with an SqlConnection that should be used instead of creating a new
     * connection. Used to implement custom behavior for unit tests.
     */
    MySqlConfig(std::shared_ptr<sql::SqlConnection> sqlConnection) : _sqlConnection(sqlConnection) {}

    /** Overload output operator for current class
     *
     * @param out
     * @param mysqlConfig
     * @return an output stream
     */
    friend std::ostream& operator<<(std::ostream &out, MySqlConfig const& mysqlConfig);

    /** Return a string representation of the object
     *
     * @return a string representation of the object
     */
    std::string toString() const;

    std::string username;
    std::string password;
    std::string hostname;
    unsigned int port;
    std::string socket;
    std::string dbName;
    size_t maxTableSizeMB;

    /**
     * Get the connection object to use if one was provided.
     * This is useful for unit testing.
     */
    std::shared_ptr<sql::SqlConnection> getConnection() const;

private:
    std::shared_ptr<sql::SqlConnection> _sqlConnection;
};

}}} // namespace lsst::qserv::mysql

#endif // LSST_QSERV_MYSQL_MYSQLCONFIG_H
