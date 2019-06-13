/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_SQL_SQLEXCEPTION_H
#define LSST_QSERV_SQL_SQLEXCEPTION_H

// System headers

// Third-party headers

// Qserv headers
#include "util/Issue.h"


namespace lsst {
namespace qserv {
namespace sql {


class SqlException : public util::Issue {
public:
    SqlException(util::Issue::Context const& ctx, std::string const& message)
        : util::Issue(ctx, message)
    {}

    virtual ~SqlException() {}
};


class NoSuchDb : public SqlException {
public:
    NoSuchDb(util::Issue::Context const& ctx, std::string const& name)
        : SqlException(ctx, "No such database: " + name)
    {}

    virtual ~NoSuchDb() {}
};


class NoSuchTable : public SqlException {
public:
    NoSuchTable(util::Issue::Context const& ctx, std::string const& dbName,
                std::string const& tableName)
        : SqlException(ctx, "No such table: " + tableName + " in database: " + dbName)
    {}

    virtual ~NoSuchTable() {}
};


}}} // namespace lsst::qserv::sql

#endif // LSST_QSERV_SQL_SQLEXCEPTION_H
