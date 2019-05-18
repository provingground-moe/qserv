// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
/**
  * @file
  *
  * @brief class ColumnRef implementation
  *
  * @author Daniel L. Wang, SLAC
  */


// Class header
#include "query/ColumnRef.h"

// System headers
#include <iostream>
#include <tuple>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/TableRef.h"
#include "query/QueryTemplate.h"
#include "query/TableRef.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.query.ColumnRef");

typedef std::tuple<lsst::qserv::query::TableRefBase const&, std::string const&> TableRefStringTuple;

}


namespace lsst {
namespace qserv {
namespace query {


std::ostream& operator<<(std::ostream& os, ColumnRef const& cr) {
    os << "ColumnRef(";
    os << *cr._tableRef;
    os << ", \"" << cr._column << "\"";
    os << ")";
    return os;
}


std::ostream& operator<<(std::ostream& os, ColumnRef const* cr) {
    if (nullptr == cr) {
        os << "nullptr";
    } else {
        return os << *cr;
    }
    return os;
}


ColumnRef::Ptr ColumnRef::newShared(std::string const& db, std::string const& table,
        std::string const& column) {
    return std::make_shared<ColumnRef>(db, table, column);
}

ColumnRef::ColumnRef(std::string db, std::string table, std::string column)
    : _tableRef(std::make_shared<TableRefBase>(db, table, "")), _column(column) {
}


ColumnRef::ColumnRef(std::string db, std::string table, std::string tableAlias, std::string column)
    : _tableRef(std::make_shared<TableRefBase>(db, table, tableAlias)), _column(column) {
}


ColumnRef::ColumnRef(std::shared_ptr<TableRefBase> const& table, std::string const& column)
    : _tableRef(table), _column(column) {
}


std::string const& ColumnRef::getDb() const {
    return _tableRef->getDb();
}


std::string const& ColumnRef::getTable() const {
    return _tableRef->getTable();
}


std::string const& ColumnRef::getColumn() const {
    return _column;
}


std::string const& ColumnRef::getTableAlias() const {
    return _tableRef->getAlias();
}


std::shared_ptr<TableRefBase> ColumnRef::getTableRef() const {
    return _tableRef;
}


std::shared_ptr<TableRefBase>& ColumnRef::getTableRef() {
    return _tableRef;
}


void ColumnRef::setDb(std::string const& db) {
    LOGS(_log, LOG_LVL_TRACE, *this << "; set db:" << db);
    _tableRef->setDb(db);
}


void ColumnRef::setTable(std::string const& table) {
    LOGS(_log, LOG_LVL_TRACE, *this << "; set table:" << table);
    _tableRef->setTable(table);
}


void ColumnRef::setColumn(std::string const& column) {
    LOGS(_log, LOG_LVL_TRACE, *this << "; set column:" << column);
    _column = column;
}


void ColumnRef::set(std::string const& db, std::string const& table, std::string const& column) {
    setDb(db);
    setTable(table);
    setColumn(column);
}


void ColumnRef::renderTo(QueryTemplate& qt) const {
    qt.append(*this);
}


bool ColumnRef::isSubsetOf(const ColumnRef::Ptr & rhs) const {
    // the columns can not be empty
    if (_column.empty() || rhs->_column.empty()) {
        throw logic_error("Column ref should not have an empty column name.");
    }
    if (not _tableRef->isSubsetOf(rhs->_tableRef)) {
        return false;
    }
    if (_column != rhs->_column) {
        return false;
    }
    return true;
}


bool ColumnRef::equal(ColumnRef const& rhs, bool useAlias) const {
    // if they match they compare
    if (*this == rhs) {
        return true;
    // if we're not supposed to check the alias then we're done; no match.
    } else if (not useAlias) {
        return false;
    }
    // if we use the alias, check the column first
    if (_column != rhs._column) {
        return false;
    }
    // now see if either of the tableRefs is an alias of the other
    if (true == _tableRef->isAliasedBy(*rhs._tableRef) || true == rhs._tableRef->isAliasedBy(*_tableRef)) {
        return true;
    }
    // and now if we haven't returned true then no match, return false.
    return false;
}


bool ColumnRef::lessThan(ColumnRef const& rhs, bool useAlias) const {
    if (useAlias && this->equal(rhs, useAlias)) {
        return false;
    }
    return *this < rhs;
}


bool ColumnRef::operator==(const ColumnRef& rhs) const {
    // this implementation _could_ use TableRef's operator==, but historically this function does _not_
    // consider the table's alias (except where it was assigned to the _table string that used to be in this
    // object, instead of having a TableRef). For the time being, to preserve functionality, I'm going to
    // keep this as similar to what it was as possible. However, since some functions assigned alias to the
    // table of the ColumnRef and that will be going away, this may have to be revisited. I suspect a rule
    // like "if both TableRefs have an alias use that, otherwise use the db & table". OTOH, if all TableRefs
    // are to be fully populated then maybe it does not matter if the alias or db+table are used? TBD.
    return std::tie(_tableRef->getDb(), _tableRef->getTable(), _column) ==
            std::tie(rhs._tableRef->getDb(), rhs._tableRef->getTable(), rhs._column);
}


bool ColumnRef::operator<(const ColumnRef& rhs) const {
    // this implementation _could_ use TableRef's operator<, but for now it's not, for the reasons discussed
    // in the comment of ColumnRef::operator==.
    return std::tie(_tableRef->getDb(), _tableRef->getTable(), _column) <
            std::tie(rhs._tableRef->getDb(), rhs._tableRef->getTable(), rhs._column);
}


}}} // namespace lsst::qserv::query
