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


namespace {
    LOG_LOGGER _log = LOG_GET("lsst.qserv.query.ColumnRef");
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


ColumnRef::ColumnRef(std::string db, std::string table, std::string column)
    : _tableRef(std::make_shared<TableRef>(db, table, "")), _column(column)
{}


ColumnRef::Ptr ColumnRef::newShared(std::string const& db, std::string const& table,
        std::string const& column) {
    return std::make_shared<ColumnRef>(db, table, column);
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


void ColumnRef::renderTo(QueryTemplate& qt) const {
    qt.append(*this);
}


bool ColumnRef::isSubsetOf(const ColumnRef::Ptr & rhs) const {
    // the columns can not be empty
    if (_column.empty() || rhs->_column.empty()) {
        return false;
    }
    if (not _tableRef->isSubsetOf(rhs->_tableRef)) {
        return false;
    }
    if (_column != rhs->_column) {
        return false;
    }
    return true;
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
