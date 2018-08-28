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

// Qserv headers
#include "query/QueryTemplate.h"

namespace {

// TODO this is duplicated from TableRef.h; find a good common location & move it there.
std::string unquote(std::string str) {
    if (str.length() >= 3 && str.find('`') == 0 && str.rfind('`') == str.length()-1) {
        str.erase(str.begin());
        str.erase(--(str.end()));
    }
    return str;
}

} // end anonymous namespace

namespace lsst {
namespace qserv {
namespace query {


ColumnRef::ColumnRef(std::string db_, std::string table_, std::string column_)
: _tableRef(std::make_shared<TableRef>(db_, table_, ""))
, _column(column_)
, _unquotedColumn(unquote(column_))
{}


void ColumnRef::setColumn(std::string const & column) {
    _column = column;
    _unquotedColumn = unquote(column);
}


std::ostream& operator<<(std::ostream& os, ColumnRef const& cr) {
    os << "ColumnRef(";
    os << "tableRef:" << *(cr._tableRef);
    os << ", column:" << cr.getColumn();
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

void ColumnRef::renderTo(QueryTemplate& qt) const {
    qt.append(*this);
}

bool ColumnRef::isSubsetOf(const ColumnRef::Ptr & rhs) const {
    // the columns can not be empty
    if (getColumn().empty() || rhs->getColumn().empty()) {
        return false;
    }
    // if the table is empty, the db must be empty
    if (getTable().empty() && !getDb().empty()) {
        return false;
    }
    if (rhs->getTable().empty() && !rhs->getDb().empty()) {
        return false;
    }

    if (!getDb().empty()) {
        if (getDb() != rhs->getDb()) {
            return false;
        }
    }
    if (!getTable().empty()) {
        if (getTable() != rhs->getTable()) {
            return false;
        }
    }
    if (getColumn() != rhs->getColumn()) {
        return false;
    }
    return true;
}


bool ColumnRef::operator==(const ColumnRef& rhs) const {
    return std::tie(*_tableRef, _unquotedColumn) == std::tie(*rhs._tableRef, rhs._column);
}


bool ColumnRef::operator<(const ColumnRef& rhs) const {
    return std::tie(*_tableRef, _unquotedColumn) < std::tie(*rhs._tableRef, rhs._column);
}


}}} // namespace lsst::qserv::query
