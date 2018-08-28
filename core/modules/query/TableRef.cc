// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 AURA/LSST.
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
  * @brief TableRefN, SimpleTableN, JoinRefN implementations
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/TableRef.h"

// System headers
#include <algorithm>
#include <sstream>

 // Third-party headers

// Qserv headers
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "util/IterableFormatter.h"
#include "util/PointerCompare.h"

namespace {
lsst::qserv::query::JoinRef::Ptr
joinRefClone(lsst::qserv::query::JoinRef::Ptr const& r) {
    return r->clone();
}

std::string unquote(std::string str) {
    if (str.length() >= 3 && str.find('`') == 0 && str.rfind('`') == str.length()-1) {
        str.erase(str.begin());
        str.erase(--(str.end()));
    }
    return str;
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace query {

////////////////////////////////////////////////////////////////////////
// TableRef
////////////////////////////////////////////////////////////////////////
TableRef::TableRef(std::string const& db_, std::string const& table_, std::string const& alias_)
        : _alias(alias_), _db(db_), _table(table_), _unquotedTable(unquote(table_))  {
}

std::ostream& operator<<(std::ostream& os, TableRef const& ref) {
    os << "TableRef(";
    os << "alias:" << ref._alias;
    os << ", db:" << ref._db;
    os << ", table:" << ref._table;
    os << ", joinRefs:" << util::printable(ref._joinRefs);
    os << ")";
    return os;
}

std::ostream& operator<<(std::ostream& os, TableRef const* ref) {
    if (nullptr == ref) {
        os << "nullptr";
    } else {
        os << *ref;
    }
    return os;
}

void TableRef::render::applyToQT(TableRef const& ref) {
    if (_count++ > 0) _qt.append(",");
    ref.putTemplate(_qt);
}

std::ostream& TableRef::putStream(std::ostream& os) const {
    os << "Table(" << _db << "." << _table << ")";
    if (!_alias.empty()) { os << " AS " << _alias; }
    typedef JoinRefPtrVector::const_iterator Iter;
    for(Iter i=_joinRefs.begin(), e=_joinRefs.end(); i != e; ++i) {
        JoinRef const& j = **i;
        os << " " << j;
    }
    return os;
}

void TableRef::putTemplate(QueryTemplate& qt) const {
    if (!_db.empty()) {
        qt.append(_db); // Use TableEntry?
        qt.append(".");
    }
    qt.append(_table);
    if (!_alias.empty()) {
        qt.append("AS");
        qt.append(_alias);
    }
    typedef JoinRefPtrVector::const_iterator Iter;
    for(Iter i=_joinRefs.begin(), e=_joinRefs.end(); i != e; ++i) {
        JoinRef const& j = **i;
        j.putTemplate(qt);
    }
}

void TableRef::setTable(std::string const& table_) {
    _table = table_;
    _unquotedTable = _table;
    unquote(_unquotedTable);
}


void TableRef::addJoin(std::shared_ptr<JoinRef> r) {
    _joinRefs.push_back(r);
}

void TableRef::addJoins(const JoinRefPtrVector& r) {
    _joinRefs.insert(std::end(_joinRefs), std::begin(r), std::end(r));
}

void TableRef::apply(TableRef::Func& f) {
    f(*this);
    typedef JoinRefPtrVector::iterator Iter;
    for(Iter i=_joinRefs.begin(), e=_joinRefs.end(); i != e; ++i) {
        JoinRef& j = **i;
        j.getRight()->apply(f);
    }
}

void TableRef::apply(TableRef::FuncC& f) const {
    f(*this);
    typedef JoinRefPtrVector::const_iterator Iter;
    for(Iter i=_joinRefs.begin(), e=_joinRefs.end(); i != e; ++i) {
        JoinRef const& j = **i;
        j.getRight()->apply(f);
    }
}

TableRef::Ptr TableRef::clone() const {
    TableRef::Ptr newCopy = std::make_shared<TableRef>(_db, _table, _alias);
    std::transform(_joinRefs.begin(), _joinRefs.end(),
                   std::back_inserter(newCopy->_joinRefs), joinRefClone);
    return newCopy;
}

bool TableRef::operator==(const TableRef& rhs) const {
    // TODO joinrefs *should* be considered in operator==, right? (or no?)
    return std::tie(_alias, _db, _table) == std::tie(rhs._alias, rhs._db, rhs._table) &&
            std::equal(_joinRefs.begin(), _joinRefs.end(), rhs._joinRefs.begin(), rhs._joinRefs.end(),
                    [](std::shared_ptr<JoinRef> const & lhs, std::shared_ptr<JoinRef> const & rhs) {
                        return *lhs == *rhs;});
}

bool TableRef::operator<(const TableRef& rhs) const {
    // TODO what does it mean for joinrefs to be <?
    return std::tie(_alias, _db, _table) < std::tie(rhs._alias, rhs._db, rhs._table);
}

}}} // Namespace lsst::qserv::query
