// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2019 LSST Corporation.
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


#ifndef LSST_QSERV_QUERY_TABLEALIAS_H
#define LSST_QSERV_QUERY_TABLEALIAS_H


// System headers
#include <map>
#include <vector>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/member.hpp>

// Local headers
#include "query/DbTablePair.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"


// forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class TableRef;
}}}


namespace lsst {
namespace qserv {
namespace query {


template <typename T>
class Aliases {
public:
    Aliases() = default;
    virtual ~Aliases() = default;

    bool set(T const& object, std::string const& alias) {
        for (auto&& aliasInfo : _aliasInfo) {
            if (alias == aliasInfo.alias) {
                return false;
            }
        }
        _aliasInfo.emplace_back(object, alias);
        return true;
    }

    // get an alias for a given object
    std::string get(T const& object) const {
        for (auto&& aliasInfo : _aliasInfo) {
            if (object.compareValue(aliasInfo.object)) { // nptodo probably want a compare functor or something
                return aliasInfo.alias;
            }
        }
        return std::string();
    }

    // get the first-registered object for a given alias
    T& get(std::string const& alias) const {
        for (auto&& aliasInfo : _aliasInfo) {
            if (alias == aliasInfo.alias) {
                return aliasInfo.object;
            }
        }
        return T();
    }

protected:
    struct AliasInfo {
        T object;
        std::string alias;
        AliasInfo(T const& object_, std::string const& alias_) : object(object_), alias(alias_) {}
    };
    std::vector<AliasInfo> _aliasInfo;
};


class SelectListAliases : public Aliases<std::shared_ptr<query::ValueExpr>> {
public:
    SelectListAliases() = default;

    /**
     * @brief Get the alias for a ColumnRef
     *
     * Looks first for an exact match (all fields must match). Then looks for the first "subset" match
     * (for example "objectId" would match "Object.objectId").
     *
     * @param columnRef
     * @return std::string
     */
    std::pair<std::string, std::shared_ptr<query::ValueExpr>>
    getAliasFor(query::ColumnRef const& columnRef) const {
        AliasInfo const* subsetMatch = nullptr;
        for (auto&& aliasInfo : _aliasInfo) {
            auto&& factorOps = aliasInfo.object->getFactorOps();
            if (factorOps.size() == 1) {
                auto&& aliasColumnRef = factorOps[0].factor->getColumnRef();
                if (nullptr != aliasColumnRef) {
                    if (columnRef == *aliasColumnRef) {
                        return std::make_pair(aliasInfo.alias, aliasInfo.object);
                    }
                    if (nullptr == subsetMatch && columnRef.isSubsetOf(aliasColumnRef)) {
                        subsetMatch = &aliasInfo;
                    }
                }
            }
        }
        if (nullptr != subsetMatch) {
            return std::make_pair(subsetMatch->alias, subsetMatch->object);
        }
        return std::make_pair(std::string(), nullptr);
    }
};


// nptodo this can probably get factored into Aliases. Maybe Aliases wants its own file?
class TableAliases {
public:
    TableAliases() = default;

    /// Add an alias for a given db + table
    bool set(std::string const& db, std::string const& table, std::string const& alias);

    /// Add an alias provided by a table ref
    bool set(std::shared_ptr<TableRef> const& tableRef);

    /// Get a db + table for an alias
    DbTablePair get(std::string const& alias) const;

    /// Get an alias for a db + table
    std::string const get(std::string db, std::string table) const;

    /// Get an alias for a DbTablePair (a.k.a. a db + table)
    std::string const get(DbTablePair const& dbTablePair) const;

    // nptodo ? might need to add support for an "ambiguous" lookup (or set?), something to do with the
    // db not being set, or the alias not being set? the impl in TableAliasReverse looks wrong or I don't
    // understand it yet. TBD if we run into this as an issue maybe it was never actually used.

private:
    struct TableAlias {
        std::string alias;
        DbTablePair dbTablePair;
        TableAlias(std::string alias_, DbTablePair dbTablePair_)
        : alias(alias_), dbTablePair(dbTablePair_)
        {}
    };

    struct ByAlias{};
    struct ByDbTablePair{};

    typedef boost::multi_index::multi_index_container<
        TableAlias,
        boost::multi_index::indexed_by<
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<ByAlias>,
                boost::multi_index::member<TableAlias, std::string, &TableAlias::alias>
            >,
            boost::multi_index::ordered_non_unique<
                boost::multi_index::tag<ByDbTablePair>,
                boost::multi_index::member<TableAlias, DbTablePair, &TableAlias::dbTablePair>
            >
        >
    > TableAliasSet;

    TableAliasSet tableAliasSet;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_TABLEALIAS_H
