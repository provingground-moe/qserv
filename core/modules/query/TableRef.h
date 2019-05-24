// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2017 LSST Corporation.
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
  * @brief Declarations for TableRefN and subclasses SimpleTableN and JoinRefN
  *
  * @author Daniel L. Wang, SLAC
  */


#ifndef LSST_QSERV_QUERY_TABLEREF_H
#define LSST_QSERV_QUERY_TABLEREF_H


// System headers
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// Local headers
#include "query/DbTablePair.h"
#include "query/QueryTemplate.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryTemplate;
    class JoinSpec;
    class JoinRef;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


typedef std::vector<std::shared_ptr<JoinRef> > JoinRefPtrVector;


/// TableRefN is a parsed table reference node
// table_ref :
//   table_ref_aux (options{greedy=true;}:qualified_join | cross_join)*
// table_ref_aux :
//   (n:table_name | /*derived_table*/q:table_subquery) ((as:"as")? c:correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)?
class TableRef {
public:
    typedef std::shared_ptr<TableRef> Ptr;
    typedef std::shared_ptr<TableRef const> CPtr;

    /**
     * @brief Construct a new Table Ref object
     *
     * If db is popuated, table must also be populated.
     *
     * @throws logic_error if db is populated and table is not populated.
     */
    TableRef(std::string const& db_, std::string const& table_, std::string const& alias_);

    virtual ~TableRef() {}

    std::ostream& putStream(std::ostream& os) const;
    void putTemplate(QueryTemplate& qt) const;
    std::string sqlFragment() const;

    bool isSimple() const { return _joinRefs.empty(); }
    std::string const& getDb() const { return _db; }
    std::string const& getTable() const { return _table; }
    std::string const& getAlias() const { return _alias; }
    bool hasAlias() const { return not _alias.empty(); }
    JoinRefPtrVector const& getJoins() const { return _joinRefs; }
    /// Get all the db+table names used by this TableRef and all of its joins.
    void getRelatedDbTableInfo(std::vector<DbTablePair>& dbTablePairs) const;

    bool hasDb() const;
    bool hasTable() const;
    bool hasAlias() const;

    // Modifiers
    void setAlias(std::string const& alias);
    void setDb(std::string const& db);
    void setTable(std::string const& table);
    JoinRefPtrVector& getJoins() { return _joinRefs; }
    void addJoin(std::shared_ptr<JoinRef> r);
    void addJoins(const JoinRefPtrVector& r);

    class Func {
    public:
        virtual ~Func() {}
        virtual void operator()(TableRef& t)=0;
    };
    class FuncC {
     public:
        virtual ~FuncC() {}
        virtual void operator()(TableRef const& t)=0;
    };
    void apply(Func& f);
    void apply(FuncC& f) const;

    /**
     * @brief Verify the table is set and set a database if one is not set. Recurses to all join refs.
     *
     * @throws If an empty string is passed for default then this will throw if the value is not set in the
     *         instance.
     *
     * @param defaultDb the default database to assign, or an empty string for no default.
     */
    void verifyPopulated(std::string const& defaultDb=std::string());

    bool isSubsetOf(TableRef const& rhs) const override { throw logic_error("not supported"); };

    TableRef::Ptr clone() const;

    // Returns true if the fields in rhs have the same values as the fields in this, without considering
    // unpopulated fields. This can be used to determine if this could refer to the same table as a
    // more-populated rhs.
    // ColumnRef.
    bool isSubsetOf(std::shared_ptr<TableRef> const& rhs);

    bool operator==(const TableRef& rhs) const;

    /**
     * @brief less than operator for TableRef. DOES NOT SUPPORT JOIN.
     *
     * Will throw a logic_error if used on a TableRef that has joinRefs.
     *
     * @param rhs
     * @return true
     * @return false
     */
    bool operator<(const TableRef& rhs) const;

    class render;
private:
    friend std::ostream& operator<<(std::ostream& os, TableRef const& refN);
    friend std::ostream& operator<<(std::ostream& os, TableRef const* refN);

    std::string _alias;
    std::string _db;
    std::string _table;
    JoinRefPtrVector _joinRefs;
};


class TableRef::render {
public:
    render(QueryTemplate& qt) : _qt(qt), _count(0) {}
    void applyToQT(TableRef const& trn);
    inline void applyToQT(TableRef::Ptr const trn) {
        if(trn != nullptr) applyToQT(*trn);
    }
    QueryTemplate& _qt;
    int _count;
};


// Containers
typedef std::vector<TableRef::Ptr> TableRefList;
typedef std::shared_ptr<TableRefList> TableRefListPtr;


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_TABLEREF_H
