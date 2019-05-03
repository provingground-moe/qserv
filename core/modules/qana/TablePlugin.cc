// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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

/// \file
/// \brief TablePlugin implementation.
///
/// TablePlugin modifies the parsed query to assign an alias to all the table
/// references in the query from-list. It then rewrites all column references
/// (e.g. in the where clause) to use the appropriate aliases. This allows
/// changing a table reference in a query without editing anything except the
/// from-clause.
///
/// During the concrete query planning phase, TablePlugin determines whether
/// each query proposed for parallel (worker-side) execution is actually
/// parallelizable and how this should be done - that is, it determines whether
/// or not sub-chunking should be used and which director table(s) to use
/// overlap for. Finally, it rewrites table references to use name patterns
/// into which (sub-)chunk numbers can be substituted. This act of substitution
/// is the final step in generating the queries sent out to workers.
///
/// \author Daniel L. Wang, SLAC

// Class header
#include "qana/TablePlugin.h"

// System headers
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qana/QueryMapping.h"
#include "qana/RelationGraph.h"
#include "qana/TableInfoPool.h"

#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/OrderByClause.h"
#include "query/QueryContext.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/TableAlias.h"
#include "query/TableRef.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "util/common.h"
#include "util/IterableFormatter.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.TablePlugin");
}

namespace lsst {
namespace qserv {
namespace qana {


////////////////////////////////////////////////////////////////////////
// fixExprAlias is a functor that acts on ValueExpr objects and
// modifys them in-place, altering table names to use an aliased name
// that is mapped via TableAliases.
// It does not add table qualifiers where none already exist, because
// there is no compelling reason to do so (yet).
////////////////////////////////////////////////////////////////////////
class fixExprAlias {
public:
    fixExprAlias(std::string const& db, query::TableAliases const& tableAliases,
                 query::SelectListAliases const& selectListAliases) :
        _defaultDb(db), _tableAliases(tableAliases), _selectListAliases(selectListAliases) {}

    void operator()(query::ValueExprPtr& valueExpr) {
        if (nullptr == valueExpr)
            return;

        // For each factor in the expr, patch for aliasing:
        query::ValueExpr::FactorOpVector& factorOps = valueExpr->getFactorOps();
        for (auto&& factorOp : factorOps) {
            if (nullptr == factorOp.factor) {
                throw std::logic_error("Bad ValueExpr::FactorOps");
            }
            auto valueFactor = factorOp.factor;
            switch(valueFactor->getType()) {
            case query::ValueFactor::COLUMNREF:
                // check columnref.
                _patchColumnRef(*valueFactor);
                break;
            case query::ValueFactor::FUNCTION:
            case query::ValueFactor::AGGFUNC:
                // recurse for func params (aggfunc is special case of function)
                _patchFuncExpr(*valueFactor->getFuncExpr());
                break;
            case query::ValueFactor::STAR:
                // Patch db/table name if applicable
                _patchStar(*valueFactor);
                break;
            case query::ValueFactor::CONST:
                break; // Constants don't need patching.
            default:
                LOGS(_log, LOG_LVL_WARN, "Unhandled ValueFactor:" << *valueFactor);
                break;
            }
        }
    }

private:
    void _patchColumnRef(query::ValueFactor& valueFactor) {
        auto&& ref = valueFactor.getColumnRef();
        auto&& aliasPair = _selectListAliases.getAliasFor(*ref);
        if (not aliasPair.first.empty()) {
            // replace the ValueExpr in the ValueFactor with one that is aliased, and is the one in the
            // SelectList (nptodo this is right, right?)
            LOGS(_log, LOG_LVL_TRACE, "changing ColumnRef from: " << ref);
            valueFactor.set(aliasPair.second);
            LOGS(_log, LOG_LVL_TRACE, "changed ColumnRef to:    " << ref);
            return;
        }

        // nptodo
        // TableRefs from the FROM list are in the _tableAlias container.
        // All the ColumnRefs need to be patched; replace the embedded TableRefBase with the one from the
        // container, which comes from the from list.
        // But first, ColumnRef needs to use TableRefBase instead of having its own db and table.

        std::string newAlias = _getAlias(ref->getDb(), ref->getTable());
        if (newAlias.empty()) {
            return; // Ignore if no replacement exists.
        }
        // Eliminate db. Replace table with aliased table.
        ref->setDb("");
        ref->setTable(newAlias);
    }

    void _patchFuncExpr(query::FuncExpr& fe) {
        std::for_each(fe.params.begin(), fe.params.end(),
                fixExprAlias(_defaultDb, _tableAliases, _selectListAliases));
    }

    void _patchStar(query::ValueFactor& vt) {
        // nptodo I need to understand why the star was using alias for this.
        // I *think* the need comes from wanting to put a default db, plus table
        // name onto the *. I wonder if the alias was the right way to do that, or
        // if it was a hack that Just Worked?

        // TODO: No support for <db>.<table>.* in framework
        // Only <table>.* is supported.
        std::string newAlias = _getAlias("", vt.getConstVal());
        if (newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.
        else { vt.setConstVal(newAlias); }
    }

    std::string _getAlias(std::string const& db,
                          std::string const& table) {
        return _tableAliases.getAliasFor(db.empty() ? _defaultDb : db, table).first;
    }

    std::string const& _defaultDb;
    query::TableAliases const& _tableAliases;
    query::SelectListAliases const& _selectListAliases;
};


////////////////////////////////////////////////////////////////////////
// TablePlugin implementation
////////////////////////////////////////////////////////////////////////
void
TablePlugin::applyLogical(query::SelectStmt& stmt,
                          query::QueryContext& context) {
    LOGS(_log, LOG_LVL_TRACE, "applyLogical begin:\n\t" << stmt.getQueryTemplate() << "\n\t" << stmt);
    query::FromList& fromList = stmt.getFromList();
    context.collectTopLevelTableSchema(fromList);

    // for each top-level ValueExpr in the SELECT list that does not have an alias, assign an alias that
    // matches the original user query and add that item to the selectListAlias list.
    for (auto& valueExpr : *(stmt.getSelectList().getValueExprList())) {
        if (not valueExpr->hasAlias() && not valueExpr->isStar())
            valueExpr->setAlias(valueExpr->sqlFragment(false));
        if (not context.selectListAliases.set(valueExpr, valueExpr->getAlias())) {
            throw std::logic_error("could not set alias for " + valueExpr->sqlFragment(false));
        }
    }
    // nptodo make each of the value exprs in the later clauses (where, group by, etc) refer to the alias
    // of the ValueExpr (if we don't do this already?)
    // nptodo the alias _may_ need disambiguation, but really only if the user may have used an alias
    // that matches a non-aliased ValueExpr

    // update the "resolver tables" (which is to say; the tables used in the FROM list) in the context.
    query::DbTableVector v = fromList.computeResolverTables();
    LOGS(_log, LOG_LVL_TRACE, "changing resolver tables from " << util::printable(context.resolverTables) <<
            " to " << util::printable(v));
    context.resolverTables.swap(v);

    // make sure the TableRefs in the from list are all completetly populated (db AND table)
    query::TableRefList& fromListTableRefs = fromList.getTableRefList();
    for (auto&& tableRef : fromListTableRefs) {
        tableRef->verifyPopulated(context.defaultDb);
    }

    // update the dominant db in the context ("dominant" is not the same as the default db)
    if (fromListTableRefs.size() > 0) {
        context.dominantDb = fromListTableRefs[0]->getDb();
        _dominantDb = context.dominantDb;
    }

    // Add aliases to all table references in the from-list (if
    // they don't exist already) and then patch the other clauses so
    // that they refer to the aliases.
    //
    // The purpose of this is to confine table name references to the
    // from-list so that the later table-name substitution is confined
    // to modifying the from-list.
    //
    // Note also that this must happen after the default db context
    // has been filled in, or alias lookups will be incorrect.

    std::function<void(query::TableRef::Ptr)> aliasSetter = [&] (query::TableRef::Ptr tableRef) {
        if (nullptr == tableRef) {
            return;
        }
        if (not tableRef->hasAlias()) {
            tableRef->setAlias("`" + tableRef->getDb() + "." + tableRef->getTable() + "`");
        }
        if (not context.tableAliases.set(tableRef, tableRef->getAlias())) {
            throw std::logic_error("could not set alias for " + tableRef->sqlFragment());
        }
        for (auto&& joinRef : tableRef->getJoins()){
            aliasSetter(joinRef->getRight());
        }
    };
    std::for_each(fromListTableRefs.begin(), fromListTableRefs.end(), aliasSetter);

    // make the TableRef ptrs in the SELECT list point to TableRefs in the FROM list
    query::SelectList& selectlist = stmt.getSelectList();
    auto&& selectListValueExprs = selectlist.getValueExprList();
    for (auto&& valueExpr : *selectListValueExprs) {
        std::vector<std::shared_ptr<query::ColumnRef>> columnRefs;
        valueExpr->findColumnRefs(columnRefs);
        for (auto&& columnRef : columnRefs) {
            std::shared_ptr<query::TableRefBase>& tableRef = columnRef->getTableRef();
            auto&& tableRefMatch = context.tableAliases.getTableRefMatch(tableRef);
            if (nullptr != tableRefMatch) {
                tableRef = tableRefMatch;
            }
        }
    }

    // nptodo implement this using using the TableAliases class.
    // // Patch table references in the select list,
    // LOGS(_log, LOG_LVL_TRACE, "SelectList:");
    // query::SelectList& selectlist = stmt.getSelectList();
    // query::ValueExprPtrVector& exprList = *selectlist.getValueExprList();
    // std::for_each(exprList.begin(), exprList.end(), fixExprAlias(
    //     context.defaultDb, context.tableAliases, context.selectListAliases));

    // order by, group by, and having need to be in the select list and identified the same way.
    // where and from will not be returned and do not require same-identification (but do downstream
    // plugins require it?)
    // order by clause,
    if (stmt.hasOrderBy()) {
        query::ValueExprPtrVector valueExprs;
        stmt.getOrderBy().findValueExprs(valueExprs);
        for (auto&& valueExpr : valueExprs) {
            auto&& valueExprMatch = context.selectListAliases.getValueExprMatch(valueExpr);
            if (nullptr != valueExprMatch) {
                valueExpr = valueExprMatch;
            }
        }
    }




    // where clause,
    LOGS(_log, LOG_LVL_TRACE, "WhereClause:");
    if (stmt.hasWhereClause()) {
        query::ValueExprPtrVector e;
        stmt.getWhereClause().findValueExprs(e);
        std::for_each(e.begin(), e.end(), fixExprAlias(
            context.defaultDb, context.tableAliases, context.selectListAliases));
    }
    // group by clause,
    LOGS(_log, LOG_LVL_TRACE, "GroupByClause:");
    if (stmt.hasGroupBy()) {
        query::ValueExprPtrVector e;
        stmt.getGroupBy().findValueExprs(e);
        std::for_each(e.begin(), e.end(), fixExprAlias(
            context.defaultDb, context.tableAliases, context.selectListAliases));
    }
    // having clause,
    LOGS(_log, LOG_LVL_TRACE, "HavingClause:");
    if (stmt.hasHaving()) {
        query::ValueExprPtrVector e;
        stmt.getHaving().findValueExprs(e);
        std::for_each(e.begin(), e.end(), fixExprAlias(
            context.defaultDb, context.tableAliases, context.selectListAliases));
    }
    LOGS(_log, LOG_LVL_TRACE, "OrderByClause:");
    // order by clause,
    if (stmt.hasOrderBy()) {
        query::ValueExprPtrVector e;
        stmt.getOrderBy().findValueExprs(e);
        std::for_each(e.begin(), e.end(), fixExprAlias(
            context.defaultDb, context.tableAliases, context.selectListAliases));
    }
    LOGS(_log, LOG_LVL_TRACE, "OnClauses of Join:");
    // and in the on clauses of all join specifications.
    for (auto&& tableRef : fromListTableRefs) {
        for (auto&& joinRef : tableRef->getJoins()) {
            auto&& joinSpec = joinRef->getSpec();
            if (joinSpec) {
                fixExprAlias fix(context.defaultDb, context.tableAliases, context.selectListAliases);
                // A column name in a using clause should be unqualified,
                // so only patch on clauses.
                auto&& onBoolTerm = joinSpec->getOn();
                if (onBoolTerm) {
                    query::ValueExprPtrVector valueExprs;
                    onBoolTerm->findValueExprs(valueExprs);
                    std::for_each(valueExprs.begin(), valueExprs.end(), fix);
                }
            }
        }
    }
    LOGS(_log, LOG_LVL_TRACE, "applyLogical end:\n\t" << stmt.getQueryTemplate() << "\n\t" << stmt);
}

void
TablePlugin::applyPhysical(QueryPlugin::Plan& p,
                           query::QueryContext& context)
{
    TableInfoPool pool(context.defaultDb, *context.css);
    if (!context.queryMapping) {
        context.queryMapping = std::make_shared<QueryMapping>();
    }

    if ((not p.stmtParallel.empty()) && p.stmtParallel.front() != nullptr) {
        p.stmtPreFlight = p.stmtParallel.front()->clone();
        LOGS(_log, LOG_LVL_TRACE, "set local worker query:" << p.stmtPreFlight->getQueryTemplate().sqlFragment());
    }

    // Process each entry in the parallel select statement set.
    typedef SelectStmtPtrVector::iterator Iter;
    SelectStmtPtrVector newList;
    for(Iter i=p.stmtParallel.begin(), e=p.stmtParallel.end(); i != e; ++i) {
        RelationGraph g(**i, pool);
        g.rewrite(newList, *context.queryMapping);
    }
    p.dominantDb = _dominantDb;
    p.stmtParallel.swap(newList);
}

}}} // namespace lsst::qserv::qana
