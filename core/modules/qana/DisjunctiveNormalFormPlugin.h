// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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
#ifndef LSST_QSERV_QANA_DISJUNCTIVENORMALFORMPLUGIN_H
#define LSST_QSERV_QANA_DISJUNCTIVENORMALFORMPLUGIN_H


// Local headers
#include "qana/QueryPlugin.h"


namespace lsst {
namespace qserv {
namespace query {
    class LogicalTerm;
    class SelectStmt;
    class QueryContext;
}
namespace qana {


/// DisjunctiveNormalFormPlugin rewrites the IR to be in disjunctive normal form in the WHERE clause.
class DisjunctiveNormalFormPlugin : public QueryPlugin {
public:
    virtual ~DisjunctiveNormalFormPlugin() {}

    /// The QueryContext is not used by this plugin, so this form of `applyLogical` does the actual work and
    /// does not take a QueryContext. It is useful for situations that do not have a QueryContext, e.g. unit
    /// testing.
    void applyLogical(query::SelectStmt& stmt);

    /// Apply the plugin's actions to the parsed, but not planned query
    void applyLogical(query::SelectStmt& stmt, query::QueryContext&) override { applyLogical(stmt); }
};

// if it's not a logicalTerm, nothing to do
// if is's an OrTerm, it's children must be AndTerms.
// if it's an AndTerm, its children must be searched for OrTerms.


}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_DISJUNCTIVENORMALFORMPLUGIN_H
