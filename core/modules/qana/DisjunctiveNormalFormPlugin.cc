// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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

#include "DisjunctiveNormalFormPlugin.h"

#include "query/AndTerm.h"
#include "query/OrTerm.h"
#include "query/SelectStmt.h"
#include "query/QueryContext.h"
#include "query/WhereClause.h"


namespace lsst {
namespace qserv {
namespace qana {


void DisjunctiveNormalFormPlugin::applyLogical(query::SelectStmt& stmt) {
    if (not stmt.hasWhereClause()) {
        return;
    }
    auto whereClause = stmt.getWhereClause();
    auto rootTerm = whereClause.getRootTerm();
    if (rootTerm == nullptr) {
        return;
    }
    auto orTerm = std::dynamic_pointer_cast<query::OrTerm>(rootTerm);
    if (nullptr != orTerm) {
        orTerm->toDisjunctiveNormalForm();
        return;
    }
    auto andTerm = std::dynamic_pointer_cast<query::AndTerm>(rootTerm);
    if (nullptr != andTerm) {
        andTerm->toDisjunctiveNormalForm();
        whereClause.setRootTerm(std::make_shared<query::OrTerm>(andTerm));
        return;
    }
    rootTerm->toDisjunctiveNormalForm();
    whereClause.setRootTerm(std::make_shared<query::OrTerm>(std::make_shared<query::AndTerm>(rootTerm)));
}


}}} // namespace lsst::qserv::qana
