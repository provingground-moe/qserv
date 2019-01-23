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


// Class header
#include "DisjunctiveNormalFormPlugin.h"

// System headers
#include <memory>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/AndTerm.h"
#include "query/BoolFactor.h"
#include "query/BoolFactorTerm.h"
#include "query/BoolTermFactor.h"
#include "query/OrTerm.h"
#include "query/CompPredicate.h"
#include "query/SelectStmt.h"
#include "query/QueryContext.h"
#include "query/WhereClause.h"


namespace lsst {
namespace qserv {
namespace qana {


namespace {


LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.DisjunctiveNormalFormPlugin");


void walkAndApply(std::shared_ptr<query::OrTerm> const& orTerm);


void walkAndApply(std::shared_ptr<query::BoolTermFactor> const& boolTermFactor) {
    LOGS(_log, LOG_LVL_TRACE, *boolTermFactor);
    auto orTerm = std::dynamic_pointer_cast<query::OrTerm>(boolTermFactor->_term);
    if (nullptr == orTerm) {
        orTerm = std::make_shared<query::OrTerm>(boolTermFactor->_term);
        boolTermFactor->_term = orTerm;
    }
    walkAndApply(orTerm);
}


void walkAndApply(std::shared_ptr<query::BoolTerm> const& boolTerm) {
    LOGS(_log, LOG_LVL_TRACE, *boolTerm);
    auto boolFactor = std::dynamic_pointer_cast<query::BoolFactor>(boolTerm);
    if (nullptr != boolFactor) {
        // BoolFactor has _terms, a list of BoolFactorTerm
        for (auto boolFactorTermItr = boolFactor->_terms.begin();
                boolFactorTermItr != boolFactor->_terms.end();
                ++boolFactorTermItr) {
            auto boolTermFactor = std::dynamic_pointer_cast<query::BoolTermFactor>(*boolFactorTermItr);
            if (nullptr != boolTermFactor) {
                walkAndApply(boolTermFactor);
            } else {
                // std::vector<std::shared_ptr<query::ValueExpr>> valueExprs;
                // (*boolFactorTermItr)->findValueExprs(valueExprs);
                // if (valueExprs.size() != 0) {
                //     auto boolFactor = std::make_shared<query::BoolFactor>(*boolFactorTermItr);
                //     auto andTerm = std::make_shared<query::AndTerm>(boolFactor);
                //     auto orTerm = std::make_shared<query::OrTerm>(andTerm);
                //     auto boolTermFactor = std::make_shared<query::BoolTermFactor>(orTerm);
                //     *boolFactorTermItr = boolTermFactor;
                // }
                // InPredicate has ValueExprs but does not get nested.
                if (nullptr != std::dynamic_pointer_cast<query::CompPredicate>(*boolFactorTermItr)) {
                    auto boolFactor = std::make_shared<query::BoolFactor>(*boolFactorTermItr);
                    auto andTerm = std::make_shared<query::AndTerm>(boolFactor);
                    auto orTerm = std::make_shared<query::OrTerm>(andTerm);
                    auto boolTermFactor = std::make_shared<query::BoolTermFactor>(orTerm);
                    *boolFactorTermItr = boolTermFactor;
                }

            }
        }
        return;
    }

    auto orTerm = std::dynamic_pointer_cast<query::OrTerm>(boolTerm);
    if (nullptr != orTerm) {
        walkAndApply(orTerm);
    }

    auto logicalTerm = std::dynamic_pointer_cast<query::LogicalTerm>(boolTerm);
    if (nullptr != logicalTerm) {
        for (auto& boolTerm : logicalTerm->_terms) {
            walkAndApply(boolTerm);
        }
        return;
    }

    // PassListTerm has _terms, but all are supposed to be ignorable.
}


void walkAndApply(std::shared_ptr<query::OrTerm> const& orTerm) {
    LOGS(_log, LOG_LVL_TRACE, *orTerm);
    for (auto termItr = orTerm->_terms.begin(); termItr != orTerm->_terms.end(); ++termItr) {
        walkAndApply(*termItr);
        if (nullptr == std::dynamic_pointer_cast<query::AndTerm>(*termItr)) {
            *termItr = std::make_shared<query::AndTerm>(*termItr);
        }
    }
}


} // end of namespace


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
        walkAndApply(orTerm);
        return;
    }
    auto andTerm = std::dynamic_pointer_cast<query::AndTerm>(rootTerm);
    if (nullptr != andTerm) {
        walkAndApply(andTerm);
        auto orTerm = std::make_shared<query::OrTerm>(andTerm);
        whereClause.setRootTerm(orTerm);
        return;
    }
    walkAndApply(rootTerm);
    whereClause.setRootTerm(std::make_shared<query::OrTerm>(std::make_shared<query::AndTerm>(rootTerm)));
}


}}} // namespace lsst::qserv::qana
