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


#ifndef LSST_QSERV_QUERY_BOOLFACTORTERM_H
#define LSST_QSERV_QUERY_BOOLFACTORTERM_H


// System headers
#include <memory>
#include <ostream>
#include <vector>

// Third-party headers
#include "boost/iterator_adaptors.hpp"

// Qserv headers
#include "query/typedefs.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryTemplate;
    class ColumnRef;
    class ValueExpr;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// BoolFactorTerm is a term in a in a BoolFactor
class BoolFactorTerm {
public:
    typedef std::shared_ptr<BoolFactorTerm> Ptr;
    typedef std::vector<Ptr> PtrVector;

    virtual ~BoolFactorTerm() {}

    /// Make a deep copy of this term.
    virtual Ptr clone() const = 0;

    /// Make a shallow copy of this term.
    virtual Ptr copySyntax() const = 0;

    /// Write a human-readable version of this instance to the ostream for debug output.
    virtual std::ostream& putStream(std::ostream& os) const = 0;

    /// Serialze this instance as SQL to the QueryTemplate.
    virtual void renderTo(QueryTemplate& qt) const = 0;

    /// Get a vector of the ValueExprs this contains.
    virtual void findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const = 0;

    /// Get a vector of references to pointers to the ValueExprs this contains.
    virtual void findValueExprRefs(ValueExprPtrRefVector& vector) = 0;

    /// Get a vector of the ColumnRefs this contains.
    virtual void findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const = 0;

    virtual bool operator==(BoolFactorTerm const& rhs) const = 0;

protected:
    friend std::ostream& operator<<(std::ostream& os, BoolFactorTerm const& bft) {
        bft.dbgPrint(os);
        return os;
    }

    friend std::ostream& operator<<(std::ostream& os, BoolFactorTerm const* bft) {
        (nullptr == bft) ? os << "nullptr" : os << *bft;
        return os;
    }

    /// Serialize this instance to os for debug output.
    virtual void dbgPrint(std::ostream& os) const = 0;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_BOOLFACTORTERM_H
