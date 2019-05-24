// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 LSST Corporation.
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
  * @author Daniel L. Wang, SLAC
  */


// ValueFactor is a term in a ValueExpr's "term (term_op term)*" phrase


#ifndef LSST_QSERV_QUERY_VALUEFACTOR_H
#define LSST_QSERV_QUERY_VALUEFACTOR_H

// System headers
#include <memory>
#include <string>

// Local headers
#include "query/ColumnRef.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryTemplate;
    class FuncExpr;
    class ValueExpr; // To support nested expressions.
    class ValueFactor;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


typedef std::shared_ptr<ValueFactor> ValueFactorPtr;


/// ValueFactor is some kind of value that can exist in a column. It can be
/// logical (i.e. a column name) or physical (a constant number or value).
class ValueFactor {
public:
    enum Type { NONE, COLUMNREF, FUNCTION, AGGFUNC, STAR, CONST, EXPR };
    static std::string getTypeString(Type t) {
        switch(t) {
        case NONE:      return "NONE";
        case COLUMNREF: return "COLUMNREF";
        case FUNCTION:  return "FUNCTION";
        case AGGFUNC:   return "AGGFUNC";
        case STAR:      return "STAR";
        case CONST:     return "CONST";
        case EXPR:      return "EXPR";
        }
        return "unknown";
    }

    ValueFactor() = default;

    // Construct a ColumnRef ValueFactor.
    ValueFactor(std::shared_ptr<ColumnRef> const& columnRef);

    // Construct a FuncExpr ValueFactor.
    ValueFactor(std::shared_ptr<FuncExpr> const& funcExpr);

    // Construct a "const" (string value) ValueFactor.
    ValueFactor(std::string const& constVal);

    // May need non-const, otherwise, need new construction
    std::shared_ptr<ColumnRef const> getColumnRef() const { return _columnRef; }
    std::shared_ptr<ColumnRef> getColumnRef() { return _columnRef; }
    std::shared_ptr<FuncExpr const> getFuncExpr() const { return _funcExpr; }
    std::shared_ptr<FuncExpr> getFuncExpr() { return _funcExpr; }
    std::shared_ptr<ValueExpr const> getExpr() const { return _valueExpr; }
    std::shared_ptr<ValueExpr> getExpr() { return _valueExpr; }
    Type getType() const { return _type; }

    std::string const& getConstVal() const { return _constVal; }
    void setConstVal(std::string const& a) { _constVal = a; }
    bool isConstVal() const { return not _constVal.empty(); }

    void findColumnRefs(ColumnRef::Vector& vector) const;

    ValueFactorPtr clone() const;

    static ValueFactorPtr newColumnRefFactor(std::shared_ptr<ColumnRef const> cr);
    static ValueFactorPtr newStarFactor(std::string const& table);
    static ValueFactorPtr newAggFactor(std::shared_ptr<FuncExpr> const& fe);
    static ValueFactorPtr newFuncFactor(std::shared_ptr<FuncExpr> const& fe);
    /// Makes a new ValueFactor with type=const and value=alnum
    /// Any trailing whitespace is removed.
    static ValueFactorPtr newConstFactor(std::string const& alnum);
    static ValueFactorPtr newExprFactor(std::shared_ptr<ValueExpr> const& ve);

    friend std::ostream& operator<<(std::ostream& os, ValueFactor const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueFactor const* ve);

    class render;
    friend class render;

    bool operator==(const ValueFactor& rhs) const;

    /// Assign a new ValueExpr to this object, any previous parameters will be cleared.
    void set(std::shared_ptr<ValueExpr> const& valueExpr);

private:

    /// Clear this object - drop all its parameters
    void _reset();

    Type _type{NONE};
    std::shared_ptr<ColumnRef> _columnRef;
    std::shared_ptr<FuncExpr> _funcExpr;
    std::shared_ptr<ValueExpr> _valueExpr;
    std::string _constVal;  // formerly named "tablestar"
                            // seems to often contain a string representation of a number. Can also contain
                            // `*` (presumably as in `SELECT *`. It would probably be good to factor it so
                            // so that `constVal` is exclusivly a string number and `*` is a different field.
};


class ValueFactor::render {
public:
    render(QueryTemplate& qt) : _qt(qt) {}
    void applyToQT(ValueFactor const& ve);
    void applyToQT(ValueFactor const* vep) {
        if(vep) applyToQT(*vep);
    }
    void applyToQT(std::shared_ptr<ValueFactor> const& vep) {
        applyToQT(vep.get());
    }
    QueryTemplate& _qt;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_VALUEFACTOR_H
