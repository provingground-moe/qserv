// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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


#ifndef LSST_QSERV_QUERY_IDENTIFIER_H
#define LSST_QSERV_QUERY_IDENTIFIER_H


#include <memory>
#include <string>

namespace lsst {
namespace qserv {
namespace query {


// Identifier is a class to contain and manipulate sql identifier strings
class Identifier {
public:
    enum tQuote {
        UNMODIFIED, NO_QUOTES, WITH_QUOTES
    };

    typedef std::shared_ptr<Identifier> Ptr;
    typedef std::shared_ptr<const Identifier> ConstPtr;

    Identifier() : _val(""), _didRemoveQuotes(false) {}
    Identifier(std::string const & val);

    const std::string get(tQuote quoting=NO_QUOTES) const;

    void set(std::string const & val);

    bool empty() const;

    void operator=(Identifier const & rhs);
    void operator=(std::string const & val);
    bool operator<(Identifier const & rhs);
    bool operator==(Identifier const & rhs);

private:
    /// Modifies the stored value, if needed, to not contain quotes.
    /// Stores whether the value originally had quotes in _didRemoveQuotes
    void _unquoteVal();

    std::string _val;
    bool _didRemoveQuotes; // true if quotes were removed from the identifier, else false.
};


}}} // namespace lsst::qserv::query


#endif // LSST_QSERV_QUERY_IDENTIFIER_H
