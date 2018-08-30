// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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


#include "Identifier.h"


const char QUOTE_CHAR('"');


namespace lsst {
namespace qserv {
namespace query {


Identifier::Identifier(std::string const & val)
: _val(val)
, _didRemoveQuotes(false)
{
    _unquoteVal();
}


const std::string Identifier::get(tQuote quoting) const {
    if ((UNMODIFIED == quoting && false == _didRemoveQuotes) ||
        NO_QUOTES == quoting) {
        return _val;
    }
    std::string ret = QUOTE_CHAR + _val + QUOTE_CHAR;
    return ret;
}


void Identifier::set(std::string const & val) {
    _val = val;
    _unquoteVal();
}


bool Identifier::empty() const {
    return _val.empty();
}


void Identifier::operator=(Identifier const & rhs) {
    set(rhs.get(Identifier::UNMODIFIED));
}


void Identifier::operator=(std::string const & val) {
    set(val);
}


bool Identifier::operator<(Identifier const & rhs) {
    return _val < rhs._val;
}


bool Identifier::operator==(Identifier const & rhs) {
    return _val == rhs._val;
}


void Identifier::_unquoteVal() {
    // This implementation assumes that the quote character can only be one kind of character, specified by
    // QUOTE_CHAR. This could be expanded to other characters (e.g. single and double quotes) as needed.
    if (_val.find(QUOTE_CHAR) == 0) {
        if (_val.rfind(QUOTE_CHAR) != _val.length()-1) {
            // throw: the value starts with a quote, it must end with a quote.
        }
        // TODO test for single quotes inside the val; this is not allowed - they must be double quotes to escape.
        _val.erase(_val.begin());
        _val.erase(--(_val.end()));
        _didRemoveQuotes = true;
    } else {
        _didRemoveQuotes = false;
    }
}


}}} // namespace lsst::qserv::query
