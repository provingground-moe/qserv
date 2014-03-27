/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
  * @file Facade.h
  *
  * @brief A facade to the Central State System used by all Qserv core modules.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_CSS_FACADE_H
#define LSST_QSERV_CSS_FACADE_H

// Standard
#include <iostream>
#include <string>
#include <vector>

// Boost
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>     // for mutex

// Local imports
#include "cssInterface.h"
#include "IntPair.h"

namespace lsst {
namespace qserv {
namespace css {

// Forward
class CssInterface;
    
/** The class stores Qserv-specific metadata and state information from the
    Central State System.
 */

class Facade {
public:
    Facade(std::string const&);
    Facade(std::string const&, std::string const&);
    Facade(std::string const&, bool);
    ~Facade();

    // accessors
    bool checkIfContainsDb(std::string const&);
    bool checkIfContainsTable(std::string const&, std::string const&);
    bool checkIfTableIsChunked(std::string const&, std::string const&);
    bool checkIfTableIsSubChunked(std::string const&, std::string const&);
    std::vector<std::string> getAllowedDbs();
    std::vector<std::string> getChunkedTables(std::string const&);
    std::vector<std::string> getSubChunkedTables(std::string const&);
    std::vector<std::string> getPartitionCols(std::string const&, 
                                              std::string const&);
    int getChunkLevel(std::string const&, std::string const&);
    std::string getKeyColumn(std::string const&, std::string const&);
    IntPair getDbStriping(std::string const&);

private:
    void _validateDbExists(std::string const&);
    void _validateTbExists(std::string const&, std::string const&);
    void _validateDbTbExists(std::string const&, std::string const&);
    bool _checkIfContainsTable(std::string const&, std::string const&);
    bool _checkIfTableIsChunked(std::string const&, std::string const&);
    bool _checkIfTableIsSubChunked(std::string const&, std::string const&);
    int _getIntValue(std::string const&);

private:
    CssInterface* _cssI;
    std::string _prefix; // optional prefix, for isolating tests from production
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_FACADE_H
