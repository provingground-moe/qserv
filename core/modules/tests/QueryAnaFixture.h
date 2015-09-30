/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @brief Test functions and structures used in QueryAnalysis tests
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */


#ifndef LSST_QSERV_TESTS_QUERYANAFIXTURE_H
#define LSST_QSERV_TESTS_QUERYANAFIXTURE_H

// System headers
#include <iostream>
#include <memory>
#include <string>
#include <sstream>

#include "css/CssAccess.h"
#include "qproc/QuerySession.h"
#include "qproc/testMap.h" // Generated by scons action from testMap.kvmap
#include "query/Constraint.h"
#include "util/IterableFormatter.h"
#include "tests/QueryAnaHelper.h"

namespace lsst {
namespace qserv {
namespace tests {

struct QueryAnaFixture {

    QueryAnaFixture() {
        qsTest.cfgNum = 0;
        qsTest.defaultDb = "LSST";
        // To learn how to dump the map, see qserv/core/css/KvInterfaceImplMem.cc
        // Use admin/examples/testMap_generateMap
        std::string mapBuffer(reinterpret_cast<char const*>(testMap),
                              testMap_length);
        std::istringstream mapStream(mapBuffer);
        std::string emptyChunkPath(".");
        qsTest.css = css::CssAccess::makeMemCss(mapStream, emptyChunkPath);
    };

    qproc::QuerySession::Test qsTest;
    QueryAnaHelper queryAnaHelper;
};

}}} // namespace lsst::qserv::tests


#endif // LSST_QSERV_TESTS_QUERYANAFIXTURE_H
