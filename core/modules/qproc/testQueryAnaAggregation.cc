// -*- LSST-C++ -*-
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
  * @brief Test C++ parsing and query analysis logic for select expressions
  * with an "ORDER BY" clause.
  *
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */

// System headers
#include <string>

// Third-party headers
#include "boost/assign/list_of.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaAggregation
#include "boost/test/included/unit_test.hpp"

// LSST headers

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "parser/SelectParser.h"
#include "qproc/QuerySession.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "sql/MockSql.h"
#include "tests/QueryAnaFixture.h"

using boost::assign::list_of;
using boost::assign::map_list_of;
using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::parser::SelectParser;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::SelectStmt;
using lsst::qserv::query::QueryContext;
using lsst::qserv::sql::MockSql;
using lsst::qserv::tests::QueryAnaFixture;
////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(Aggregate, QueryAnaFixture)

BOOST_AUTO_TEST_CASE(Aggregate) {
    std::string stmt = "select sum(pm_declErr),chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";

    std::string expPar = "SELECT sum(`LSST.Object`.pm_declErr) AS `QS1_SUM`,"
                            "`LSST.Object`.chunkId AS `chunkId`,"
                            "COUNT(`LSST.Object`.bMagF2) AS `QS2_COUNT`,"
                            "SUM(`LSST.Object`.bMagF2) AS `QS3_SUM` "
                         "FROM LSST.Object_100 AS `LSST.Object` "
                         "WHERE `LSST.Object`.bMagF>20.0 "
                         "GROUP BY `chunkId`";
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(
        map_list_of("Object", list_of("pm_declErr")("chunkId")("bMagF2")("bMagF"))));
    queryAnaHelper.buildQuerySession(qsTest, stmt);
    auto& qs = queryAnaHelper.querySession;
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    SelectStmt const& ss = qs->getStmt();
    BOOST_TEST_MESSAGE("produced stmt:" << qs->getStmt());

    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());
    BOOST_REQUIRE(ss.hasGroupBy());

    std::string parallel = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(expPar, parallel);
}

BOOST_AUTO_TEST_CASE(Avg) {
    std::string stmt = "select chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0;";
    std::string expPar = "SELECT `LSST.Object`.chunkId AS `chunkId`,COUNT(`LSST.Object`.bMagF2) AS `QS1_COUNT`,SUM(`LSST.Object`.bMagF2) AS `QS2_SUM` FROM LSST.Object_100 AS `LSST.Object` WHERE `LSST.Object`.bMagF>20.0";
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(
        map_list_of("Object", list_of("chunkId")("bMagF2")("bMagF"))));
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();

    BOOST_CHECK(context);
    BOOST_CHECK(!context->restrictors);
    BOOST_CHECK(context->hasChunks());
    BOOST_CHECK(!context->hasSubChunks());

    std::string parallel = queryAnaHelper.buildFirstParallelQuery();
    BOOST_CHECK_EQUAL(expPar, parallel);
}

BOOST_AUTO_TEST_SUITE_END()
