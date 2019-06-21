// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2017 AURA/LSST.
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
#include <memory>
#include <string>
#include <vector>

// Third-party headers
#include "boost/assign/list_of.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaIn
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "parser/SelectParser.h"
#include "query/QsRestrictor.h"
#include "query/QueryContext.h"
#include "sql/MockSql.h"
#include "tests/QueryAnaFixture.h"

using boost::assign::list_of;
using boost::assign::map_list_of;
using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::parser::SelectParser;
using lsst::qserv::qproc::ChunkQuerySpec;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::QsRestrictor;
using lsst::qserv::query::QueryContext;
using lsst::qserv::sql::MockSql;
using lsst::qserv::tests::QueryAnaFixture;

////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(OrderBy, QueryAnaFixture)

BOOST_AUTO_TEST_CASE(SecondaryIndex) {
    std::string stmt = "select * from Object where objectIdObjTest in (2,3145,9999);";
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(
        map_list_of("Object", list_of("objectIdObjTest"))));
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1U);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST", "Object", "objectIdObjTest", "2", "3145", "9999"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+6);
}

BOOST_AUTO_TEST_CASE(CountIn) {
    std::string stmt = "select COUNT(*) AS N FROM Source WHERE objectId IN(386950783579546, 386942193651348);";
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(
        map_list_of("Source", list_of("objectId"))));
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::string expectedParallel = "SELECT COUNT(*) AS `QS1_COUNT` FROM LSST.Source_100 AS `LSST.Source` "
                                   "WHERE `LSST.Source`.objectId IN(386950783579546,386942193651348)";
    std::string expectedMerge = "SELECT SUM(QS1_COUNT) AS `N`";
    auto queries = queryAnaHelper.getInternalQueries(qsTest, stmt);
    BOOST_CHECK_EQUAL(queries[0], expectedParallel);
    BOOST_CHECK_EQUAL(queries[1], expectedMerge);
    for(auto i = queryAnaHelper.querySession->cQueryBegin(), e = queryAnaHelper.querySession->cQueryEnd();
        i != e; ++i) {
        auto queryTemplates = queryAnaHelper.querySession->makeQueryTemplates();
        auto cs = queryAnaHelper.querySession->buildChunkQuerySpec(queryTemplates, *i);
        LOGS_DEBUG("Chunk spec: " << *cs);
    }
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_CHECK(context->hasChunks());
}

BOOST_AUTO_TEST_CASE(RestrictorObjectIdAlias) {
    std::string stmt = "select * from Object as o1 where objectIdObjTest IN (2,3145,9999);";
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(
        map_list_of("Object", list_of("objectIdObjTest"))));
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, stmt);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(context->dominantDb, std::string("LSST"));
    BOOST_REQUIRE(context->restrictors);
    BOOST_CHECK_EQUAL(context->restrictors->size(), 1U);
    BOOST_REQUIRE(context->restrictors->front());
    QsRestrictor& r = *context->restrictors->front();
    BOOST_CHECK_EQUAL(r._name, "sIndex");
    char const* params[] = {"LSST","Object", "objectIdObjTest", "2","3145","9999"};
    BOOST_CHECK_EQUAL_COLLECTIONS(r._params.begin(), r._params.end(),
                                  params, params+6);
}

BOOST_AUTO_TEST_SUITE_END()
