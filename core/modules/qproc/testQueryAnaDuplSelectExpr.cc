// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2015 AURA/LSST.
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
  *
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */

// System headers
#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <string>

// Third-party headers
#include "boost/algorithm/string.hpp"
#include "boost/format.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE QueryAnaDuplicateSelectExpr
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "parser/SelectParser.h"
#include "qana/DuplSelectExprPlugin.h"
#include "query/QueryContext.h"
#include "sql/MockSql.h"
#include "tests/QueryAnaFixture.h"
#include "util/Error.h"
#include "util/MultiError.h"


using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::parser::SelectParser;
using lsst::qserv::qana::DuplSelectExprPlugin;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::QueryContext;
using lsst::qserv::sql::MockSql;
using lsst::qserv::tests::QueryAnaFixture;
using lsst::qserv::util::Error;
using lsst::qserv::util::ErrorCode;
using lsst::qserv::util::MultiError;

/**
 * Reproduce exception message caused by a duplicated select field
 *
 * @param n     number of occurences found
 * @param name  name of the duplicated field
 * @param pos   position of the occurences found
 */
std::string build_exception_msg(std::string n, std::string name, std::string pos) {
    MultiError multiError;
    boost::format dupl_err_msg = boost::format(DuplSelectExprPlugin::ERR_MSG) %
                                               name % pos;

    Error error(ErrorCode::DUPLICATE_SELECT_EXPR, dupl_err_msg.str());
    multiError.push_back(error);
    std::string err_msg = "AnalysisError:" + DuplSelectExprPlugin::EXCEPTION_MSG +
                          multiError.toOneLineString();
    return err_msg;
}

////////////////////////////////////////////////////////////////////////
// CppParser basic tests
////////////////////////////////////////////////////////////////////////
BOOST_FIXTURE_TEST_SUITE(DuplSelectExpr, QueryAnaFixture)

BOOST_AUTO_TEST_CASE(Alias) {
    std::string sql = "select chunkId as f1, pm_declErr AS f1 from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";

    std::string expected_err_msg = build_exception_msg("2", "f1", " 1 2");

    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Object", {"pm_declErr", "chunkId", "bMagF"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, sql, true);
    BOOST_CHECK_EQUAL(qs->getError(), expected_err_msg);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
}

BOOST_AUTO_TEST_CASE(CaseInsensitive) {
    std::string sql = "select chunkId, CHUNKID from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";

    std::string expected_err_msg = build_exception_msg("2", "chunkid", " 1 2");

    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, sql, true);
    BOOST_CHECK_EQUAL(qs->getError(), expected_err_msg);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
}

BOOST_AUTO_TEST_CASE(Function) {
    std::string sql = "select sum(pm_declErr), chunkId as f1, chunkId AS f1, avg(pm_declErr) from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";

    std::string expected_err_msg = build_exception_msg("2", "f1", " 2 3");
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Object", {"pm_declErr", "chunkId", "bMagF"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, sql, true);
    BOOST_CHECK_EQUAL(qs->getError(), expected_err_msg);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
}

BOOST_AUTO_TEST_CASE(Simple) {
    std::string sql = "select pm_declErr, chunkId, ra_Test from LSST.Object where bMagF > 20.0 GROUP BY chunkId;";
    std::string expected_err_msg = build_exception_msg("2", "f1", " 2 3");
    MockSql::DbTableColumns dbTableColumns = {{"LSST", {{"Object", {"pm_declErr", "chunkId", "ra_Test", "bMagF"}}}}};
    qsTest.mysqlSchemaConfig = MySqlConfig(std::make_shared<MockSql>(dbTableColumns));
    std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, sql);
    std::shared_ptr<QueryContext> context = qs->dbgGetContext();
    BOOST_CHECK(context);
}

BOOST_AUTO_TEST_CASE(SameNameDifferentTable) {
    std::string sql = "SELECT o1.objectId, o2.objectId, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance "
            "FROM Object o1, Object o2 "
            "WHERE scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.05 "
            "AND  o1.objectId <> o2.objectId;";

    std::string expected_err_msg = build_exception_msg("2", "objectid", " 1 2");

     std::shared_ptr<QuerySession> qs = queryAnaHelper.buildQuerySession(qsTest, sql, true);
     BOOST_CHECK_EQUAL(qs->getError(), expected_err_msg);
     std::shared_ptr<QueryContext> context = qs->dbgGetContext();
     BOOST_CHECK(context);
}

BOOST_AUTO_TEST_SUITE_END()
