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
  /**
  *
  * @brief Simple testing for the ColumnRef class
  *
  */


// list must be included before boost/test/data/test_case.hpp, because it is used there but not included.
// (or that file could be included after boost/test/included/unit_test.hpp, which does cause list to be
// included. But, we like to include our headers alphabetically so I'm including list here.
#include <list>
#include <string>

// Qserv headers
#include "query/Identifier.h"

// Boost unit test header
#define BOOST_TEST_MODULE Identifier
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using namespace lsst::qserv;


BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(Unquoted) {
    auto id = std::make_shared<query::Identifier>("foo");
    BOOST_CHECK_EQUAL(id->get(), "foo");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "foo");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::NO_QUOTES), "foo");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::WITH_QUOTES), "\"foo\"");

    id->set("bar");
    BOOST_CHECK_EQUAL(id->get(), "bar");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "bar");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::NO_QUOTES), "bar");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::WITH_QUOTES), "\"bar\"");
}


BOOST_AUTO_TEST_CASE(Quoted) {
    auto id = std::make_shared<query::Identifier>("\"foo\"");
    BOOST_CHECK_EQUAL(id->get(), "foo");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "\"foo\"");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::NO_QUOTES), "foo");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::WITH_QUOTES), "\"foo\"");

    id->set("\"bar\"");
    BOOST_CHECK_EQUAL(id->get(), "bar");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "\"bar\"");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::NO_QUOTES), "bar");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::WITH_QUOTES), "\"bar\"");
}


BOOST_AUTO_TEST_CASE(ChangeQuoting) {
    auto id = std::make_shared<query::Identifier>("\"foo\"");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "\"foo\"");
    id->set("foo");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "foo");
    id->set("\"foo\"");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "\"foo\"");
}


BOOST_AUTO_TEST_CASE(Const) {
    auto id = std::make_shared<query::Identifier>("\"foo\"");
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "\"foo\"");
    *id = "foo";
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "foo");

    auto id2 = std::make_shared<query::Identifier>("bar");
    *id = *id2;
    BOOST_CHECK_EQUAL(id->get(query::Identifier::UNMODIFIED), "bar");
}


BOOST_AUTO_TEST_SUITE_END()



