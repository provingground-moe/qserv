/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// replica_job_replicate.cc implements a command-line tool which analyzes
/// chunk disposition in the specified databasae and (if needed) increases 
/// the number of chunk replicas to the desider level.

// System headers

#include <iostream>
#include <stdexcept>
#include <string>

// Qserv headers

#include "proto/replication.pb.h"
#include "replica/CmdParser.h"
#include "replica_core/Controller.h"
#include "replica_core/ReplicaInfo.h"
#include "replica_core/ReplicateJob.h"
#include "replica_core/ServiceProvider.h"

namespace r  = lsst::qserv::replica;
namespace rc = lsst::qserv::replica_core;

namespace {

// Command line parameters

std::string  databaseName;
unsigned int numReplicas;
bool         bestEffort;
bool         progressReport;
bool         errorReport;
std::string  configUrl;

/// Run the test
bool test () {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests
        // Note that omFinish callbak which are activated upon a completion
        // of the requsts will be run in that Controller's thread.

        rc::ServiceProvider provider (configUrl);

        rc::Controller::pointer controller = rc::Controller::create (provider);

        controller->run();

        ////////////////////
        // Start replication

        auto job =
            rc::ReplicateJob::create (
                numReplicas,
                databaseName,
                controller,
                [](rc::ReplicateJob::pointer job) {
                    // Not using the callback because the completion of
                    // the request will be caught by the tracker below
                    ;
                },
                bestEffort
            );

        job->start();
        job->track (progressReport,
                    errorReport,
                    std::cout);

        ///////////////////////////////////////////////////
        // Shutdown the controller and join with its thread

        controller->stop();
        controller->join();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return true;
}
} /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        r::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <database> <num-replicas> [--best-effort] [--progress-report] [--error-report]\n"
            "                            [--config=<url>]\n"
            "\n"
            "Parameters:\n"
            "  <database>         - the name of a database to inspect\n"
            "  <num-replicas>     - increase the number of chunk replicas to this level\n"
            "\n"
            "Flags and options:\n"
            "  --best-effort      - allowing the operation even after not getting chunk disposition from\n"
            "                       all workers\n"
            "  --progress-report  - the flag triggering progress report when executing batches of requests\n"
            "  --error-report     - the flag triggering detailed report on failed requests\n"
            "  --config           - a configuration URL (a configuration file or a set of the database\n"
            "                       connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::databaseName   = parser.parameter<std::string>(1);
        ::numReplicas    = parser.parameter<int>        (2);
        ::bestEffort     = parser.flag                  ("best-effort");
        ::progressReport = parser.flag                  ("progress-report");
        ::errorReport    = parser.flag                  ("error-report");
        ::configUrl      = parser.option   <std::string>("config", "file:replication.cfg");

    } catch (std::exception &ex) {
        return 1;
    }  
    ::test();
    return 0;
}
