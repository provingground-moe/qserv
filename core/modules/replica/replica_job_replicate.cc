#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <vector>
#include <stdexcept>
#include <string>

#include "proto/replication.pb.h"
#include "replica/CmdParser.h"
#include "replica_core/Configuration.h"
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
std::string  configFileName;

/// Run the test
bool test () {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests
        // Note that omFinish callbak which are activated upon a completion
        // of the requsts will be run in that Controller's thread.

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::Controller::pointer controller = rc::Controller::create(provider);

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
            "                            [--config=<file>]\n"
            "\n"
            "Parameters:\n"
            "  <database>         - the name of a database to inspect\n"
            "  <num-replicas>     - increase the number of replicas in each chunk to this level\n"
            "\n"
            "Flags and options:\n"
            "  --best-effort      - allowing the replication even after not getting chunk disposition from\n"
            "                       all workers\n"
            "  --progress-report  - the flag triggering progress report when executing batches of requests\n"
            "  --error-report     - the flag triggering detailed report on failed requests\n"
            "  --config           - the name of the configuration file.\n"
            "                       [ DEFAULT: replication.cfg ]\n");

        ::databaseName   = parser.parameter<std::string>(1);
        ::numReplicas    = parser.parameter<int>        (2);
        ::bestEffort     = parser.flag                  ("best-effort");
        ::progressReport = parser.flag                  ("progress-report");
        ::errorReport    = parser.flag                  ("error-report");
        ::configFileName = parser.option   <std::string>("config", "replication.cfg");

    } catch (std::exception &ex) {
        return 1;
    }  
    ::test();
    return 0;
}
