#include <atomic>
#include <iomanip>
#include <iostream>
#include <map>
#include <vector>
#include <stdexcept>
#include <string>

#include "proto/replication.pb.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/Controller.h"
#include "replica_core/FindAllRequest.h"
#include "replica_core/ReplicaInfo.h"
#include "replica_core/ServiceProvider.h"

namespace rc = lsst::qserv::replica_core;

namespace {

const char* usage =
    "Usage:\n"
    "  <config> <database> [--error-report]\n";


// Command line parameters

std::string configFileName;
std::string databaseName;

bool  errorReport=false;

/// The collection of all requests     
typedef std::vector<rc::FindAllRequest::pointer> RequestsCollection;


/// Print a detauled report on any requests which haven't succeeded
void printErrorReport (const RequestsCollection &requests) {

    std::cout
       << "FAILED REQUESTS:\n"
       << "--------------------------------------+--------+----------+-------------+----------------------+--------------------------\n"
       << "                                   id | worker | database |       state |            ext.state |          server err.code \n"
       << "--------------------------------------+--------+----------+-------------+----------------------+--------------------------\n";   
    for (const auto &ptr: requests) {
        std::cout
            << " "   << std::setw(36) <<                           ptr->id()
            << " | " << std::setw( 6) <<                           ptr->worker()
            << " | " << std::setw( 8) <<                           ptr->database()
            << " | " << std::setw(11) << rc::Request::state2string(ptr->state())
            << " | " << std::setw(20) << rc::Request::state2string(ptr->extendedState())
            << " | " << std::setw(24) << rc::status2string(        ptr->extendedServerStatus())
            << "\n";
    }
    std::cout
       << "--------------------------------------+--------+----------+-------------+----------------------+--------------------------\n"
       << std::endl;
}


/// Run the test
bool test () {

    try {

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::Controller::pointer controller = rc::Controller::create(provider);

        // Start the controller in its own thread before injecting any requests
        controller->run();

        
        // Get the names of all workers  from the configuration, and ask each worker
        // which replicas it has.
        const auto workerNames = config.workers();

        // The collection of all requests all requests      
        RequestsCollection requests;

        // The counter of requests which will be updated
        std::atomic<size_t> numSuccess(0);
        std::atomic<size_t> numFailure(0);
        std::atomic<size_t> numTotal  (0);

        // Launch requests against all workers
        //
        // ATTENTION: calbacks on the request completion callbacks of the requests will
        //            be executed within the Contoller's thread. Watch for proper
        //            synchronization when inspecting/updating shared variables.

        std::cout
            << "\n"
            << "WORKERS:";
        for (const auto &worker: workerNames) {
            std::cout << " " << worker;
        }
        std::cout
            << "\n"
            << std::endl;

        for (const auto &worker: workerNames) {
            numTotal++;
            requests.push_back (
                controller->findAllReplicas (
                    worker, databaseName,
                    [&numSuccess, &numFailure] (rc::FindAllRequest::pointer request) {
                        if (request->extendedState() == rc::Request::ExtendedState::SUCCESS)
                            numSuccess++;
                        else
                            numFailure++;
                    }
                )
            );
        }

        // Wait before all request are finished

        rc::BlockPost blockPost (100, 200);
        while (numSuccess + numFailure < numTotal) {
            std::cout << "success / failure / total: "
                << numSuccess << " / "
                << numFailure << " / "
                << numTotal   << std::endl;
            blockPost.wait();
        }
        std::cout << "success / failure / total: "
            << numSuccess << " / "
            << numFailure << " / "
            << numTotal   << std::endl;

        // Analyse and display results

            
        // A collection of workers for each chunk
        std::map<unsigned int, std::vector<std::string>> chunk2workers;
        std::map<std::string, std::vector<unsigned int>> worker2chunks;

        for (const auto &request: requests) {

            if ((request->state()         == rc::Request::State::FINISHED) &&
                (request->extendedState() == rc::Request::ExtendedState::SUCCESS)) {

                const auto &replicaInfoCollection = request->responseData ();
                for (const auto &replicaInfo: replicaInfoCollection) {
                    chunk2workers[replicaInfo.chunk()].push_back (
                        replicaInfo.worker() + (replicaInfo.status() == rc::ReplicaInfo::Status::COMPLETE ? "" : "(!)"));
                    worker2chunks[replicaInfo.worker()].push_back(replicaInfo.chunk());
                }
            }
        }
        std::cout
            << "CHUNK DISTRIBUTION:\n"
            << "----------+------------\n"
            << "   worker | num.chunks \n"
            << "----------+------------\n";

        for (const auto &entry: worker2chunks) {
            const auto &worker = entry.first;
            const auto &chunks = entry.second;
            std::cout
                << " " << std::setw(8) << worker << " | " << std::setw(10) << chunks.size() << "\n";
        }
        std::cout
            << "----------+------------\n"
            << std::endl;

        std::cout
            << "REPLICAS:\n"
            << "----------+--------------+---------------------------------------------\n"
            << "    chunk | num.replicas | worker:replica_status \n"
            << "----------+--------------+---------------------------------------------\n";

        for (const auto &entry: chunk2workers) {
            const auto &chunk    = entry.first;
            const auto &replicas = entry.second;
            std::cout
                << " " << std::setw(8) << chunk << " | " << std::setw(12) << replicas.size() << " |";
            for (const auto &replica: replicas) {
                std::cout << " " << replica;
            }
            std::cout << "\n";
        }
        std::cout
            << "----------+--------------+---------------------------------------------\n"
            << std::endl;

        if (errorReport && numFailure) {
            printErrorReport(requests);
        }

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

    if (argc < 3) {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    ::configFileName = argv[1];
    ::databaseName   = argv[2];

    if (argc >= 4) {
        const std::string opt = argv[3];
        if (opt == "--error-report")
            ::errorReport = true;
        else {
            std::cerr << "unrecognized command option: " << opt << "\n"
                << ::usage << std::endl;
            return 1;
        }
    }
 
    ::test();
    return 0;
}