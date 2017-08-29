#include <algorithm>
#include <atomic>
#include <cstdlib>
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
    "  <config> [<database>]\n";


// Command line parameters

std::string configFileName;
std::string databaseName;

/// Run the test
bool test () {

    try {

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::Controller::pointer controller = rc::Controller::create(provider);

        // Start the controller in its own thread before injecting any requests
        controller->run();

        
        // Get the names of all workers and databases from the configuration,
        // and ask each worker which replicas it has.

        const auto workerNames   = config.workers();
        const auto databaseNames = databaseName.empty() ? config.databases() : std::vector<std::string>{databaseName};

        // Registry of all requests groupped by [<database>][<worker>]        
        std::map<std::string,
                 std::map<std::string,
                          rc::FindAllRequest::pointer>> requests;

        // The counter of requests which will be updated
        std::atomic<size_t> numLaunched(0);
        std::atomic<size_t> numFinished(0);

        // Launch requests against all workers and databases
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

        for (const auto &database: databaseNames) {
            for (const auto &worker: workerNames) {
                numLaunched++;
                requests[database][worker] =
                    controller->findAllReplicas (
                        worker, database,
                        [&numFinished] (rc::FindAllRequest::pointer request) {
                            numFinished++;
                        });
            }
        }

        // Wait before all request are finished

        rc::BlockPost blockPost (100, 200);
        while (numFinished < numLaunched) {
            std::cout << "...processing: " << numFinished << "/" << numLaunched << std::endl;
            blockPost.wait();
        }
        std::cout << "...processing: " << numFinished << "/" << numLaunched << std::endl;

        // Analyse and display results

        for (const auto &database: databaseNames) {
            
            // A collection of workers for each chunk
            std::map<unsigned int, std::vector<std::string>> chunk2workers;
            std::map<std::string, std::vector<unsigned int>> worker2chunks;

            for (const auto &worker: workerNames) {

                auto &request = requests[database][worker];
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
                << "\n"
                << "DATABASE: " << database << "\n"
                << std::endl;

            std::cout
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

    if (argc < 2) {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    ::configFileName = argv[1];

    if (argc >= 3)
        ::databaseName = argv[2];
 
    ::test();
    return 0;
}