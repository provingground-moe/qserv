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
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/Controller.h"
#include "replica_core/FindAllRequest.h"
#include "replica_core/ReplicaInfo.h"
#include "replica_core/DeleteRequest.h"
#include "replica_core/ServiceProvider.h"

namespace rc = lsst::qserv::replica_core;

namespace {

const char* usage =
    "Usage:\n"
    "  <config> <database> <num-replicas>\n";


// Command line parameters

std::string  configFileName;
std::string  databaseName;
unsigned int numReplicas;

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

        // Registry of  FindAll requests groupped by [<database>][<worker>]        
        std::map<std::string,
                 std::map<std::string,
                          rc::FindAllRequest::pointer>> findAllRequests;

        // The counter of requests which will be updated
        std::atomic<size_t> numFindAllRequstsSuccess(0);
        std::atomic<size_t> numFindAllRequstsFailure(0);
        std::atomic<size_t> numFindAllRequstsTotal  (0);

        // Launch requests against all workers and databases
        //
        // ATTENTION: calbacks on the request completion callbacks of the requests will
        //            be executed within the Contoller's thread. Watch for proper
        //            synchronization when inspecting/updating shared variables.

        for (const auto &database: databaseNames) {
            for (const auto &worker: workerNames) {
                numFindAllRequstsTotal++;
                findAllRequests[database][worker] =
                    controller->findAllReplicas (
                        worker, database,
                        [&numFindAllRequstsSuccess,&numFindAllRequstsFailure] (rc::FindAllRequest::pointer request) {
                            if (request->extendedState() == rc::Request::ExtendedState::SUCCESS)
                                numFindAllRequstsSuccess++;
                            else
                                numFindAllRequstsFailure++;
                        });
            }
        }

        // Wait before all request are finished

        rc::BlockPost blockPost (100, 200);
        while (numFindAllRequstsSuccess + numFindAllRequstsFailure < numFindAllRequstsTotal) {
            std::cout << "success / failure / total: "
                << numFindAllRequstsSuccess << " / "
                << numFindAllRequstsFailure << " / "
                << numFindAllRequstsTotal   << std::endl;
            blockPost.wait();
        }
        std::cout << "success / failure / total: "
            << numFindAllRequstsSuccess << " / "
            << numFindAllRequstsFailure << " / "
            << numFindAllRequstsTotal   << std::endl;

        // Analyse results and prepare a purge plan to shave off extra
        // replocas while trying to keep all nodes equally loaded

        // Registry of  replication requests groupped by [<database>][<worker>]        
        std::map<std::string,
                 std::map<std::string,
                          std::list<rc::DeleteRequest::pointer>>> deleteRequests;

        std::atomic<size_t> numDeleteRequestsSuccess(0);
        std::atomic<size_t> numDeleteRequestsFailure(0);
        std::atomic<size_t> numDeleteRequestsTotal  (0);

        for (const auto &database: databaseNames) {
            
            // A collection of workers for each chunk
            std::map<unsigned int, std::list<std::string>> chunk2workers;
            std::map<std::string, std::list<unsigned int>> worker2chunks;

            for (const auto &worker: workerNames) {

                auto &request = findAllRequests[database][worker];
                if ((request->state()         == rc::Request::State::FINISHED) &&
                    (request->extendedState() == rc::Request::ExtendedState::SUCCESS)) {

                    const auto &replicaInfoCollection = request->responseData ();
                    for (const auto &replicaInfo: replicaInfoCollection) {
                        if (replicaInfo.status() == rc::ReplicaInfo::Status::COMPLETE) {
                            chunk2workers[replicaInfo.chunk ()].push_back(replicaInfo.worker());
                            worker2chunks[replicaInfo.worker()].push_back(replicaInfo.chunk ());
                        }
                    }
                }
            }

            // Check which chunk replicas need to be eliminated. Then find the most loaded
            // worker holding the chunk and launch a delete request.
            //
            // TODO: this algorithm is way to simplistic as it won't take into
            //       an account other chunks. Ideally, it neees to be a two-pass
            //       scan.

            for (auto &entry: chunk2workers) {

                const unsigned int chunk{entry.first};

                // This collection is going to be modified
                std::list<std::string> replicas{entry.second};

                // Note that some chunks may have fewer replicas than required. In that case
                // the difference would be negative.
                const int numReplicas2delete =  replicas.size() - numReplicas;

                for (int i = 0; i < numReplicas2delete; ++i) {

                    // Find a candidate worker with the most number of chunks.
                    // This worker will be select as the 'destinationWorker' for the new replica.

                    std::string destinationWorker;
                    size_t      numChunksPerDestinationWorker = 0;

                    for (const auto &worker: replicas) {
                        if (worker2chunks[worker].size() > numChunksPerDestinationWorker) {
                            destinationWorker = worker;
                            numChunksPerDestinationWorker = worker2chunks[worker].size();
                        }
                    }
                    if (destinationWorker.empty()) {
                        std::cerr << "failed to find the most populated worker for replicating chunk: " << chunk
                            << ", skipping this chunk" << std::endl;
                        break;
                    }
                     
                    // Remove this chunk with the worker to decrease the number of chunks per
                    // the worker so that this updated stats will be accounted for later as
                    // the replication process goes.
                    std::remove(worker2chunks[destinationWorker].begin(),
                                worker2chunks[destinationWorker].end(),
                                chunk);

                    // Also register the worker in the chunk2workers[chunk] to prevent it
                    // from being select as the 'destinationWorker' for the same replica
                    // in case if more than one replica needs to be created.
                    // Also remove the worker from the local copy of the replicas, so that
                    // it won't be tries again
                    std::remove(replicas.begin(),
                                replicas.end(),
                                destinationWorker);
                    
                    // Finally, launch and register for further tracking the deletion
                    // request.
                    
                    numDeleteRequestsTotal++;
                    deleteRequests[database][destinationWorker].push_back (
                        controller->deleteReplica (
                            destinationWorker, database, chunk,
                            [&numDeleteRequestsSuccess,&numDeleteRequestsFailure] (rc::DeleteRequest::pointer request) {
                                if (request->extendedState() == rc::Request::ExtendedState::SUCCESS)
                                    numDeleteRequestsSuccess++;
                                else
                                    numDeleteRequestsFailure++;
                            }
                        )
                    );
                }
            }
        }

        // Wait before all request are finished

        rc::BlockPost longBlockPost (1000, 2000);
        while (numDeleteRequestsSuccess + numDeleteRequestsFailure < numDeleteRequestsTotal) {
            std::cout << "success / failure / total: "
                << numDeleteRequestsSuccess << " / "
                << numDeleteRequestsFailure << " / "
                << numDeleteRequestsTotal   << std::endl;
            longBlockPost.wait();
        }
        std::cout << "success / failure / total: "
            << numDeleteRequestsSuccess << " / "
            << numDeleteRequestsFailure << " / "
            << numDeleteRequestsTotal   << std::endl;
    
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

    if (argc != 4) {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    ::configFileName = argv[1];
    ::databaseName   = argv[2];
    try {
        ::numReplicas = std::stoul(argv[3]);
        if (!::numReplicas || ::numReplicas > 3)
            throw std::invalid_argument("the number of replicas must be in a range of 1 to 3");
    } catch (std::invalid_argument&) {
        std::cerr << "invalid number of chunks found in the command line\n"
            << ::usage << std::endl;
        return 1;
    }
 
    ::test();
    return 0;
}