#include <chrono>
#include <iostream>
#include <list>
#include <random>
#include <ratio>        // std::milli
#include <string>
#include <thread>

#include "replica_core/Configuration.h"
#include "replica_core/MasterServer.h"
#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceProvider.h"

namespace rc = lsst::qserv::replica_core;

namespace {
        
    
    std::random_device rd;                      // Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd());                     // Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> dis(0, 5000);
    
    auto random_interval_milliseconds = [gen,dis]() -> int {
        return dis(gen);
    };
    
    /**
     * Instantiate and launch the client in its own thread. Then block
     * the current thread in a series of repeated timeouts.
     */
    void client (const std::string &configFileName) {

        const std::string database = "wise_00";

        try {
            rc::Configuration  ::pointer config   = rc::Configuration  ::create (configFileName);
            rc::ServiceProvider::pointer provider = rc::ServiceProvider::create (config);
            rc::MasterServer   ::pointer server   = rc::MasterServer   ::create (provider);

            // Start the server in its own thread before injecting any requests

            server->run();

            // Create a few requests per each known worker

            unsigned int chunk = 0;

            for (const std::string &workerName : provider->workers()) {
                
                const std::string &sourceWorker      = workerName;
                const std::string &destinationWorker = workerName;
                
                for (unsigned int i = 0; i < 10; ++i) {

                    rc::ReplicationRequest::pointer request =
                        server->replicate (
                            database,
                            chunk++,
                            sourceWorker,
                            destinationWorker,
                            [] (rc::ReplicationRequest::pointer request) {
                                std::cout
                                    << "<REPLICATE>\n"
                                    << "    id : " << request->id() << "\n"
                                    << "    status : " << request->state2string(request->state()) << "::"
                                                       << request->state2string(request->extendedState()) << std::endl;
                            }
                        );
                }
            }        
        
            std::cout << "server is " << (server->isRunning() ? "" : "NOT ") << "running" << std::endl;

            // Continue injecting requests on the periodic bases, one at a time for
            // each known worker.

            while (true) {

                const std::chrono::duration<long, std::milli> interval = std::chrono::milliseconds(random_interval_milliseconds());
                std::this_thread::sleep_for(interval);

                for (const std::string &workerName : provider->workers()) {
                    
                    const std::string &sourceWorker      = workerName;
                    const std::string &destinationWorker = workerName;

                    rc::ReplicationRequest::pointer request =
                        server->replicate (
                            database,
                            chunk++,
                            sourceWorker,
                            destinationWorker,
                            [] (rc::ReplicationRequest::pointer request) {
                                std::cout
                                    << "<REPLICATE>\n"
                                    << "    id : " << request->id() << "\n"
                                    << "    status : " << request->state2string(request->state()) << "::"
                                                       << request->state2string(request->extendedState()) << std::endl;
                            }
                        );
                }
                
                // Stop when had enough
                
                if (chunk > 13) break;
            }
            
            // Do a proper clean up of the service by stopping it. This way of stopping
            // the service will guarantee that all outstanding opeations will finish
            // and not aborted.
            //
            // NOTE: Joining to the server's thread is not needed because
            //       this will always be done internally inside the stop method.
            //       The only reason for joining would be for having an option of
            //       integrating the server into a larger application.

            server->stop();

            std::cout << "server is " << (server->isRunning() ? "" : "NOT ") << "running" << std::endl;

            server->run();

            std::cout << "server is " << (server->isRunning() ? "" : "NOT ") << "running" << std::endl;

            // Launch another thread which will test injecting requests from there
            
            std::thread another ( [provider,server,database,random_interval_milliseconds] () {

                unsigned int chunk = 2000;

                while (true) {
    
                    const std::chrono::duration<long, std::milli> interval = std::chrono::milliseconds(random_interval_milliseconds());
                    std::this_thread::sleep_for(interval);
    
                    for (const std::string &workerName : provider->workers()) {
                        
                        const std::string &sourceWorker      = workerName;
                        const std::string &destinationWorker = workerName;
    
                        rc::ReplicationRequest::pointer request =
                            server->replicate (
                                database,
                                chunk++,
                                sourceWorker,
                                destinationWorker,
                                [] (rc::ReplicationRequest::pointer request) {
                                    std::cout
                                        << "<REPLICATE>\n"
                                        << "    id : " << request->id() << "\n"
                                        << "    status : " << request->state2string(request->state()) << "::"
                                                           << request->state2string(request->extendedState()) << std::endl;
                                }
                            );
                    }
                    
                    // Stop when had enough
                
                    if (chunk > 3000) break;
                }

            });

            // Continue injecting requests on the periodic bases, one at a time for
            // each known worker.

            std::vector<rc::ReplicationRequest::pointer> requests;
            
            while (true) {

                const std::chrono::duration<long, std::milli> interval = std::chrono::milliseconds(random_interval_milliseconds());
                std::this_thread::sleep_for(interval);

                for (const std::string &workerName : provider->workers()) {
                    
                    const std::string &sourceWorker      = workerName;
                    const std::string &destinationWorker = workerName;

                    rc::ReplicationRequest::pointer request =
                        server->replicate (
                            database,
                            chunk++,
                            sourceWorker,
                            destinationWorker,
                            [] (rc::ReplicationRequest::pointer request) {
                                std::cout
                                    << "<REPLICATE>\n"
                                    << "    id : " << request->id() << "\n"
                                    << "    status : " << request->state2string(request->state()) << "::"
                                                       << request->state2string(request->extendedState()) << std::endl;
                            }
                        );
                    requests.push_back(request);
                }
                
                // Stop when had enough
                
                if (chunk > 16) break;
            }

            // Launch other kinds of requests

            for (auto request : requests) {
                server->statusOfReplication (
                    request->worker(),
                    request->id(),
                    [] (rc::StatusRequest::pointer request) {
                        std::cout
                            << "<STATUS>\n"
                            << "    id : " << request->id() << "\n"
                            << "    status : " << request->state2string(request->state()) << "::"
                                               << request->state2string(request->extendedState()) << "\n"
                            << "    replicationRequestId : " << request->replicationRequestId() << std::endl;
                    }
                );
                server->stopReplication (
                    request->worker(),
                    request->id(),
                    [] (rc::StopRequest::pointer request) {
                        std::cout
                            << "<STOP> finished\n"
                            << "    id : " << request->id() << "\n"
                            << "    status : " << request->state2string(request->state()) << "::"
                                               << request->state2string(request->extendedState()) << "\n"
                            << "    replicationRequestId : " << request->replicationRequestId()
                            << std::endl;
                    }
                );
            }
            
            std::cout << "sever is " << (server->isRunning() ? "" : "NOT ") << "running" << std::endl;

            another.join();
            
            // Now run it w/o stopping. This should block the current thread
            // indefinitively.

            server->join();

        } catch (const std::exception& e) {
            std::cerr << e.what() << std::endl;
        }
    }
}

int main (int argc, const char* const argv[]) {

    if (argc != 2) {
        std::cerr << "Usage: <config>" << std::endl;
        return 1;
    }
    const std::string configFileName(argv[1]);
    
    ::client(configFileName);

    return 0;
}