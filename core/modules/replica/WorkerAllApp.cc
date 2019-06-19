/*
 * LSST Data Management System
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

// Class header
#include "replica/WorkerAllApp.h"

// System headers
#include <thread>

// Qserv headers
#include "replica/FileServer.h"
#include "replica/IngestServer.h"
#include "replica/WorkerRequestFactory.h"
#include "replica/WorkerServer.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
    "This application runs all worker servers within a single process."
    " NOTE: a special single-node configuration is required by this test."
    " Also, each logical worker must get a unique path in a data file"
    " system. The files must be read-write enabled for a user account"
    " under which the test is run.";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

WorkerAllApp::Ptr WorkerAllApp::create(int argc, char* argv[]) {
    return Ptr(
        new WorkerAllApp(argc, argv)
    );
}


WorkerAllApp::WorkerAllApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ),
        _log(LOG_GET("lsst.qserv.replica.WorkerAllApp")),
        _qservDbPassword(Configuration::qservWorkerDatabasePassword()) {

    // Configure the command line parser

    parser().flag(
        "all-workers",
        "Launch worker services for all known workers regardless of their"
        " configuration status (DISABLED or READ-ONLY).",
        _allWorkers);

    parser().option(
        "qserv-db-password",
        "A password for the MySQL account of the Qserv worker database. The account"
        " name is found in the Configuration. NOTE: an assumption is that all worker"
        " databases are configured in the same way",
        _qservDbPassword
    );

    parser().flag(
        "enable-file-server",
        "Also launch a dedicated file server for each worker.",
        _enableFileServer);

    parser().flag(
        "enable-ingest-server",
        "Also launch a dedicated catalog ingest server for each worker.",
        _enableIngestServer);
}


int WorkerAllApp::runImpl() {

    // Set the database password
    Configuration::setQservWorkerDatabasePassword(_qservDbPassword);

    WorkerRequestFactory requestFactory(serviceProvider());

    // Run the worker servers

    _runAllWorkers(requestFactory);

    // Then block the calling thread forever.
    util::BlockPost blockPost(1000, 5000);
    while (true) {
        blockPost.wait();  
    }
    return 0;
}


void WorkerAllApp::_runAllWorkers(WorkerRequestFactory& requestFactory) {

    auto workers = _allWorkers ?
        serviceProvider()->config()->allWorkers() :
        serviceProvider()->config()->workers();

    for (auto&& workerName: workers) {
        
        // Create the request processing server and run it within a dedicated thread
        // because it's the blocking operation for the launching thread.

        auto const reqProcSrv = WorkerServer::create(serviceProvider(), requestFactory, workerName);

        thread reqProcSrvThread([reqProcSrv] () {
            reqProcSrv->run();
        });
        reqProcSrvThread.detach();
        
        // Run the heartbeat monitor for the server within another thread
 
        thread reqProcMonThread([reqProcSrv,this] () {
            util::BlockPost blockPost(1000, 5000);
            while (true) {
                blockPost.wait();
                LOGS(this->_log, LOG_LVL_INFO, "<WORKER:" << reqProcSrv->worker() << " HEARTBEAT> "
                    << " processor state: " << reqProcSrv->processor()->state2string()
                    << " new:"              << reqProcSrv->processor()->numNewRequests()
                    << " in-progress: "     << reqProcSrv->processor()->numInProgressRequests()
                    << " finished: "        << reqProcSrv->processor()->numFinishedRequests());
            }
        });
        reqProcMonThread.detach();

        // If requested then also create and run the additional servers or serving
        // files and catalog ingest requests. Note the servers will be run in
        // in dedicated threads because starting each service is the blocking
        // operation for the launching thread.

        if (_enableFileServer) {
            auto fileSrv = FileServer::create(serviceProvider(), workerName);
    
            thread fileSrvThread([fileSrv] () {
                fileSrv->run();
            });
            fileSrvThread.detach();
        }
        if (_enableIngestServer) {
            auto ingestSrv = IngestServer::create(serviceProvider(), workerName);
    
            thread ingestSrvThread([ingestSrv] () {
                ingestSrv->run();
            });
            ingestSrvThread.detach();
        }
    }
}

}}} // namespace lsst::qserv::replica
