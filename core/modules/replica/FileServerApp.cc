/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/FileServerApp.h"

// System headers
#include <thread>

// Qserv headers
#include "replica/FileServer.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description {
    "This is an  application which runs a read-only file server"
    " on behalf of a Replication system's worker"
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

FileServerApp::Ptr FileServerApp::create(int argc,
                                         const char* const argv[]) {
    return Ptr(
        new FileServerApp(
            argc,
            argv
        )
    );
}


FileServerApp::FileServerApp(int argc,
                             const char* const argv[])
    :   Application(
            argc,
            argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ),
        _log(LOG_GET("lsst.qserv.replica.tools.qserv-replica-file-server")) {

    // Configure the command line parser

    parser().required(
        "worker",
        "the name of a worker for which the server will be run",
        _workerName);

    parser().flag(
        "verbose",
        "enable the periodic 'heartbeat' printouts",
        _verbose);

}


int FileServerApp::runImpl() {

    FileServer::Ptr const server = FileServer::create(serviceProvider(), _workerName);

    thread serverLauncherThread([server] () {
        server->run();
    });
    serverLauncherThread.detach();
    
    // Block the current thread while periodically printing the "heartbeat"
    // report after a random delay in an interval of [1,5] seconds

    util::BlockPost blockPost(1000, 5000);
    while (true) {
        blockPost.wait();
        if (_verbose) {
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT  worker: " << server->worker());
        }
    }
}

}}} // namespace lsst::qserv::replica
