#include <cerrno>
#include <cstdio>           // std::FILE, C-style file I/O
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

#include "proto/replication.pb.h"
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/FileClient.h"

namespace rc = lsst::qserv::replica_core;

namespace {


const char *usage = "usage: <config> <worker> <database> <infile> <outfile> [--verbose]";

// Command line parameters

std::string configFileName;
std::string workerName;
std::string databaseName;
std::string inFileName;
std::string outFileName;

bool verbose = false;

// Record buffer

constexpr size_t bufSize{1000000};
uint8_t buf[bufSize];

/**
 * Instantiate and launch the service in its own thread. Then block
 * the current thread in a series of repeated timeouts.
 */
int run () {
  
    std::FILE* fp = 0;
    try {
        rc::Configuration   config   {configFileName};
        rc::ServiceProvider provider {config};

        if (rc::FileClient::pointer file =
            rc::FileClient::open (provider, workerName, databaseName, inFileName)) {

            const size_t fileSize = file->size();
            if (verbose)
                std::cout << "file size: " << fileSize << " bytes" << std::endl;

            if ((fp = std::fopen(outFileName.c_str(), "wb"))) {
                
                size_t totalRead = 0;
                size_t num;
                while ((num = file->read(buf, bufSize))) {
                    totalRead += num;
                    if (verbose)
                        std::cout << "read " << totalRead << "/" << fileSize << std::endl;
                    std::fwrite(buf, sizeof(uint8_t), num, fp);
                }
                if (fileSize == totalRead) {
                    std::fflush(fp);
                    std::fclose(fp);
                    return 0;
                }
                std::cerr << "input file was closed too early after reading " << totalRead
                    << " bytes instead of " << fileSize << std::endl;
            }
            std::cerr << "failed to open the output file, error: " << std::strerror(errno) << std::endl;
        }
        std::cerr << "failed to open the input file" << std::endl;

    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
    }
    if (fp) std::fclose(fp);

    return 1;
}
}  /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;
 
    if (argc < 6) {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    ::configFileName = argv[1];
    ::workerName     = argv[2];
    ::databaseName   = argv[3];
    ::inFileName     = argv[4];
    ::outFileName    = argv[5];

    if (argc >= 7) {
        const std::string opt(argv[6]); 
        if (opt == "--verbose") {
            ::verbose = true;
        } else {
            std::cerr << "unrecognized parameter: " << opt << "\n" << ::usage << std::endl;
            return 1;
        }
    }
    ::run ();

    return 0;
}
