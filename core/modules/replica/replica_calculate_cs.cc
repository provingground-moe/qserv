
#include "replica/CmdParser.h"
#include "replica_core/FileUtils.h"

#include <iostream> 
#include <stdexcept>
#include <string> 

namespace r  = lsst::qserv::replica;
namespace rc = lsst::qserv::replica_core;

namespace {

/// The name of an input file to be processed
std::string fileName;

/// The test
void test () {
    try {
        const uint64_t cs = rc::FileUtils::compute_cs (fileName);
        std::cout << cs << std::endl;
    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
        std::exit(1);
    }
}
} // namespace

int main(int argc, const char *argv[]) {

    // Parse command line parameters
    try {
        r::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <filename>\n"
            "\n"
            "Parameters:\n"
            "  <filename>  - the name of a file to read\n");

        ::fileName = parser.parameter<std::string>(1);

    } catch (std::exception &ex) {
        return 1;
    } 

    ::test();
    return 0;
}