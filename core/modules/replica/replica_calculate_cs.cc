
#include "replica_core/FileUtils.h"

#include <iostream> 
#include <stdexcept>

namespace rc = lsst::qserv::replica_core;

int main(int argc, const char *argv[]) {
    if (argc != 2) {
        std::cerr << "usage: <filename>" << std::endl;
        return 1;
    }
    try {
        const uint64_t cs = rc::FileUtils::compute_cs (argv[1]);
        std::cout << "cs: " << cs << std::endl;
    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }
    return 0;
}