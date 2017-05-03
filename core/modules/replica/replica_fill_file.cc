#include "XrdCl/XrdClFile.hh"

#include <iostream>
#include <string>

namespace {

    // Test buffer

    const uint32_t size = 1024*1024;
    const void  *buffer = new char[1024*1024];

    int createAndFill (const std::string &url) {

        XrdCl::XRootDStatus status;

        // Create the file.

        XrdCl::File file;                         
        status = file.Open (
            url,
            XrdCl::OpenFlags::Flags::New |
            XrdCl::OpenFlags::Flags::SeqIO,
            XrdCl::Access::Mode::UR |
            XrdCl::Access::Mode::UW);
        if (!status.IsOK()) {
            std::cerr << status.ToString() << std::endl;
            return status.GetShellCode();
        }
        uint64_t offset=0;
        const int numRecords = 128;
        for (int i=0; i < numRecords; ++i) {
            status = file.Write(offset, size, buffer);
            if (!status.IsOK()) {
                std::cerr << status.ToString() << std::endl;
                file.Close();
                return status.GetShellCode();
            }
            offset += size;
        }
        file.Sync();
        return file.Close().GetShellCode();
    }

    const char* usage = "usage: <outFileUrl>";
}

int main (int argc, const char* const argv[]) {
    if (argc != 2) {
        std::cerr
            << "error: please, provide the URL for the output file.\n"
            << usage << std::endl;
        return 1;
    }
    const std::string outFileUrl = argv[1];
    return ::createAndFill(outFileUrl);
}