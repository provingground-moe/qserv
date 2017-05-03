#include "XrdCl/XrdClFile.hh"

#include <iostream>
#include <string>

namespace {

    // Data buffer

    const uint32_t size = 1024*1024;
    void *buffer = new char[1024*1024];

    int copyFile (const std::string &inFileUrl,
                  const std::string &outFileUrl) {

        XrdCl::XRootDStatus status;

        // Open the input file.

        XrdCl::File inFile;                         
        status = inFile.Open (
            inFileUrl,
            XrdCl::OpenFlags::Flags::Read |
            XrdCl::OpenFlags::Flags::SeqIO);
        if (!status.IsOK()) {
            std::cerr << status.ToString() << std::endl;
            return status.GetShellCode();
        }

        // Create the output file.

        XrdCl::File outFile;                         
        status = outFile.Open (
            outFileUrl,
            XrdCl::OpenFlags::Flags::New |
            XrdCl::OpenFlags::Flags::SeqIO,
            XrdCl::Access::Mode::UR |
            XrdCl::Access::Mode::UW);
        if (!status.IsOK()) {
            std::cerr << status.ToString() << std::endl;
            return status.GetShellCode();
        }

        // Copy records from the input file and write them into the output
        // one.

        uint64_t offset=0;
        while (true) {
            uint32_t bytesRead = 0;
            status = inFile.Read (offset, size, buffer, bytesRead);
            if (!status.IsOK()) {
                std::cerr << status.ToString() << std::endl;
                return status.GetShellCode();
            }
            if (!bytesRead) break;
            status = outFile.Write(offset, bytesRead, buffer);
            if (!status.IsOK()) {
                std::cerr << status.ToString() << std::endl;
                return status.GetShellCode();
            }
            offset += bytesRead;
        }
        inFile.Close();
        outFile.Sync();
        outFile.Close();
        return 0;
    }
    
    const char* usage = "usage: <inFileUrl> <outFileUrl>";
}

int main (int argc, const char* const argv[]) {
    if (argc != 3) {
        std::cerr
            << "error: please, provide the URLs for both files.\n"
            << usage << std::endl;
        return 1;
    }
    const std::string  inFileUrl = argv[1];
    const std::string outFileUrl = argv[2];

    return ::copyFile(inFileUrl, outFileUrl);
}