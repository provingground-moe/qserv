#include <cstring>
#include <ctime>
#include <iostream>
#include <string>

#include <boost/array.hpp>
#include <boost/asio.hpp>

using namespace std;
using namespace boost::asio;

namespace {

    string make_daytime_string () {
        time_t now = time(0);
        return ctime(&now);
    }
    
    void run_server () {
        try {
            io_service io_service;

            ip::tcp::acceptor acceptor(
                io_service,
                ip::tcp::endpoint(ip::tcp::v4(), 50000));

            boost::system::error_code err;

            for (;;) {

                ip::tcp::socket socket(io_service);
                acceptor.accept(socket);
    
    
                for (;;) {
                    // Wait for a request
                    boost::array<char, 128> buf;
                    size_t len = socket.read_some(buffer(buf), err);
                    if      (err == error::eof) break;            // Connection closed cleanly by peer.
                    else if (err)
                        throw boost::system::system_error(err); // Some other error.
    
                    if (!strncmp(buf.data(), "DAYTIME", len)) {
                        write(socket, buffer(make_daytime_string()), err);
                    } else {
                        cerr << "unknown request: ";
                        cerr.write(buf.data(), len);
                        cerr << endl;
                    }
                }
            }
        } catch (exception& e) {
            cerr << e.what() << endl;
        }
    }
}

int main (int argc, const char* const argv[]) {
    ::run_server();
    return 0;
}