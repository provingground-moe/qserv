#include <chrono>
#include <ctime>
#include <iostream>
#include <string>
#include <thread>

#include <boost/array.hpp>
#include <boost/asio.hpp>

using namespace std;
using namespace boost::asio;

namespace {
    
    void client (const char *host) {
        try {
            io_service io_service;

            ip::tcp::resolver           resolver(io_service);
            ip::tcp::resolver::query    query(host, "50000");
            ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

            ip::tcp::socket socket(io_service);
            connect(socket, endpoint_iterator);

                boost::system::error_code err;

            for (;;) {
                write(socket, buffer("DAYTIME"), err);

                boost::array<char, 128> buf;
                size_t len = socket.read_some(buffer(buf), err);
                if (err == boost::asio::error::eof) break;      // Connection closed cleanly by peer.
                else if (err)
                    throw boost::system::system_error(err);     // Some other error.

                cout.write(buf.data(), len);

                this_thread::sleep_for(chrono::milliseconds(2000));
            }
        } catch (exception& e) {
            cerr << e.what() << endl;
        }
    }
}

int main (int argc, const char* const argv[]) {
    if (argc != 2) {
        cerr << "Usage: <host>" << endl;
        return 1;
    }
    ::client(argv[1]);
    return 0;
}