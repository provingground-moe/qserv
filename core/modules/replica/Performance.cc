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
#include "replica/Performance.h"

// System headers
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

// Qserv headers
#include "replica/protocol.pb.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Performance");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

uint64_t PerformanceUtils::now() {
    return chrono::duration_cast<chrono::milliseconds>(
                chrono::system_clock::now().time_since_epoch()).count();
}

string PerformanceUtils::toDateTimeString(chrono::milliseconds const& millisecondsSinceEpoch) {

    chrono::time_point<chrono::system_clock> const point(millisecondsSinceEpoch);
    auto const timer = chrono::system_clock::to_time_t(point);
    auto broken_time = *localtime(&timer);

    ostringstream ss;
    ss << put_time(&broken_time, "%Y-%m-%d %H:%M:%S");
    ss << '.' << setfill('0') << setw(3) << millisecondsSinceEpoch.count() % 1000;
    return ss.str();
}


Performance::Performance()
    :   c_create_time(PerformanceUtils::now()),
        c_start_time(0),
        w_receive_time(0),
        w_start_time(0),
        w_finish_time(0),
        c_finish_time(0) {
}


void Performance::update(ProtocolPerformance const& workerPerformanceInfo) {
    w_receive_time = workerPerformanceInfo.receive_time();
    w_start_time   = workerPerformanceInfo.start_time();
    w_finish_time  = workerPerformanceInfo.finish_time();
}


uint64_t Performance::setUpdateStart() {
    uint64_t const t = c_start_time;
    c_start_time = PerformanceUtils::now();
    return t;
}


uint64_t Performance::setUpdateFinish() {
    uint64_t const t = c_finish_time;
    c_finish_time = PerformanceUtils::now();
    return t;
}


ostream& operator<<(ostream& os, Performance const& p) {
    os  << "Performance "
        << " c.create:"   << p.c_create_time
        << " c.start:"    << p.c_start_time
        << " w.receive:"  << p.w_receive_time
        << " w.start:"    << p.w_start_time
        << " w.finish:"   << p.w_finish_time
        << " c.finish:"   << p.c_finish_time
        << " length.sec:" << (p.c_finish_time ? (p.c_finish_time - p.c_start_time)/1000. : '*');
    return os;
}


WorkerPerformance::WorkerPerformance()
    :   receive_time(PerformanceUtils::now()),
        start_time(0),
        finish_time(0) {
}


uint64_t WorkerPerformance::setUpdateStart() {
    uint64_t const t = start_time;
    start_time = PerformanceUtils::now();
    return t;
}


uint64_t WorkerPerformance::setUpdateFinish() {
    uint64_t const t = finish_time;
    finish_time = PerformanceUtils::now();
    return t;
}


unique_ptr<ProtocolPerformance> WorkerPerformance::info() const {
    auto ptr = make_unique<ProtocolPerformance>();
    ptr->set_receive_time(receive_time);
    ptr->set_start_time(start_time);
    ptr->set_finish_time(finish_time);
    return ptr;
}


ostream& operator<<(ostream& os, WorkerPerformance const& p) {
    os  << "WorkerPerformance "
        << " receive:"    << p.receive_time
        << " start:"      << p.start_time
        << " finish:"     << p.finish_time
        << " length.sec:" << (p.finish_time ? (p.finish_time - p.receive_time)/1000. : '*');
    return os;
}

}}} // namespace lsst::qserv::replica