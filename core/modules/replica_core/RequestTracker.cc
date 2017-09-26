/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

#include "replica_core/RequestTracker.h"

// System headers

// Qserv headers

#include "replica_core/BlockPost.h"

namespace lsst {
namespace qserv {
namespace replica_core {


///////////////////////////////////////////
//          RequestTrackerBase           //
///////////////////////////////////////////

RequestTrackerBase::RequestTrackerBase (std::ostream& os,
                                        bool          progressReport,
                                        bool          errorReport)
    :   _numLaunched (0),
        _numFinished (0),
        _numSuccess  (0),
        _os(os),
        _progressReport(progressReport),
        _errorReport   (errorReport) {
}

void
RequestTrackerBase::track () const {

    // Wait before all request are finished. Then analyze results
    // and print a report on failed requests (if any)

    replica_core::BlockPost blockPost (100, 200);
    while (_numFinished < _numLaunched) {
        blockPost.wait();
        if (_progressReport)
            _os << "RequestTracker::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
    }
    if (_progressReport)
        _os << "RequestTracker::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;

    if (_errorReport && _numLaunched - _numSuccess)
        printErrorReport (_os);
}

RequestTrackerBase::~RequestTrackerBase () {
}


//////////////////////////////////////////
//          AnyRequestTracker           //
//////////////////////////////////////////


AnyRequestTracker::AnyRequestTracker (std::ostream& os,
                                      bool          progressReport,
                                      bool          errorReport)
    :   RequestTrackerBase (os,
                            progressReport,
                            errorReport) {
}

AnyRequestTracker::~AnyRequestTracker () {
}

void
AnyRequestTracker::onFinish (Request::pointer const& ptr) {
    RequestTrackerBase::_numFinished++;
    if (ptr->extendedState() == Request::ExtendedState::SUCCESS)
        RequestTrackerBase::_numSuccess++;
}

void
AnyRequestTracker::add (Request::pointer const& ptr) {
    RequestTrackerBase::_numLaunched++;
    requests.push_back(ptr);
}

void
AnyRequestTracker::printErrorReport (std::ostream& os) const {
    replica_core::reportRequestState (requests, os);
}

}}} // namespace lsst::qserv::replica_core
