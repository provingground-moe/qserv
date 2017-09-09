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
#ifndef LSST_QSERV_REPLICA_REQUEST_TRACKER_H
#define LSST_QSERV_REPLICA_REQUEST_TRACKER_H

/// RequestTracker.h declares:
///
/// class RequestTrackerBase
/// class RequestTracker
/// (see individual class documentation for more information)

// System headers

#include <atomic>
#include <ostream>
#include <string>
#include <list>

// Qserv headers

#include "replica/ErrorReporting.h"
#include "replica_core/Request.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The base class implements a type-independent foundation for the common
 * tracker for a collection of homogenious requests whose type is specified
 * as the template parameter of the class.
 */
class RequestTrackerBase {

public:

    // Default construction and copy semantics are proxibited

    RequestTrackerBase () = delete;
    RequestTrackerBase (RequestTrackerBase const&) = delete;
    RequestTrackerBase& operator= (RequestTrackerBase const&) = delete;

    /// Destructor
    virtual ~RequestTrackerBase ();

    /**
     * Block the calling thread until all request are finished. Then post
     * a summary report on failed requests if the optional flag 'errorReport'
     * was specified when constructing the tracker object. A progress of
     * the request execution will also be reported if the optional flag
     * 'progressReport' is passed into the constructor.
     */
    void track () const;

protected:

    /**
     * The constructor sets up tracking options.
     *
     * @param os             - an output stream for monitoring and error printouts
     * @param progressReport - triggers periodic printout onto an output stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     */
    explicit RequestTrackerBase (std::ostream& os,
                                 bool          progressReport=true,
                                 bool          errorReport=false);

    /**
     * The method to be implemented by a subclass in order to print
     * type-specific report.
     * 
     * @param os - an output stream for the printout
     */
    virtual void printErrorReport (std::ostream& os) const=0;

protected:

    // The counter of requests which will be updated. They need to be atomic
    // to avoid race condition between the onFinish() callbacks executed within
    // the Controller's thread and this thread.

    std::atomic<size_t> _numLaunched;   ///< the total number of requests launched
    std::atomic<size_t> _numFinished;   ///< the total number of finished requests
    std::atomic<size_t> _numSuccess;    ///< the number of successfully completed requests

private:

    // Parameters of the object    

    std::ostream& _os;

    bool _progressReport;
    bool _errorReport;
};

/**
 * The class implements a type-aware common tracker for a collection of
 * homogenious requests whose type is specified as the template parameter
 * of the class.
 */
template <class T>
class CommonRequestTracker
    :   public RequestTrackerBase {

public:

    // Default construction and copy semantics are proxibited

    CommonRequestTracker () = delete;
    CommonRequestTracker (CommonRequestTracker const&) = delete;
    CommonRequestTracker& operator= (CommonRequestTracker const&) = delete;

    /**
     * The constructor sets up tracking options.
     *
     * @param os             - an output stream for monitoring and error printouts
     * @param progressReport - triggers periodic printout onto an output stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     */
    explicit CommonRequestTracker (std::ostream& os,
                                   bool          progressReport=true,
                                   bool          errorReport=false)
        :   RequestTrackerBase (os,
                                progressReport,
                                errorReport) {
    }

    /// Destructor
    ~CommonRequestTracker () override {
    }

    /// Th ecallback function to be registered with each request
    /// injected into the tracker.

    void onFinish (typename T::pointer ptr) {
        RequestTrackerBase::_numFinished++;
        if (ptr->extendedState() == replica_core::Request::ExtendedState::SUCCESS)
            RequestTrackerBase::_numSuccess++;
    }

    /**
     * Add a request to be tracked. Note that in order to be tracked
     * requests needs to be constructed with the above specified function
     */
    void add (typename T::pointer const& ptr) {
        RequestTrackerBase::_numLaunched++;
        requests.push_back(ptr);
    }

protected:
    
    /**
     * Implement the corresponding method defined in the base class.
     *
     * @see RequestTrackerBase::printErrorReport
     */
    void printErrorReport (std::ostream& os) const override {
        replica::reportRequestState (requests, os);
    }

public:
    
    /// All requests launched
    std::list<typename T::pointer> requests;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUEST_TRACKER_H