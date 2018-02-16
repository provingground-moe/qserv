// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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
#include "qdisp/ResponsePool.h"

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.ResponsePool");
}




namespace lsst {
namespace qserv {
namespace qdisp {


// must hold pq._mtx before calling
std::ostream& operator<<(std::ostream& os, PriorityQueue const& pq) {
    auto iter = pq._queues.begin();
    auto end = pq._queues.end();
    for (;iter != end; ++iter) {
        PriorityQueue::PriQ::Ptr const& que = iter->second;
        os << "(pri=" << que->getPriority()
           << ":sz="  << que->size()
           << ":r="   << que->running << ")";
    }
    return os;
}


///< @Return true if the queue could be added.
bool PriorityQueue::addPriQueue(int priority, int minRunning) {
    std::lock_guard<std::mutex> lock(_mtx);
    auto q = std::make_shared<PriQ>(priority, minRunning);
    std::pair<int, PriQ::Ptr> item(priority, q);
    auto ret = _queues.insert(item);
    if (!ret.second) {
        ; /// &&& add log message
    }
    return ret.second;
}

/// The pool needs to be able to place commands in this queue for shutdown.
void PriorityQueue::queCmd(util::Command::Ptr const& cmd) {
    {
        std::lock_guard<std::mutex> lock(_mtx);
        LOGS (_log, LOG_LVL_DEBUG, "&&&&&&& PriorityQueue::queCmd");
        auto iter = _queues.find(_defaultPriority);
        if (iter == _queues.end()) {
            throw Bug("PriorityQueue default priority queue not found a!");
        }
        iter->second->queCmd(cmd);
        LOGS (_log, LOG_LVL_DEBUG, "&&&priQueCmd" << *this);
        _changed = true;
    }
    _cv.notify_all();
}


void PriorityQueue::queCmd(PriorityCommand::Ptr const& cmd, int priority) {
    {
        std::lock_guard<std::mutex> lock(_mtx);
        auto iter = _queues.find(priority);
        if (iter == _queues.end()) {
            // give it the default priority
            // &&& add log message
            iter = _queues.find(_defaultPriority);
            if (iter == _queues.end()) {
                throw Bug("PriorityQueue default priority queue not found b!");
            }
        }
        cmd->_priority = priority;
        iter->second->queCmd(cmd);
        LOGS (_log, LOG_LVL_DEBUG, "&&&priQue p=" << priority << *this);
        _changed = true;
    }
    _cv.notify_all();
}


util::Command::Ptr PriorityQueue::getCmd(bool wait){
    util::Command::Ptr ptr;
    std::unique_lock<std::mutex> uLock(_mtx);
    while (true) {
        _changed = false;
        LOGS (_log, LOG_LVL_DEBUG, "&&&priQueGet" << *this);

        /// Make sure minimum number of jobs running per priority.
        auto iter = _queues.begin();
        auto end = _queues.end();
        if (!_shuttingDown) {
            // If shutting down, this could prevent all jobs from completing.
            // Goes from highest to lowest priority queue
            for (;iter != end; ++iter) {
                PriQ::Ptr const& que = iter->second;
                if (que->running < que->getMinRunning()) {
                    ptr = que->getCmd(false); // no wait
                    if (ptr != nullptr) {
                        return ptr;
                    }
                }
            }
        }

        LOGS (_log, LOG_LVL_DEBUG, "&&&get");
        // Since all the minimums are met, just run the first command found.
        iter = _queues.begin();
        for (;iter != end; ++iter) {
            PriQ::Ptr const& que = iter->second;
            ptr = que->getCmd(false); // no wait
            if (ptr != nullptr) {
                return ptr;
            }
        }

        LOGS (_log, LOG_LVL_DEBUG, "&&&get");
        // If nothing was found, wait or return nullptr.
        if (wait) {
            LOGS (_log, LOG_LVL_DEBUG, "&&&get wait " << *this);
            _cv.wait(uLock, [this](){ return _changed; });
        } else {
            return ptr;
        }
        LOGS (_log, LOG_LVL_DEBUG, "&&&get wait_end " << *this);
    }
}


void PriorityQueue::prepareShutdown() {
    std::lock_guard<std::mutex> lock(_mtx);
    _shuttingDown = true;
}


void PriorityQueue::_incrDecrRunningCount(util::Command::Ptr const& cmd, int incrDecr) {
    LOGS (_log, LOG_LVL_DEBUG, "&&& _incrDecrRunningCount " <<  incrDecr);
    std::lock_guard<std::mutex> lock(_mtx);
    PriorityCommand::Ptr priCmd = std::dynamic_pointer_cast<PriorityCommand>(cmd);
    if (priCmd != nullptr) {
        int priority = priCmd->_priority;
        auto iter = _queues.find(priority);
        if (iter != _queues.end()) {
            iter->second->running += incrDecr;
            return;
        }
    } else if (cmd != nullptr) {
        // Non-PriorityCommands go on the default queue.
        auto iter = _queues.find(_defaultPriority);
        if (iter != _queues.end()) {
            iter->second->running += incrDecr;
        }
    }
}


void PriorityQueue::commandStart(util::Command::Ptr const& cmd) {
    // Increase running count by 1
    _incrDecrRunningCount(cmd, 1);
}


void PriorityQueue::commandFinish(util::Command::Ptr const& cmd) {
    // Reduce running count by 1
    _incrDecrRunningCount(cmd, -1);
}


std::string PriorityQueue::statsStr() {
    std::lock_guard<std::mutex> lock(_mtx);
    std::stringstream os;
    os << *this;
    return os.str();
}

}}} // namespace lsst:qserv::qdisp
