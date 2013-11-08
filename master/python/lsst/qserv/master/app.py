#
# LSST Data Management System
# Copyright 2008-2013 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

# app module for lsst.qserv.master
#
# The app module can be thought-of as the top-level code module for
# the qserv frontend's functionality.  It implements the "interesting"
# code that accepts an incoming query and sends it to be
# executed. Most of its logic should eventually be pushed to C++ code
# for greater efficiency and maintainability. The dispatch and query
# management has been migrated over to the C++ layer for efficiency, so
# it makes sense to move closely-related code there to reduce the pain
# of Python-C++ language boundary crossings.
#
# This is the  "high-level application logic" and glue of the qserv
# master/frontend.
#
# InBandQueryAction is the biggest actor in this module. Leftover code
# from older parsing/manipulation/dispatch models may exist and should
# be removed (please open a ticket).
#
# The biggest ugliness here is due to the use of a Python geometry
# module for computing the chunk numbers, given a RA/decl area
# specification. The C++ layer extracts these specifications from the
# query, the code here must pull them out, pass them to the geometry
# module, and then push the resulting specifications down to C++ again
# so that the chunk queries can be dispatched without language
# crossings for each chunk query.
#
# Questions? Contact Daniel L. Wang, SLAC (danielw)
#
# Standard Python imports
import errno
import hashlib
from itertools import chain, imap, ifilter
import os
import cProfile as profile
import pstats
import random
import re
from subprocess import Popen, PIPE
import sys
import threading
import time
import traceback
from string import Template

# Package imports
import metadata
import spatial
import lsst.qserv.master.config
from lsst.qserv.master import geometry
from lsst.qserv.master.geometry import SphericalBox
from lsst.qserv.master.geometry import SphericalConvexPolygon, convexHull
from lsst.qserv.master.db import TaskDb as Persistence
from lsst.qserv.master.db import Db
from lsst.qserv.master import protocol

from lsst.qserv.meta.status import Status, QmsException
from lsst.qserv.meta.client import Client

# SWIG'd functions

# xrdfile - raw xrootd access
from lsst.qserv.master import xrdOpen, xrdClose, xrdRead, xrdWrite
from lsst.qserv.master import xrdLseekSet, xrdReadStr
from lsst.qserv.master import xrdReadToLocalFile, xrdOpenWriteReadSaveClose

from lsst.qserv.master import charArray_frompointer, charArray

# transaction
from lsst.qserv.master import TransactionSpec

# Dispatcher
from lsst.qserv.master import newSession, discardSession
from lsst.qserv.master import setupQuery, getSessionError
from lsst.qserv.master import getConstraints, addChunk, ChunkSpec
from lsst.qserv.master import getDominantDb
from lsst.qserv.master import configureSessionMerger3, submitQuery3


from lsst.qserv.master import submitQuery, submitQueryMsg
from lsst.qserv.master import initDispatcher
from lsst.qserv.master import tryJoinQuery, joinSession
from lsst.qserv.master import getQueryStateString, getErrorDesc
from lsst.qserv.master import SUCCESS as QueryState_SUCCESS
from lsst.qserv.master import pauseReadTrans, resumeReadTrans
# Parser
from lsst.qserv.master import ChunkMeta
# Merger
from lsst.qserv.master import TableMerger, TableMergerError, TableMergerConfig
from lsst.qserv.master import configureSessionMerger, getSessionResultName

# Metadata
from lsst.qserv.master import newMetadataSession, discardMetadataSession
from lsst.qserv.master import addDbInfoNonPartitioned
from lsst.qserv.master import addDbInfoPartitionedSphBox
from lsst.qserv.master import addTbInfoNonPartitioned
from lsst.qserv.master import addTbInfoPartitionedSphBox
from lsst.qserv.master import printMetadataCache

# Experimental interactive prompt (not currently working)
import code, traceback, signal


# Constant, long-term, this should be defined differently
dummyEmptyChunk = 1234567890
CHUNK_COL = "chunkId"
SUBCHUNK_COL = "subChunkId"

def debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d={'_frame':frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message  = "Signal recieved : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    i.interact(message)

def listen():
    """Register debug() as a signal handler to SIGUSR1"""
    signal.signal(signal.SIGUSR1, debug)  # Register handler
listen()


######################################################################
## Methods
######################################################################
## MySQL interface helpers
def computeShortCircuitQuery(query, conditions):
    """Return the result for a short-circuit query, or None if query is
    not a known short-circuit query."""
    if query == "select current_user()":
        return ("qserv@%","","")
    return # not a short circuit query.

## Helpers
def getResultTable(tableName):
    """Spawn a subprocess to invoke mysql and retrieve the rows of
    table tableName."""
    # Minimal sanitizing
    tableName = tableName.strip()
    assert not filter(lambda x: x in tableName, ["(",")",";"])
    sqlCmd = "SELECT * FROM %s;" % tableName

    # Get config
    config = lsst.qserv.master.config.config
    socket = config.get("resultdb", "unix_socket")
    db = config.get("resultdb", "db")
    mysql = config.get("mysql", "mysqlclient")
    if not mysql:
        mysql = "mysql"

    # Execute
    cmdList = [mysql, "--socket", socket, db]
    p = Popen(cmdList, bufsize=0,
              stdin=PIPE, stdout=PIPE, close_fds=True)
    (outdata,errdummy) = p.communicate(sqlCmd)
    p.stdin.close()
    if p.wait() != 0:
        return "Error getting table %s." % tableName, outdata
    return outdata

######################################################################
## Classes
######################################################################

_defaultMetadataCacheSessionId = None

class MetadataCacheIface:
    """MetadataCacheIface encapsulates logic to prepare, metadata
       information by fetching it from qserv metadata server into
       C++ memory structure. It is stateless. Throws exceptions on
       failure."""

    def getDefaultSessionId(self):
        """Returns default sessionId. It will initialize it if needed.
           Note: initialization involves contacting qserv metadata
           server (qms). This is the only time qserv talks to qms.
           This function throws QmsException if case of problems."""
        global _defaultMetadataCacheSessionId
        if _defaultMetadataCacheSessionId is None:
            _defaultMetadataCacheSessionId = self.newSession()
            self.printSession(_defaultMetadataCacheSessionId)
        return _defaultMetadataCacheSessionId

    def newSession(self):
        """Creates a new session: assigns sessionId, populates the C++
           cache and returns the sessionId."""
        sessionId = newMetadataSession()
        qmsClient = Client(
            lsst.qserv.master.config.config.get("metaServer", "host"),
            int(lsst.qserv.master.config.config.get("metaServer", "port")),
            lsst.qserv.master.config.config.get("metaServer", "user"),
            lsst.qserv.master.config.config.get("metaServer", "passwd"))
        self._fetchAllData(sessionId, qmsClient)
        return sessionId

    def printSession(self, sessionId):
        printMetadataCache(sessionId)

    def discardSession(self, sessionId):
        discardMetadataSession(sessionId)

    def _fetchAllData(self, sessionId, qmsClient):
        dbs = qmsClient.listDbs()
        for dbName in dbs:
            partStrategy = self._addDb(dbName, sessionId, qmsClient);
            tables = qmsClient.listTables(dbName)
            for tableName in tables:
                self._addTable(dbName, tableName, partStrategy, sessionId, qmsClient)

    def _addDb(self, dbName, sessionId, qmsClient):
        # retrieve info about each db
        x = qmsClient.retrieveDbInfo(dbName)
        # call the c++ function
        if x["partitioningStrategy"] == "sphBox":
            #print "add partitioned, ", db, x
            ret = addDbInfoPartitionedSphBox(
                sessionId, dbName,
                int(x["stripes"]),
                int(x["subStripes"]),
                float(x["defaultOverlap_fuzziness"]),
                float(x["defaultOverlap_nearNeigh"]))
        elif x["partitioningStrategy"] == "None":
            #print "add non partitioned, ", db
            ret = addDbInfoNonPartitioned(sessionId, dbName)
        else:
            raise QmsException(Status.ERR_INVALID_PART)
        if ret != 0:
            if ret == -1: # the dbInfo does not exist
                raise QmsException(Status.ERR_DB_NOT_EXISTS)
            if ret == -2: # the table is already there
                raise QmsException(Status.ERR_TABLE_EXISTS)
            raise QmsException(Status.ERR_INTERNAL)
        return x["partitioningStrategy"]

    def _addTable(self, dbName, tableName, partStrategy, sessionId, qmsClient):
        # retrieve info about each db
        x = qmsClient.retrieveTableInfo(dbName, tableName)
        # call the c++ function
        if partStrategy == "sphBox": # db is partitioned
            if "overlap" in x:       # but this table does not have to be
                ret = addTbInfoPartitionedSphBox(
                    sessionId,
                    dbName,
                    tableName,
                    float(x["overlap"]),
                    x["phiCol"],
                    x["thetaCol"],
                    x["objIdCol"],
                    int(x["phiColNo"]),
                    int(x["thetaColNo"]),
                    int(x["objIdColNo"]),
                    int(x["logicalPart"]),
                    int(x["physChunking"]))
            else:                    # db is not partitioned
                ret = addTbInfoNonPartitioned(sessionId, dbName, tableName)
        elif partStrategy == "None":
            ret = addTbInfoNonPartitioned(sessionId, dbName, tableName)
        else:
            raise QmsException(Status.ERR_INVALID_PART)
        if ret != 0:
            if ret == -1: # the dbInfo does not exist
                raise QmsException(Status.ERR_DB_NOT_EXISTS)
            if ret == -2: # the table is already there
                raise QmsException(Status.ERR_TABLE_EXISTS)
            raise QmsException(Status.ERR_INTERNAL)

########################################################################

class TaskTracker:
    def __init__(self):
        self.tasks = {}
        self.pers = Persistence()
        pass

    def track(self, name, task, querytext):
        # create task in db with name
        taskId = self.pers.addTask((None, querytext))
        self.tasks[taskId] = {"task" : task}
        return taskId

    def task(self, taskId):
        return self.tasks[taskId]["task"]

########################################################################

class SleepingThread(threading.Thread):
    def __init__(self, howlong=1.0):
        self.howlong=howlong
        threading.Thread.__init__(self)
    def run(self):
        time.sleep(self.howlong)

########################################################################
class QueryHintError(Exception):
    """An error in handling query hints"""
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return repr(self.reason)
class ParseError(Exception):
    """An error in parsing the query"""
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return repr(self.reason)
########################################################################
def setupResultScratch():
    """Prepare the configured scratch directory for use, creating if
    necessary and checking for r/w access. """
    # Make sure scratch directory exists.
    cm = lsst.qserv.master.config
    c = lsst.qserv.master.config.config

    scratchPath = c.get("frontend", "scratch_path")
    try: # Make sure the path is there
        os.makedirs(scratchPath)
    except OSError, exc:
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise cm.ConfigError("Bad scratch_dir")
    # Make sure we can read/write the dir.
    if not os.access(scratchPath, os.R_OK | os.W_OK):
        raise cm.ConfigError("No access for scratch_path(%s)" % scratchPath)
    return scratchPath

class IndexLookup:
    def __init__(self, db, table, keyColumn, keyVals):
        self.db = db
        self.table = table
        self.keyColumn = keyColumn
        self.keyVals = keyVals
        pass
class SecondaryIndex:
## FIXME: subchunk index creation
##    "SELECT DISTINCT %s, %s FROM %s" % (CHUNK_COL, SUBCHUNK_COL, table)

    def lookup(self, indexLookups):
        sqls = []
        for lookup in indexLookups:
            table = metadata.getIndexNameForTable("%s.%s" % (lookup.db,
                                                             lookup.table))
            keys = ",".join(lookup.keyVals)
            condition = "%s IN (%s)" % (lookup.keyColumn, keys)
            sql = "SELECT %s FROM %s WHERE %s" % (CHUNK_COL, table, condition)
            sqls.append(sql)
        if not sqls:
            return
        sql = " UNION ".join(sqls)
        db = Db()
        db.activate()
        cids = db.applySql(sql)
        try:
            print "cids are ", cids
            cids = map(lambda t: t[0], cids)
        except:
            raise QueryHintError("mysqld error during index lookup q=" + sql)
        del db
        return cids

########################################################################
class InbandQueryAction:
    """InbandQueryAction is an action which represents a user-query
    that is executed using qserv in many pieces. It borrows a little
    from HintedQueryAction, but uses different abstractions
    underneath.
    """
    def __init__(self, query, hints, reportError, resultName=""):
        """Construct an InbandQueryAction
        @param query SQL query text (SELECT...)
        @param hints dict containing query hints and context
        @param reportError unary function that accepts the description
                           of an encountered error.
        @param resultName name of result table for query results."""
        ## Fields with leading underscores are internal-only
        ## Those without leading underscores may be read by clients
        self.queryStr = query.strip()# Pull trailing whitespace
        # Force semicolon to facilitate worker-side splitting
        if self.queryStr[-1] != ";":  # Add terminal semicolon
            self.queryStr += ";"

        # queryHash identifies the top-level query.
        self.queryHash = self._computeHash(self.queryStr)[:18]
        self.chunkLimit = 2**32 # something big
        self.isValid = False

        self.hints = hints
        self.hintList = [] # C++ parser-extracted hints only.

        self._importQconfig()
        self._invokeLock = threading.Semaphore()
        self._invokeLock.acquire() # Prevent res-retrieval before invoke
        self._resultName = resultName
        self._reportError = reportError
        try:
            self.metaCacheSession = MetadataCacheIface().getDefaultSessionId()
            self._prepareForExec()
            self.isValid = True
        except QueryHintError, e:
            self._error = str(e)
        except ParseError, e:
            self._error = str(e)
        except:
            self._error = "Unexpected error: " + str(sys.exc_info())
            print self._error, traceback.format_exc()
        pass

    def invoke(self):
        """Begin execution of the query"""
        self._execAndJoin()
        self._invokeLock.release()

    def getError(self):
        """@return description of last error encountered. """
        try:
            return self._error
        except:
            return "Unknown error InbandQueryAction"

    def getResult(self):
        """Wait for query to complete (as necessary) and then return
        a handle to the result.
        @return name of result table"""
        self._invokeLock.acquire()
        # Make sure it's joined.
        self._invokeLock.release()
        return self._resultName

    def getIsValid(self):
        return self.isValid

    def _computeHash(self, bytes):
        return hashlib.md5(bytes).hexdigest()

    def _prepareForExec(self):
        """Prepare data structures and objects for query execution"""
        self.hints = self.hints.copy() # make a copy
        self._dbContext = self.hints.get("db", "")

        cfg = self._prepareCppConfig()
        self.sessionId = newSession(cfg)
        setupQuery(self.sessionId, self.queryStr, self._resultName)
        errorMsg = getSessionError(self.sessionId)
        if errorMsg: raise ParseError(errorMsg)

        self._applyConstraints()
        self._prepareMerger()
        pass

    def _evaluateHints(self, dominantDb, hintList, pmap):
        """Modify self.fullSky and self.partitionCoverage according to
        spatial hints. This is copied from older parser model."""
        self._isFullSky = True
        self._intersectIter = pmap
        if hintList:
            regions = self._computeSpatialRegions(hintList)
            indexRegions = self._computeIndexRegions(hintList)

            if regions != []:
                # Remove the region portion from the intersection tuple
                self._intersectIter = map(
                    lambda i: (i[0], map(lambda j:j[0], i[1])),
                    pmap.intersect(regions))
                self._isFullSky = False
            if indexRegions:
                if regions != []:
                    self._intersectIter = chain(self._intersectIter, indexRegions)
                else:
                    self._intersectIter = map(lambda i: (i,[]), indexRegions)
                self._isFullSky = False
                if not self._intersectIter:
                    self._intersectIter = [(dummyEmptyChunk, [])]
        # _isFullSky indicates that no spatial hints were found.
        # However, if spatial tables are not found in the query, then
        # we should use the dummy chunk so the query can be dispatched
        # once to a node of the balancer's choosing.

        # If hints only apply when partitioned tables are in play.
        # FIXME: we should check if partitionined tables are being accessed,
        # and then act to support the heaviest need (e.g., if a chunked table
        # is being used, then issue chunked queries).
        #print "Affected chunks: ", [x[0] for x in self._intersectIter]
        pass

    def _applyConstraints(self):
        """Extract constraints from parsed query(C++), re-marshall values,
        call evaluateHints, and add the chunkIds into the query(C++) """
        # Retrieve constraints as (name, [param1,param2,param3,...])
        self.constraints = getConstraints(self.sessionId)
        #print "Getting constraints", self.constraints, "size=",self.constraints.size()
        dominantDb = getDominantDb(self.sessionId)
        self.pmap = spatial.makePmap(dominantDb, self.metaCacheSession)

        def iterateConstraints(constraintVec):
            for i in range(constraintVec.size()):
                yield constraintVec.get(i)
        for constraint in iterateConstraints(self.constraints):
            print "constraint=", constraint
            params = [constraint.paramsGet(i)
                      for i in range(constraint.paramsSize())]
            self.hints[constraint.name] = params
            self.hintList.append((constraint.name, params))
            pass
        self._evaluateHints(dominantDb, self.hintList, self.pmap)
        self._emptyChunks = metadata.getEmptyChunks(dominantDb)
        class RangePrint:
            def __init__(self):
                self.last = -1
                self.first = -1
                pass
            def add(self, chunkId):
                if self.last != (chunkId -1):
                    if(self.last != -1): self._print()
                    self.first = chunkId
                self.last = chunkId
                pass
            def _print(self):
                        if(self.last - self.first > 1):
                            print "Rejecting chunks: %d-%d" %(self.first,
                                                              self.last)
                        else: print "Rejecting chunk: %d" %(self.last)
            def finish(self):
                self._print()
            pass
        rPrint = RangePrint()
        count = 0
        chunkLimit = self.chunkLimit
        for chunkId, subIter in self._intersectIter:
            if chunkId in self._emptyChunks:
                rPrint.add(chunkId)
                continue
            #prepare chunkspec
            c = ChunkSpec()
            c.chunkId = chunkId
            scount=0
            sList = [s for s in subIter]
            #for s in sList: # debugging
            #    c.addSubChunk(s)
            #    scount += 1
            #    if scount > 7: break ## FIXME: debug now.
            map(c.addSubChunk, sList)
            addChunk(self.sessionId, c)
            count += 1
            if count >= chunkLimit: break
        if count == 0:
            c = ChunkSpec()
            c.chunkId = dummyEmptyChunk
            scount=0
            addChunk(self.sessionId, c)
        rPrint.finish()
        pass


    def _execAndJoin(self):
        """Signal dispatch to C++ layer and block until execution completes"""
        lastTime = time.time()
        submitQuery3(self.sessionId)
        elapsed = time.time() - lastTime
        print "Query dispatch (%s) took %f seconds" % (self.sessionId,
                                                       elapsed)
        lastTime = time.time()
        s = joinSession(self.sessionId)
        elapsed = time.time() - lastTime
        print "Query exec (%s) took %f seconds" % (self.sessionId,
                                                   elapsed)

        if s != QueryState_SUCCESS:
            self._reportError(getErrorDesc(self.sessionId))
        print "Final state of all queries", getQueryStateString(s)
        if not self.isValid:
            discardSession(self.sessionId)
            return

    def _importQconfig(self):
        """Import config file settings into self"""
        # Config preparation
        cModule = lsst.qserv.master.config

        # chunk limit: For debugging
        cfgLimit = int(cModule.config.get("debug", "chunkLimit"))
        if cfgLimit > 0:
            self.chunkLimit = cfgLimit
            print "Using debugging chunklimit:",cfgLimit

        # Memory engine(unimplemented): Buffer results/temporaries in
        # memory on the master. (no control over worker)
        self._useMemory = cModule.config.get("tuning", "memoryEngine")
        return True

    def _prepareCppConfig(self):
        """Construct a C++ stringmap for passing settings and context
        to the C++ layer.
        @return the C++ StringMap object """
        cfg = lsst.qserv.master.config.getStringMap()
        cfg["frontend.scratchPath"] = setupResultScratch()
        cfg["table.defaultdb"] = self._dbContext
        cfg["query.hints"] = ";".join(
            map(lambda (k,v): k + "," + str(v), self.hints.items()))
        cfg["table.result"] = self._resultName
        cfg["runtime.metaCacheSession"] = str(self.metaCacheSession)
        return cfg

    def _computeIndexRegions(self, hintList):
        """Compute spatial region coverage based on hints.
        @return list of regions"""
        print "Looking for indexhints in ", hintList
        secIndexSpecs = ifilter(lambda t: t[0] == "sIndex", hintList)
        lookups = []
        for s in secIndexSpecs:
            params = s[1]
            lookup = IndexLookup(params[0], params[1], params[2], params[3:])
            lookups.append(lookup)
            pass
        index = SecondaryIndex()
        chunkIds = index.lookup(lookups)
        print "lookup got chunks:", chunkIds
        return chunkIds

    def _computeSpatialRegions(self, hintList):
        """Compute spatial region coverage based on hints.
        @return list of regions"""
        r = spatial.getRegionFactory()
        regs = r.getRegionFromHint(hintList)
        if regs != None:
            return regs
        else:
            if r.errorDesc:
                # How can we give a good error msg to the client?
                s = "Error parsing hint string %s"
                raise QueryHintError(s % r.errorDesc)
            return []
        pass

    def _prepareMerger(self):
        """Prepare session merger to handle incoming results."""
        c = lsst.qserv.master.config.config
        dbSock = c.get("resultdb", "unix_socket")
        dbUser = c.get("resultdb", "user")
        dbName = c.get("resultdb", "db")
        dropMem = c.get("resultdb","dropMem")

        mysqlBin = c.get("mysql", "mysqlclient")
        if not mysqlBin:
            mysqlBin = "mysql"
        configureSessionMerger3(self.sessionId)
        pass


    pass # class InbandQueryAction

class KillQueryAction:
    def __init__(self, query):
        self.query = query
        pass
    def invoke(self):
        print "invoking kill query", self.query
        return "Unimplemented"

class CheckAction:
    def __init__(self, tracker, handle):
        self.tracker = tracker
        self.texthandle = handle
        pass
    def invoke(self):
        self.results = None
        id = int(self.texthandle)
        t = self.tracker.task(id)
        if t:
            self.results = 50 # placeholder. 50%

########################################################################
########################################################################
########################################################################

#see if it's better to not bother with an action object
def results(tracker, handle):
        id = int(handle)
        t = tracker.task(id)
        if t:
            return "Some host with some port with some db"
        return None

def clauses(col, cmin, cmax):
    return ["%s between %smin and %smax" % (cmin, col, col),
            "%s between %smin and %smax" % (cmax, col, col),
            "%smin between %s and %s" % (col, cmin, cmax)]
