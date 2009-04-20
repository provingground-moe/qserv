#include "MySqlFs.h"

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysError.hh"

#include "boost/regex.hpp"
#include "mysql/mysql.h"

namespace qWorker = lsst::qserv::worker;

static std::string DUMP_BASE = "/tmp/lspeed/queries/";

qWorker::MySqlFsFile::MySqlFsFile(char* user) : XrdSfsFile(user) {
}

qWorker::MySqlFsFile::~MySqlFsFile(void) {
}

int qWorker::MySqlFsFile::open(
    char const* fileName, XrdSfsFileOpenMode openMode, mode_t createMode,
    XrdSecEntity const* client, char const* opaque) {
    _chunkId = strtod(fileName, 0, 10);
    _userName = std::string(client);
    return SFS_OK;
}

int qWorker::MySqlFsFile::close(void) {
    // optionally remove dump file
    return SFS_OK;
}

int qWorker::MySqlFsFile::fctl(
    int const cmd, char const* args, XrdOucErrInfo& outError) {
    // if rewind: do something
    // else:
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

char const* qWorker::MySqlFsFile::FName(void) {
    return 0;
}

int qWorker::MySqlFsFile::getMmap(void** Addr, off_t &Size) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::read(XrdSfsFileOffset fileOffset,
                          XrdSfsXferSize prereadSz) {
    if (!dumpFileExists(_dbName)) {
        error.setErrInfo(ENOENT, "Query results missing");
        return SFS_ERROR;
    }
    return SFS_OK;
}

XrdSfsXferSize qWorker::MySqlFsFile::read(
    XrdSfsFileOffset fileOffset, char* buffer, XrdSfsXferSize bufferSize) {
    int fd = dumpFileOpen(_dbName);
    if (fd == -1) {
        error.setErrInfo(::errno, "Query results missing");
        return -1;
    }
    off_t pos = lseek(fd, fileOffset, SEEK_SET);
    if (pos == static_cast<off_t>(-1) || pos != fileOffset) {
        error.setErrInfo(::errno, "Unable to seek in query results");
        return -1;
    }
    ssize_t bytes = read(fd, buffer, bufferSize);
    if (bytes == -1) {
        error.setErrInfo(::errno, "Unable to read query results");
        return -1;
    }
    return bytes;
}

int qWorker::MySqlFsFile::read(XrdSfsAio* aioparm) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

XrdSfsXferSize qWorker::MySqlFsFile::write(
    XrdSfsFileOffset fileOffset, char const* buffer,
    XrdSfsXferSize bufferSize) {
    if (fileOffset != 0) {
        error.setErrInfo(EINVAL, "Write beyond beginning of file");
        return -1;
    }
    if (bufferSize <= 0) {
        error.setErrInfo(EINVAL, "No query provided");
        return -1;
    }

    std::string hash = md5_hash_to_string(buffer, bufferSize);
    _dumpName = DUMP_BASE +
        hash.substr(0, 3) + "/" + hash.substr(3, 3) + "/" + hash + ".dump";
    std::string dbName = "q_" + hash;

    if (dumpFileExists()) {
        return bufferSize;
    }

    if (!runScript(std::string(buffer, bufferSize), dbName)) {
        return -1;
    }
    return bufferSize;
}

static std::string runQuery(MYSQL* db, std::string query) {
    if (mysql_real_query(db, query.c_str(), query.size()) != 0) {
        return "Unable to execute query: " + query;
    }
    do {
        MYSQL_RES* result = mysql_store_result(db);
        if (result) {
            mysql_free_result(result);
        }
        else if (mysql_field_count(db) != 0) {
            return "Unable to store result for query: " + query;
        }
        status = mysql_next_result(db);
        if (status > 0) {
            return "Error retrieving results for query: " + query;
        }
    } while (status == 0);
    return std::string();
}

class DbHandle {
public:
    DbHandle(void) : _db(mysql_init(0)) { };
    ~DbHandle(void) {
        if (_db) {
            mysql_close(_db);
            _db = 0;
        }
    };
    MYSQL* get(void) const { return _db; };
private:
    MYSQL* _db;
};

bool qWorker::MySqlFsFile::_runScript(
    std::string const& script, std::string const& dbName) {
    DbHandle db;
    if (mysql_real_connect(db.get(), 0, _userName.c_str(), 0, 0, 0, 0,
                           CLIENT_MULTI_STATEMENTS) == 0) {
        error.setErrInfo(
            EIO, ("Unable to connect to MySQL as " + _userName).c_str());
        return false;
    }

    std::string result = _runQuery(db.get(), "CREATE DATABASE " + dbName);
    if (result.size() != 0) {
        error.setErrInfo(EIO, result.c_str());
        return false;
    }

    if (mysql_select_db(db.get(), dbName.c_str()) != 0) {
        error.setErrInfo(EIO, ("Unable to select database " + dbName).c_str());
        return false;
    }

    boost::regex re("\d+");
    std::string firstLine = query.substr(0, query.find('\n'));
    boost::sregex_iterator i = make_regex_iterator(firstLine, re);
    while (i != boost::sregex_iterator()) {
        std::string subChunk = (*i).str(0);
        std::string processedQuery = (boost::format(query) % subChunk).str();
        result = _runQuery(db.get(), processedQuery);
        if (result.size() != 0) {
            error.setErrInfo(
                EIO, (result + std::endl + "Query: " + processedQuery).c_str());
            return false;
        }
    }

    // mysqldump _dbName

    std::string result = _runQuery(db.get(), "DROP DATABASE " + dbName);
    if (result.size() != 0) {
        error.setErrInfo(EIO, result.c_str());
        return false;
    }

    return true;
}

int qWorker::MySqlFsFile::write(XrdSfsAio* aioparm) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::sync(void) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::sync(XrdSfsAio* aiop) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::stat(struct stat* buf) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::truncate(XrdSfsFileOffset fileOffset) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::getCXinfo(char cxtype[4], int &cxrsz) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}
