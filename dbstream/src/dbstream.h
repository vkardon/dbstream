//
// dbstream.h
//

#ifndef _DBSTREAM_H_
#define _DBSTREAM_H_

#include <stdlib.h>
#include <stdint.h>
#include <istream>

#define DB_STREAM_READ_BEGIN  1   // Stream reading begin
#define DB_STREAM_READ_DATA   2   // Stream reading in progress
#define DB_STREAM_READ_END    3   // Stream reading completed

//
// Stream header
//
struct StreamHeader
{
    mutable uint64_t id;
    const char* descr;
    uint8_t type;
    uint64_t timestamp;
    uint64_t size;
};

//
// Interface to DB stream reader
//
struct DBStreamReader
{
    virtual ~DBStreamReader() = default; 
    virtual bool OnRead(const StreamHeader* hdr,
                        unsigned char* data, size_t size,
                        int reading_state) = 0;
};

//
// Interface to DB stream logger
//
struct DBStreamLogger
{
    virtual ~DBStreamLogger() = default; 
    // this fails in agachkpnt.cpp
    // virtual bool IsValid() = 0;
    virtual bool IsValid() {return true;}
    virtual void OnLogInfo(const char* msg) = 0;
    virtual void OnLogError(const char* err) = 0;
};

//
// Interface to DB stream
//
struct DBStream
{
    virtual ~DBStream() = default;
    virtual bool IsValid() = 0;
    virtual void Destroy() = 0;

    virtual bool Write(const StreamHeader* hdr, const unsigned char* data) = 0;
    virtual bool Write(const StreamHeader* hdr, std::istream& data_stream) = 0;

    virtual bool ReadById(uint64_t id_first, bool inclusive_first,
                          uint64_t id_last,  bool inclusive_last) = 0;

    virtual bool DeleteById(uint64_t id_first, bool inclusive_first,
                            uint64_t id_last,  bool inclusive_last) = 0;
    virtual bool DeleteAll() = 0;
    
    virtual bool GetFirst(StreamHeader* hdr) = 0;
    virtual bool GetLast(StreamHeader* hdr) = 0;

    virtual bool LookupById(uint64_t id, bool* found) = 0;

    // Diagnostics
    virtual bool Describe() = 0;
};

extern "C"
{
    __attribute__((visibility("default")))
    DBStream* CreateDBStream(const char* host, const char* user, const char* passwd,
                             const char* database, DBStreamReader* reader,
                             DBStreamLogger* logger);

    typedef DBStream* (*CreateDBStreamPtr)(const char*, const char*, const char*,
                                               const char*, DBStreamReader*,
                                               DBStreamLogger*);
}

#define CREATE_DB_STREAM_FUNC_NAME "CreateDBStream"

#endif // _DBSTREAM_H_

