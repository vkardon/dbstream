//
// mysqlstream.h
//

#ifndef _MYSQLSTREAM_H_
#define _MYSQLSTREAM_H_

#include <stdlib.h>
#include <stdint.h>
#include <cppconn/connection.h>
#include "dbstream.h"

//
// MySQL stream 
//
class MySqlStream : public DBStream
{
private:
    // Private constructor/destructor to force using Create/Destroy methods
    MySqlStream(const char* host, const char* user, const char* passwd,
                    const char* database, DBStreamReader* reader,
                    DBStreamLogger* logger);
    virtual ~MySqlStream() = default; 
    MySqlStream& operator=(const MySqlStream&) = delete; // Don't allow class copy

    // Class data
private:
    DBStreamReader* mReader = NULL;
    DBStreamLogger* mLogger = NULL;
    std::unique_ptr<sql::Connection> mCon;

    // The maximum length of the BLOB column is 65535 (2^16-1) bytes
    unsigned char mBuf[65535];

    // Methods
public:
    static MySqlStream* Create(const char* host, const char* user, const char* passwd,
                                   const char* database, DBStreamReader* reader,
                                   DBStreamLogger* logger)
    {
        return new MySqlStream(host, user, passwd, database, reader, logger);
    }
    
    //
    // Implementation of the DBStream interface
    //
    virtual bool IsValid() { return mCon.get(); }
    virtual void Destroy() { /*(this != NULL)*/ delete this; }

    virtual bool Write(const StreamHeader* hdr, const unsigned char* data);
    virtual bool Write(const StreamHeader* hdr, std::istream& data_stream);

    virtual bool ReadById(uint64_t id_first, bool inclusive_first,
                          uint64_t id_last,  bool inclusive_last);

    virtual bool DeleteById(uint64_t id_first, bool inclusive_first,
                            uint64_t id_last,  bool inclusive_last);
    virtual bool DeleteAll();

    virtual bool GetFirst(StreamHeader* hdr);
    virtual bool GetLast(StreamHeader* hdr);

    virtual bool LookupById(uint64_t id, bool* found);

    // Diagnostics
    virtual bool Describe();

private:
    bool InitDatabase(const char* database);
    bool InitTranTable(sql::DatabaseMetaData& con_meta);
    bool InitTranDataTable(sql::DatabaseMetaData& con_meta);
    bool LookupTable(sql::DatabaseMetaData& con_meta, const char* table, bool* found);

    bool Read(const char* column,
              uint64_t first, bool inclusive_first,
              uint64_t last,  bool inclusive_last);
    bool Delete(const char* column,
                uint64_t first, bool inclusive_first,
                uint64_t last,  bool inclusive_last,
                bool reset_id=false);

    bool Lookup(const char* column, uint64_t val, bool* found);
    bool Get(StreamHeader* hdr, const char* order);

    bool ReadData(const StreamHeader& hdr, bool* stopped);

    // Logging support
    enum LOG_TYPE { LOG_ERR=1, LOG_INFO };

    void WriteToLog(LOG_TYPE type, const char* msg);
    void WriteToLog(LOG_TYPE type, const std::string& msg) { WriteToLog(type, msg.c_str()); }
    void WriteToLog(LOG_TYPE type, const std::stringstream& msg) { WriteToLog(type, msg.str().c_str()); }
};

#endif // _MYSQLSTREAM_H_

