//
// mysqlstream.cpp
//
#include <stdlib.h>
#include <sstream>
#include <string.h>
#include <stdio.h>  // sprintf
#include "mysqlstream.h"
#include "streambuf.h"

#include <cppconn/exception.h>
#include <cppconn/metadata.h>
#include <cppconn/prepared_statement.h>
#include <driver/mysql_driver.h>
//#include <mysqld_error.h> // MySql server include file

#define MODULE_NAME       "MySqlStream"

#define DB_ENGINE         "InnoDB";      // Database engine type
#define STREAM_TABLE      "stream"       // Stream table
#define STREAMDATA_TABLE  "streamdata"   // Stream data table

const size_t STREAMS_PER_QUERY = 100; // Max number of streams per query
//const size_t STREAMS_PER_QUERY = 5; // Max number of streams per query


DBStream* CreateDBStream(const char* host, const char* user, const char* passwd,
                             const char* database, DBStreamReader* reader,
                             DBStreamLogger* logger)
{
    MySqlStream* mysqlStream = MySqlStream::Create(host, user, passwd, database, reader, logger);
    
    if(mysqlStream != NULL && !mysqlStream->IsValid())
    {
        mysqlStream->Destroy();
        mysqlStream = NULL;
    }
    
    return mysqlStream;
}

//
// Helpers to READ/WRITE lock/unlock tables
//
struct SqlLock
{
    enum LOCK_TYPE : char { LOCK_READ=1, LOCK_WRITE };

    SqlLock(const std::unique_ptr<sql::Statement>& s, LOCK_TYPE type) : _s(s.get())  { Lock(type); }
    SqlLock(sql::Statement* s, LOCK_TYPE type) : _s(s) { Lock(type); }
    ~SqlLock() { Unlock(); }
    SqlLock& operator=(const SqlLock&) = delete; // Don't allow class copy

    inline void Lock(LOCK_TYPE type)
    {
        if(type == LOCK_READ)
            _s->execute("LOCK TABLES " STREAM_TABLE " READ LOCAL, " STREAMDATA_TABLE " READ LOCAL");
        else
            _s->execute("LOCK TABLES " STREAM_TABLE " WRITE, " STREAMDATA_TABLE " WRITE");
    }

    inline void Unlock() { _s->execute("UNLOCK TABLES"); }

private:
    sql::Statement* _s;
};

struct SqlLockRead : public SqlLock
{
    SqlLockRead(const std::unique_ptr<sql::Statement>& s) : SqlLock(s, LOCK_READ) {}
};

struct SqlLockWrite : public SqlLock
{
    SqlLockWrite(const std::unique_ptr<sql::Statement>& s) : SqlLock(s, LOCK_WRITE) {}
};


//
// NOTE: The MySql Connector/C++ throws three different exceptions:
// - sql::MethodNotImplementedException (derived from sql::SQLException)
// - sql::InvalidArgumentException (derived from sql::SQLException)
// - sql::SQLException (derived from std::runtime_error)
//
#define TRY  try
#define CATCH                                                                                       \
    catch(sql::SQLException& e)                                                                     \
    {                                                                                               \
        /*if(e.getErrorCode() == ER_LOCK_WAIT_TIMEOUT)*/                                            \
        std::stringstream err;                                                                      \
        err << "ERROR: " << __FILE__ << ":" << __LINE__ << ": " << __func__ << ":" << std::endl;    \
        err << "ERROR: SQLException: " << e.what()                                                  \
            << ", MySql error code: " << e.getErrorCode()                                           \
            << ", SQLState: " << e.getSQLState();                                                   \
        WriteToLog(LOG_ERR, err);                                                                   \
    }                                                                                               \
    catch(std::runtime_error& e)                                                                    \
    {                                                                                               \
        std::stringstream err;                                                                      \
        err << "ERROR: " << __FILE__ << ":" << __LINE__ << ": " << __func__ << ":" << std::endl;    \
        err << "ERROR: runtime_error: " << e.what();                                                \
        WriteToLog(LOG_ERR, err);                                                                   \
    }                                                                                               \
    catch(...)                                                                                      \
    {                                                                                               \
        std::stringstream err;                                                                      \
        err << "ERROR: " << __FILE__ << ":" << __LINE__ << ": " << __func__ << ":" << std::endl;    \
        err << "ERROR: Unknown error" << std::endl;                                                 \
        WriteToLog(LOG_ERR, err);                                                                   \
    }                                                                                               \

#define THROW(msg)                                              \
    {                                                           \
        std::stringstream err;                                  \
        err << __func__ << "(" << __LINE__ << "): " << msg;     \
        throw std::runtime_error(err.str());                    \
    }                                                           \


//
// MySqlStream implementation
//
MySqlStream::MySqlStream(const char* host, const char* user, const char* passwd,
                                 const char* database, DBStreamReader* reader,
                                 DBStreamLogger* logger) : mReader(reader), mLogger(logger)
{
    TRY
    {
        // Using the Driver to create a connection
        sql::Driver* driver = sql::mysql::get_driver_instance();
        mCon = std::unique_ptr<sql::Connection>(driver->connect(host, user, passwd));

        // Init database
        if(!InitDatabase(database))
        {
            mCon->close();
            mCon.reset(); // Delete connection object
        }

        memset(mBuf, 0, sizeof(mBuf));
    }
    CATCH
}

void MySqlStream::WriteToLog(LOG_TYPE type, const char* msg)
{
    if(mLogger == NULL)
        return;
    
    if(type == LOG_ERR)
        mLogger->OnLogError(msg);
    else if(type == LOG_INFO)
        mLogger->OnLogInfo(msg);
}

bool MySqlStream::InitDatabase(const char* database)
{
    TRY
    {
        sql::DatabaseMetaData* con_meta = mCon->getMetaData();

        std::stringstream msg;
        msg << MODULE_NAME ": CDBC (API) major version = " << con_meta->getCDBCMajorVersion();
        WriteToLog(LOG_INFO, msg);

        if (con_meta->getCDBCMajorVersion() <= 0)
            THROW("API major version must not be 0");

        msg.str("");
        msg << MODULE_NAME ": CDBC (API) minor version = " << con_meta->getCDBCMinorVersion();
        WriteToLog(LOG_INFO, msg);

        // Does schema/database exist?
        // NOTE Connector C++ defines catalog = n/a, schema = MySql database
        std::unique_ptr<sql::ResultSet> res(con_meta->getSchemas());
        if(res.get() == NULL)
            THROW("getSchemas failed");

        bool hasSchema = false;
        while(res->next())
        {
            if(res->getString("TABLE_SCHEM") == database)
            {
                hasSchema = true;
                break;
            }
        }

        if(!hasSchema)
           THROW("The database '" + std::string(database) + "' does not exist");
        
        // Set schema
        mCon->setSchema(database);
        //std::unique_ptr<sql::Statement> stmt(con->createStatement());
        //stmt->execute("USE " DB_NAME);

        // test
        // Drop table if exist
        //std::unique_ptr<sql::Statement> stmt(mCon->createStatement());
        //stmt->execute("DROP TABLE IF EXISTS " STREAM_TABLE);
        // test end

        // Do we have a table?
        if(!InitTranTable(*con_meta) || !InitTranDataTable(*con_meta))
            THROW("InitTable failed");

        return true;
    }
    CATCH

    return false;
}

bool MySqlStream::LookupTable(sql::DatabaseMetaData& con_meta, const char* table, bool* found)
{
    TRY
    {
        // Do we already have table?
        std::list<sql::SQLString> table_types;
        table_types.push_back("TABLE");
        std::unique_ptr<sql::ResultSet> res(con_meta.getTables(mCon->getCatalog(), mCon->getSchema(), table, table_types));
        if(res.get() == NULL)
            THROW("getTables failed for db='" + mCon->getSchema() + "', table='" + std::string(table) + "'");

        *found = false;
        while(res->next())
        {
            if(res->getString("TABLE_NAME") == table)
            {
                *found = true;
                break;
            }
        }

        return true;
    }
    CATCH

    return false;
}

bool MySqlStream::InitTranTable(sql::DatabaseMetaData& con_meta)
{
    TRY
    {
        // Do we already have table?
        bool hasTable = false;
        if(!LookupTable(con_meta, STREAM_TABLE, &hasTable))
            THROW("LookupTable failed");

        if(hasTable)
        {
            WriteToLog(LOG_INFO, MODULE_NAME ": The table '" STREAM_TABLE "' exists.");
        }
        else
        {
            WriteToLog(LOG_INFO, MODULE_NAME ": The table '" STREAM_TABLE "' does not exist. Create...");

            // Create table is not exist
            sql::SQLString sql = "CREATE TABLE IF NOT EXISTS " STREAM_TABLE " ("
                                 "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, "
                                 "descr VARCHAR(120) NOT NULL default '', "
                                 "type TINYINT UNSIGNED NOT NULL DEFAULT '0', "
                                 "size BIGINT UNSIGNED NOT NULL DEFAULT '0', "
                                 "timestamp BIGINT UNSIGNED NOT NULL DEFAULT '0', "
                                 "PRIMARY KEY(id)) ENGINE=" DB_ENGINE;

            WriteToLog(LOG_INFO, sql);

            std::unique_ptr<sql::Statement> stmt(mCon->createStatement());
            stmt->execute(sql);

            WriteToLog(LOG_INFO, MODULE_NAME ": The table '" STREAM_TABLE "' created.");
        }

        return true;
    }
    CATCH

    return false;
}

bool MySqlStream::InitTranDataTable(sql::DatabaseMetaData& con_meta)
{
    TRY
    {
        // Do we already have table?
        bool hasTable = false;
        if(!LookupTable(con_meta, STREAMDATA_TABLE, &hasTable))
            THROW("LookupTable failed");

        if(hasTable)
        {
            WriteToLog(LOG_INFO, MODULE_NAME ": The table '" STREAMDATA_TABLE "' exists.");
        }
        else
        {
            WriteToLog(LOG_INFO, MODULE_NAME ": The table '" STREAMDATA_TABLE "' does not exist. Create...");

            // Create table is not exist
            sql::SQLString sql = "CREATE TABLE IF NOT EXISTS " STREAMDATA_TABLE " ("
                                 "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, "
                                 "masterid BIGINT UNSIGNED NOT NULL DEFAULT '0', "
                                 "data BLOB NOT NULL, "
                                 "PRIMARY KEY(id), "
                                 "FOREIGN KEY(masterid) "
                                 "REFERENCES " STREAM_TABLE "(id) "
                                 "ON DELETE CASCADE) ENGINE=" DB_ENGINE;

            WriteToLog(LOG_INFO, sql);

            std::unique_ptr<sql::Statement> stmt(mCon->createStatement());
            stmt->execute(sql);

            WriteToLog(LOG_INFO, MODULE_NAME ": The table '" STREAMDATA_TABLE "' created.");
        }

        return true;
    }
    CATCH

    return false;
}

bool MySqlStream::Describe()
{
    TRY
    {
        // Describe the actual table (if exists)
        const char* tables[] = { STREAM_TABLE, STREAMDATA_TABLE, NULL };

        std::unique_ptr<sql::Statement> stmt(mCon->createStatement());

        for(int i=0; tables[i] != NULL; i++)
        {
            std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(sql::SQLString("DESCRIBE ") + tables[i]));

            if(res->rowsCount() > 0)
            {
                // Table exists
                char buf[256] = {0};
                const char* separator = "+...........................+......................+";

                WriteToLog(LOG_INFO, MODULE_NAME ": Table '" + std::string(tables[i]) + "':");
                WriteToLog(LOG_INFO, separator);
                sprintf(buf, "| %-25s | %-20s |", "Field", "Type");
                WriteToLog(LOG_INFO, buf);
                WriteToLog(LOG_INFO, separator);

                while(res->next())
                {
                    sprintf(buf, "| %-25s | %-20s |", res->getString("Field").c_str(), res->getString("Type").c_str());
                    WriteToLog(LOG_INFO, buf);
                }

                WriteToLog(LOG_INFO, separator);
            }
            else
            {
                // Table does not yet exist
                WriteToLog(LOG_INFO, MODULE_NAME ": Table '" + std::string(tables[i]) + "' does not exist.");
            }
        }

        return true;
    }
    CATCH

    return false;
}

bool MySqlStream::Write(const StreamHeader* hdr, const unsigned char* data)
{
    if(hdr == NULL)
        THROW("StreamHeader* hdr is NULL");
    if(data == NULL)
        THROW("data is NULL");

    return Write(hdr, StreamBuf(data, hdr->size));
}

bool MySqlStream::Write(const StreamHeader* hdr, std::istream& data_stream)
{
    TRY
    {
        if(hdr == NULL)
            THROW("StreamHeader* hdr is NULL");

        // Disable autocommit as we are going to change into transaction mode
        mCon->setAutoCommit(false);

        // Insert master stream record into stream table
        std::unique_ptr<sql::Statement> tran_stmt(mCon->createStatement());

        char sql[256] = {0};
        sprintf(sql, "INSERT INTO " STREAM_TABLE " (descr, type, timestamp) VALUES ('%s', %hhu, %llu)",
                hdr->descr, hdr->type, (long long unsigned int)hdr->timestamp);
        tran_stmt->execute(sql);

        // Get the id of the just inserted stream record
        std::unique_ptr<sql::ResultSet> res(tran_stmt->executeQuery("SELECT LAST_INSERT_ID()"));
        if(res->rowsCount() == 0)
            THROW("Statement::executeQuery failed for LAST_INSERT_ID()");

        if(!res->next())
            THROW("ResultSet::next failed");

        uint64_t master_id = res->getUInt64(1);

        // We are going to use Prepared Statement to insert stream data
        std::unique_ptr<sql::PreparedStatement> data_stmt(mCon->prepareStatement(
                "INSERT INTO " STREAMDATA_TABLE " (masterid, data) VALUES (?,?)"));

        // Note: The maximum length of the BLOB column is 65535 (2^16-1) bytes
        uint64_t size_total = 0;

        while(data_stream)
        {
            data_stream.read((char*)mBuf, sizeof(mBuf));
            size_t size_read = data_stream.gcount();
            size_total += size_read;
            //std::cout << "size_read=" << size_read << ", size_total=" << size_total << std::endl;

            if(size_read > 0)
            {
                data_stmt->setUInt64(1, master_id);
                data_stmt->setBlob(2, StreamBuf(mBuf, size_read));
                data_stmt->executeUpdate();
            }
        }

        // Update master stream record with actual data size
        sprintf(sql, "UPDATE " STREAM_TABLE " SET size=%llu WHERE id=%llu", 
            (long long unsigned int)size_total, (long long unsigned int)master_id);
        tran_stmt->execute(sql);

        mCon->commit();

        hdr->id = master_id;
        return true;
    }
    CATCH
    
    mCon->rollback();

    return false;
}

bool MySqlStream::ReadById(uint64_t id_first, bool inclusive_first,
                               uint64_t id_last,  bool inclusive_last)
{
    return Read("id", id_first, inclusive_first, id_last, inclusive_last);
}

bool MySqlStream::Read(const char* column,
                           uint64_t first, bool inclusive_first,
                           uint64_t last,  bool inclusive_last)
{
    // Note: We are going to lock tables while reading. Let's limit the number
    // of read streams per query to make sure that Write() is not blocked while
    // we're reading.
    size_t limit = STREAMS_PER_QUERY; // Number of streams to read per query

    TRY
    {
        if(mReader == NULL)
            THROW("mReader is NULL");
        if(column == NULL)
            THROW("column is NULL");

        while(true)
        {
            // Format SQL query string
            char sql[256] = {0};
            const char* more = (inclusive_first ? ">=" : ">");
            const char* less = (inclusive_last  ? "<=" : "<");
            
            if(first > 0 && last > 0)
            {
                sprintf(sql, "SELECT * FROM %s WHERE %s %s %llu AND %s %s %llu", 
                     STREAM_TABLE, column, more, (long long unsigned int)first, column, less, (long long unsigned int)last);
            }
            else if(first > 0)
            {
                sprintf(sql, "SELECT * FROM %s WHERE %s %s %llu", 
                     STREAM_TABLE, column, more, (long long unsigned int)first);
            }
            else if(last > 0)
            {
                sprintf(sql, "SELECT * FROM %s WHERE %s %s %llu", 
                     STREAM_TABLE, column, less, (long long unsigned int)last);
            }
            else
            {
                sprintf(sql, "SELECT * FROM %s", STREAM_TABLE);
            }
            
            if(limit > 0)
            {
                sprintf(sql + strlen(sql), " ORDER BY %s ASC LIMIT %lu", column, limit);
            }
            else
            {
                sprintf(sql + strlen(sql), " ORDER BY %s ASC", column);
            }
            
            // Acquire READ lock to block the deletion while reading is in progress
            std::unique_ptr<sql::Statement> stmt(mCon->createStatement());
            SqlLockRead lock(stmt);

            // Execute query
            std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(sql));
            
//            std::stringstream msg;
//            msg << "Query \"" << sql << "\" : " << res->rowsCount() << " rows selected";
//            WriteToLog(LOG_INFO, msg);
            
            StreamHeader hdr;
            bool stopped = false;
            
            while(res->next())
            {
                //std::cout << "getRow()=" << res->getRow() << std::endl;

                hdr.id = res->getUInt64("id");
                hdr.type = (uint8_t)res->getUInt("type");
                hdr.size = res->getUInt64("size");
                hdr.timestamp = res->getUInt64("timestamp");

                sql::SQLString descr = res->getString("descr");
                hdr.descr = descr.c_str();
                
                if(hdr.size == 0)
                {
                    std::stringstream msg;
                    msg << MODULE_NAME << ": Invalid stream (size=0): id=" << hdr.id << ", descr='" << hdr.descr << "'";
                    WriteToLog(LOG_INFO, msg);

                    mReader->OnRead(&hdr, mBuf, 0, DB_STREAM_READ_BEGIN);
                    mReader->OnRead(&hdr, mBuf, 0, DB_STREAM_READ_END);
                    continue;
                }

                // Read stream data
                stopped = false;
                if(!ReadData(hdr, &stopped))
                {
                    THROW("ReadData failed");
                }
                else if(stopped)
                {
                    WriteToLog(LOG_INFO, "ReadData stopped by caller");
                	break; // Reading was stopped by caller
                }
            }
            
            if(stopped || limit == 0 || res->rowsCount() < limit)
                break; // Stopped by caller or No more streams left to read
            
            // Reset to keep reading from the next stream (this one is already read)
            inclusive_first = false;

            if(strcmp(column, "id") == 0)
                first = hdr.id;
            else if(strcmp(column, "timestamp") == 0)
                first = hdr.timestamp;
            else
                THROW("Invalid column='" + std::string(column) + "'");
        }
        
        return true;
    }
    CATCH
    
    //exit(1); // test
    return false;
}

bool MySqlStream::DeleteById(uint64_t id_first, bool inclusive_first,
                             uint64_t id_last,  bool inclusive_last)
{
    return Delete("id", id_first, inclusive_first, id_last, inclusive_last);
}

bool MySqlStream::DeleteAll()
{
    return Delete("id", 0, true, 0, true);
}

bool MySqlStream::Delete(const char* column,
                         uint64_t first, bool inclusive_first,
                         uint64_t last,  bool inclusive_last,
                         bool reset_id /*=false*/)
{
    TRY
    {
        if(column == NULL)
            THROW("column is NULL");

        // Enable autocommit
        mCon->setAutoCommit(true);

        // Format SQL query string
        char sql[256] = {0};
        const char* more = (inclusive_first ? ">=" : ">");
        const char* less = (inclusive_last  ? "<=" : "<");

        if(first > 0 && last > 0)
        {
            sprintf(sql, "DELETE FROM %s WHERE %s %s %llu AND id %s %llu", 
                STREAM_TABLE, column, more, (long long unsigned int)first, less, (long long unsigned int)last);
        }
        else if(first > 0)
        {
            sprintf(sql, "DELETE FROM %s WHERE %s %s %llu", 
                STREAM_TABLE, column, more, (long long unsigned int)first);
        }
        else if(last > 0)
        {
            sprintf(sql, "DELETE FROM %s WHERE %s %s %llu", 
                STREAM_TABLE, column, less, (long long unsigned int)last);
        }
        else
        {
            sprintf(sql, "DELETE FROM %s", STREAM_TABLE);

            //
            // TODO: Consider using TRUNCATE TABLE since it is faster then DELETE FROM
            //
            // NOTE: There is a bug "TRUNCATE TABLE was not allowed under LOCK TABLES"
            // which is still present in the current DB we have installed, but should
            // be fixed in the later version:
            // http://bugs.mysql.com/bug.php?id=20667
            //
            // TODO: Try to upgrade MySql and verify if TRUNCATE TABLE worked on locked table
            //
        }

        // Acquire WRITE lock to block the reading while deletion is in progress
        std::unique_ptr<sql::Statement> stmt(mCon->createStatement());
        SqlLockWrite lock(stmt);

        // Execute query
        stmt->execute(sql);

//        std::stringstream msg;
//        msg << std::boolalpha;
//        msg << MODULE_NAME ": Query \"" << sql << "\" : " << stmt->getUpdateCount() << " rows deleted";
//        WriteToLog(LOG_INFO, msg);

        if(reset_id && first == 0 && last == 0)
        {
            // Reset auto_increment since tables are empty now
            // Note: batch processing in not yet available in C++ connector,
            // hence we have to execute it for every table
            //
            stmt->execute("ALTER table " STREAM_TABLE " AUTO_INCREMENT=1");
            stmt->execute("ALTER table " STREAMDATA_TABLE " AUTO_INCREMENT=1");
        }

        return true;
    }
    CATCH

    return false;
}

bool MySqlStream::GetFirst(StreamHeader* hdr)
{
    return Get(hdr, "ASC");
}

bool MySqlStream::GetLast(StreamHeader* hdr)
{
    return Get(hdr, "DESC");
}

// Lookup first/last
bool MySqlStream::Get(StreamHeader* hdr, const char* order)
{
    TRY
    {
        if(hdr == NULL)
            THROW("StreamHeader* hdr is NULL");
        if(order == NULL)
            THROW("order is NULL");

        char sql[256] = {0};
        sprintf(sql, "SELECT * FROM " STREAM_TABLE " ORDER BY id %s LIMIT 1", order);

        // Acquire READ lock to block the deletion while reading is in progress
        std::unique_ptr<sql::Statement> stmt(mCon->createStatement());
        SqlLockRead lock(stmt);

        // Execute query
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(sql));

        if(res->rowsCount() == 0)
        {
            // Nothing selected
            hdr->id = 0;
            hdr->descr = NULL;
            hdr->size = 0;
            hdr->timestamp = 0;
        }
        else
        {
            if(!res->next())
                THROW("ResultSet::next failed");

            sql::SQLString descr = res->getString("descr");
            strcpy((char*)mBuf, descr.c_str());

            hdr->id = res->getUInt64("id");
            hdr->descr = (const char*)mBuf;
            hdr->size = res->getUInt64("size");
            hdr->timestamp = res->getUInt64("timestamp");
        }

        return true;
    }
    CATCH

    return false;
}

bool MySqlStream::LookupById(uint64_t id, bool* found)
{
    return Lookup("id", id, found);
}

// Lookup by value (id, timestamp, etc.)
bool MySqlStream::Lookup(const char* column, uint64_t val, bool* found)
{
    TRY
    {
        if(column == NULL)
            THROW("column is NULL");
        if(found == NULL)
            THROW("bool* found is NULL");

        // Format SQL query string
        // Use SELECT 1 to to prevent the checking of unnecessary fields
        // Use LIMIT 1 to prevent the checking of unnecessary rows
        char sql[256] = {0};
        sprintf(sql, "SELECT 1 FROM %s WHERE %s = %llu LIMIT 1", 
            STREAM_TABLE, column, (long long unsigned int)val);

        // Acquire READ lock to block the deletion while reading is in progress
        std::unique_ptr<sql::Statement> stmt(mCon->createStatement());
        SqlLockRead lock(stmt);

        // Execute query
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(sql));

        *found = (res->rowsCount() > 0);
        return true;
    }
    CATCH

    return false;
}

bool MySqlStream::ReadData(const StreamHeader& hdr, bool* stopped)
{
    TRY
    {
        // Don't need to call acquire READ lock as it it already acquired by Read()

        std::unique_ptr<sql::Statement> stmt(mCon->createStatement());
        char sql[256] = {0};

        // Get all data records for the given master id
        uint64_t masterid = hdr.id;
        sprintf(sql, "SELECT id FROM " STREAMDATA_TABLE " WHERE masterid = %llu order by id", 
            (long long unsigned int)masterid);
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(sql));

        bool keepReading = mReader->OnRead(&hdr, mBuf, 0, DB_STREAM_READ_BEGIN);

        while(keepReading && res->next())
        {
            // Get the data itself
            uint64_t id = res->getUInt64("id");
            sprintf(sql, "SELECT data FROM " STREAMDATA_TABLE " WHERE id=%llu", (unsigned long long int)id);
            std::unique_ptr<sql::ResultSet> data_res(stmt->executeQuery(sql));

            //if(data_res->rowsCount() == 0)
            //    THROW(__func__ ": ResultSet::rowsCount returned 0");

            if(!data_res->next())
                THROW("ResultSet::next failed");

            std::istream* blob = data_res->getBlob("data");
            if(blob == NULL)
                THROW("ResultSet::getBlob failed");

            size_t size_total = 0;

            while(keepReading && *blob)
            {
                blob->read((char*)mBuf, sizeof(mBuf));
                size_t size_read = blob->gcount();
                size_total += size_read;

                //std::cout << "data: master_id=" << master_id << ", id=" << id << ", size=" << size_total << std::endl;

                if(size_read > 0)
                	keepReading = mReader->OnRead(&hdr, mBuf, size_read, DB_STREAM_READ_DATA);
            }
        }

        mReader->OnRead(&hdr, mBuf, 0, DB_STREAM_READ_END);

        if(stopped != NULL)
        	*stopped = !keepReading;
        return true;
    }
    CATCH

    return false;
}

