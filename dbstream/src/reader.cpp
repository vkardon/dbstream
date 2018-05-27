//
// main.cpp
//
#include <stdlib.h>
#include <iostream>     // std::cout
#include <sstream>      // std::stringstream
#include <fstream>      // std::ifstream
#include <unistd.h>     // sleep
#include <dlfcn.h>      // dlopen
#include <libgen.h>     // For dirname
#include <limits.h>     // PATH_MAX
#include <string.h>

#include "dbstream.h"
#include "stopwatch.h"

using namespace std;

//
// StreamReader class to demonstrate working with DBStream
//
class StreamReader : public DBStreamReader, public DBStreamLogger
{
public:
    StreamReader(const char* libname,
            const char* host, const char* user, const char* passwd, const char* database)
    {
        // Load MySQL library
#if defined(sun) || defined(__sun)
        mMySqlLib = dlopen(libname, RTLD_NOW | RTLD_GROUP);
#else
        mMySqlLib = dlopen(libname, RTLD_NOW);
#endif

        if(mMySqlLib == nullptr)
        {
            cout << "ERROR: dlopen() failed because of " << dlerror() << endl;
            cout <<  "mDBStream=" << mDBStream << endl;
            return;
        }

        // Get the CreateDBStream() function
        CreateDBStreamPtr pfCreateDBStream =
                (CreateDBStreamPtr)dlsym(mMySqlLib, CREATE_DB_STREAM_FUNC_NAME);

        if(pfCreateDBStream == nullptr)
        {
            cout << "ERROR: dlsym() failed because of " << dlerror() << endl;
            return;
        }

        // Create DB Stream 
        mDBStream = (*pfCreateDBStream)(host, user, passwd, database, this, this);

        //cout << __func__ << ": " << "mDBStream=" << mDBStream << endl;

        mId_last = 0;
        mRowCount = 0;
    }
    
    virtual ~StreamReader()
    {
        if(mDBStream != nullptr)
            mDBStream->Destroy();

        // Unload the MySQL library if it is open.
        if(mMySqlLib != nullptr)
        {
            if(dlclose(mMySqlLib) < 0)
            {
                cout << "ERROR: dlclose() failed because of " << dlerror() << endl;
            }
            mMySqlLib = nullptr;
        }
    }
    
    bool IsValid() { return (mDBStream != nullptr); }
    
    void Read();
    void Lookup();
    void Describe() { if(mDBStream != nullptr) mDBStream->Describe(); }
    
private:
    void* mMySqlLib = nullptr;
    DBStream* mDBStream = nullptr;
    uint64_t mId_last = 0;
    int mRowCount = 0;

private:
    //
    // Implementation of DBStreamReader interface
    //
    virtual bool OnRead(const StreamHeader* hdr,
                        unsigned char* data, size_t size,
                        int reading_state);

    //
    // Implementation of DBStreamLogger interface
    //
    virtual void OnLogInfo(const char* msg) { cout << msg << endl; }
    virtual void OnLogError(const char* err) { cout << err << endl; }
};

void StreamReader::Read()
{
    while(true)
    {
        mRowCount = 0;

        cout << "StreamReader: Reading all records from id=" << mId_last + 1 << " ..." << endl;

        mDBStream->ReadById(mId_last + 1, true, 0, true);

        sleep(1);
    }
}

void StreamReader::Lookup()
{
    StreamHeader hdr;
    bool found = false;

    //memset(hdr, 0, sizeof(hdr));
    mDBStream->GetFirst(&hdr);
    cout << "Lookup first record: descr=" << hdr.descr << endl;
    mDBStream->LookupById(hdr.id, &found);
    cout << "Lookup id=" << hdr.id << ": " << found << endl;

    //memset(hdr, 0, sizeof(hdr));
    mDBStream->GetLast(&hdr);
    cout << "Lookup last  record: descr=" << hdr.descr << endl;
    mDBStream->LookupById(hdr.id, &found);
    cout << "Lookup id=" << hdr.id << ": " << found << endl;

    mDBStream->LookupById(1234567890, &found);
    cout << "Lookup id=1234567890: " << found << endl;

    mDBStream->LookupById(1876543219, &found);
    cout << "Lookup id=1876543219: " << found << endl;
}

bool StreamReader::OnRead(const StreamHeader* hdr,
                          unsigned char* data, size_t size,
                          int reading_state)
{
    static CStopWatch st(string(__func__) + ": ", true);
    static size_t read_size = 0;
    bool result = true;

    switch(reading_state)
    {
    case DB_STREAM_READ_BEGIN:
        st.Start();
        cout << __func__
             << ": id="     << hdr->id
             << ", descr='" << hdr->descr << "'"
             << ", type="   << (int)hdr->type
             << ", size="   << hdr->size << " ..." << endl;
        read_size = 0;
        break;

    case DB_STREAM_READ_DATA:
        // Simulate occasional failure
	//{
        //    static uint64_t id = 0;
	//    if(id != hdr->id && hdr->id % 3 == 0)
	//    {
	//        id = hdr->id;
        //        result = false;
	//    }
        //}

        read_size += (result ? size : 0);
        break;

    case DB_STREAM_READ_END:
        result = (read_size == hdr->size);
        cout << __func__ << (result ? "" : "[ERROR]")
             << ": id="        << hdr->id
             << ", descr='"    << hdr->descr << "'"
             << ", type="      << (int)hdr->type
             << ", size="      << hdr->size
             << ", read_size=" << read_size << endl;
        read_size = 0;

        if(result)
	{
            mId_last = hdr->id;
            mRowCount++;
        }
        st.Stop();
        break;
    }

    return result;
}

//
// Usage example for Driver, Connection, (simple) Statement, ResultSet
//
int main(int argc, const char** argv)
{
    cout << boolalpha;

    // Get the canonicalized absolute pathname
    char libname[PATH_MAX]{};
    realpath(argv[0], libname);
    const char* dir = dirname(libname);
    libname[strlen(dir)] = '\0';
    strcat(libname, "/libmysqlstream.so");

    const char* DB_HOST = "tcp://localhost:3309";
    const char* DB_NAME = "StreamDB";
    const char* DB_USER = "Loader";
    const char* DB_PASS = "Loader";

    StreamReader reader(libname, DB_HOST, DB_USER, DB_PASS, DB_NAME);

    if(!reader.IsValid())
        return 1;

    reader.Read();
    //reader.Describe();

    //reader.Lookup();
    cout << "Done!" << endl;

    return 0;
}
