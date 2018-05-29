//
// main.cpp
//
#include <stdlib.h>
#include <iostream>     // std::cout
#include <sstream>      // std::stringstream
#include <fstream>      // std::ifstream
#include <algorithm>    // std::replace
#include <dirent.h>
#include <sys/stat.h>   // stat
#include <sys/time.h>   // gettimeofday
#include <fcntl.h>      // open
#include <errno.h>      // errno
#include <string.h>     // strerror
#include <sys/mman.h>   // mmap
#include <libgen.h>     // basename
#include <unistd.h>     // close, access
#include <limits.h>     // PATH_MAX
#include <dlfcn.h>      // dlopen
#include <pwd.h>        // for getpwuid

#include "dbstream.h"
#include "stopwatch.h"

using namespace std;

//
// DBStreamClient class to demonstrate working with DBStream
//
class DBStreamClient : public DBStreamReader, public DBStreamLogger
{
public:
    DBStreamClient(const char* libname,
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

        cout << __func__ << ": " << "mDBStream=" << mDBStream << endl;

        mDatabase = database;
    }
    
    virtual ~DBStreamClient()
    {
        if(mDBStream != NULL)
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
    
    bool IsValid() { return (mDBStream != NULL); }
    
    void WriteDir(const char* dir);
    void WriteFile(const char* filename);
    void WriteFileLarge(const char* filename);
    void Read();
    void Lookup();
    void Delete();
    void Describe();
    
    void* mMySqlLib = nullptr;
    struct DBStream* mDBStream = nullptr;
    std::string mDatabase; 

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

void DBStreamClient::WriteDir(const char* dir)
{
    DIR* dpdf = opendir(dir);
    if(dpdf == NULL)
    {
        cout << "Cannot open dir=" << dir << endl;
        return;
    }

    struct dirent* epdf = NULL;
    while((epdf = readdir(dpdf)) != NULL)
    {
        // Write file as stream using std::ifstream
        string path = dir + string("/") + epdf->d_name;
        WriteFile(path.c_str());
    }

    closedir(dpdf);
}

void DBStreamClient::WriteFile(const char* filename)
{
    //cout << __func__ << ": '" << filename << "'" << endl;

    struct stat sb;
    if(stat(filename, &sb) != 0 || S_ISREG(sb.st_mode) == false)
    {
        //cout << "Not a regular file: " << filename << endl;
        return;
    }

    //cout << __func__ << ": size=" << sb.st_size << " bytes" << endl;

    // Generate timestamp (in ms)
    struct timeval  tv;
    gettimeofday(&tv, NULL);
    uint64_t timestamp = tv.tv_sec * 1000 + tv.tv_usec / 1000;

    char path[PATH_MAX] = {0};
    strcpy(path, filename);

    // Escape description string (file name)
    string descr = basename(path);
    std::replace(descr.begin(), descr.end(), '\'', '_'); 
    std::replace(descr.begin(), descr.end(), ' ', '_'); 

    StreamHeader hdr;
    hdr.descr = descr.c_str();
    hdr.type = (sb.st_size < 1024 ? 0 : sb.st_size < 1024*64 ? 1 : 2);
    hdr.timestamp = timestamp;
    hdr.size = sb.st_size;

//    // test - limit to 1MB
//    if(sb.st_size > 1024 * 1024)
//        return;

    // Open file as stream
    std::ifstream fs(filename, std::ifstream::in | std::ifstream::binary);
    if(!fs.is_open())
    {
        cout << "Error opening file '" << path << "'" << endl;
        return;
    }

    cout << endl;
    cout << __func__
         << ": descr='" << hdr.descr << "'"
         << ", size=" << hdr.size << " ..." << endl;

    CStopWatch t(string(__func__) + ": ");

    if(!mDBStream->Write(&hdr, fs))
    {
        cout << __func__
             << ": descr='" << hdr.descr << "'"
             << ", size=" << hdr.size << " [ERROR]" << endl;
    }
    else
    {
        cout << __func__
             << ": descr='" << hdr.descr << "'"
             << ", size=" << hdr.size << " [id=" << hdr.id << "]" << endl;
    }

    fs.close();
}

void DBStreamClient::WriteFileLarge(const char* filename)
{
    //cout << __func__ << ": '" << filename << "'" << endl;

    int fd = open(filename, O_RDONLY);
    if(fd < 0)
    {
        cout << "open() failed for '" << filename << "': " << strerror(errno) << endl;
        return;
    }

    struct stat sb;
    if(fstat(fd, &sb) != 0 || S_ISREG(sb.st_mode) == false)
    {
        cout << "Not a regular file: " << filename << endl;
        close(fd);
        return;
    }

    //cout << __func__ << ": size=" << sb.st_size << " bytes" << endl;

    char* addr = (char*)mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if(addr == MAP_FAILED)
    {
        cout << "mmap() failed for '" << filename << "': " << strerror(errno) << endl;
        close(fd);
        return;
    }

    // Generate timestamp (in ms)
    struct timeval  tv;
    gettimeofday(&tv, NULL);
    uint64_t timestamp = (tv.tv_sec * 1000 + tv.tv_usec / 1000);

    char path[PATH_MAX] = {0};
    strcpy(path, filename);

    StreamHeader hdr;
    hdr.descr = basename(path);
    hdr.type = (sb.st_size < 1024 ? 0 : sb.st_size < 1024*64 ? 1 : 2);
    hdr.timestamp = timestamp;
    hdr.size = sb.st_size;

    cout << endl;
    cout << __func__
         << ": descr='" << hdr.descr << "'"
         << ", size=" << hdr.size << "..." << endl;

    CStopWatch t(string(__func__) + ": ");

    if(!mDBStream->Write(&hdr, (unsigned char*)addr))
    {
        cout << __func__
             << ": descr='" << hdr.descr << "'"
             << ", size=" << hdr.size << " [ERROR]" << endl;
    }
    else
    {
        cout << __func__
             << ": descr='" << hdr.descr << "'"
             << ", size=" << hdr.size << " [id=" << hdr.id << "]" << endl;
    }

    if(munmap(addr, sb.st_size) < 0)
    {
        cout << "munmap() failed for '" << filename << "': " << strerror(errno) << endl;
    }

    if(close(fd) < 0)
    {
        cout << "close() failed for '" << filename << "': " << strerror(errno) << endl;
    }
}

void DBStreamClient::Read()
{
    cout << "Reading all records from '" << mDatabase << "'..." << endl;

    mDBStream->ReadById(0, true, 0, true);
    //mDBStream->ReadById(0, false, 16, true);
    //mDBStream->ReadById(2, false, 13, false);
}

void DBStreamClient::Lookup()
{
    StreamHeader hdr;
    bool found = false;

    //memset(hdr, 0, sizeof(hdr));
    mDBStream->GetFirst(&hdr);
    if(hdr.id > 0)
    {
       cout << "First record: id=" << hdr.id << ", descr=" << hdr.descr << endl;
       mDBStream->LookupById(hdr.id, &found);
       cout << "Lookup id=" << hdr.id << ": " << found << endl;
    }
    else
    {
       cout << "No first record found" << endl;
    }

    //memset(hdr, 0, sizeof(hdr));
    mDBStream->GetLast(&hdr);
    if(hdr.id > 0)
    {
       cout << "Last record: id=" << hdr.id << ", descr=" << hdr.descr << endl;
       mDBStream->LookupById(hdr.id, &found);
       cout << "Lookup id=" << hdr.id << ": " << found << endl;
    }
    else
    {
       cout << "No last record found" << endl;
    }

    mDBStream->LookupById(1234567890, &found);
    cout << "Lookup id=1234567890: " << found << endl;

    mDBStream->LookupById(1876543219, &found);
    cout << "Lookup id=1876543219: " << found << endl;
}

void DBStreamClient::Delete()
{
    cout << "Deleting all records from '" << mDatabase << "'..." << endl;

    CStopWatch t(string(__func__) + ": ");

    mDBStream->DeleteAll();
}

void DBStreamClient::Describe()
{
    cout << "Describe '" << mDatabase << "' tables..." << endl;

    mDBStream->Describe();
}

bool DBStreamClient::OnRead(const StreamHeader* hdr,
                            unsigned char* data, size_t size, 
                            int reading_state)
{
    static CStopWatch st(string(__func__) + ": ", true);
    static size_t read_size = 0;

    switch(reading_state)
    {
    case DB_STREAM_READ_BEGIN:
        st.Start();
        read_size = 0;
        cout << __func__
             << ": id="     << hdr->id
             << ", descr='" << hdr->descr << "'"
             << ", type="   << (int)hdr->type
             << ", size="   << hdr->size << " ..." << endl;
        break;

    case DB_STREAM_READ_DATA:
        read_size += size;
        break;

    case DB_STREAM_READ_END:
        cout << __func__ << (read_size != hdr->size ? "[ERROR]" : "")
             << ": id="        << hdr->id
             << ", descr='"    << hdr->descr << "'"
             << ", type="      << (int)hdr->type
             << ", size="      << hdr->size
             << ", read_size=" << read_size << endl;
        read_size = 0;
        st.Stop();
        break;
    }
     
    return true;
}

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

    DBStreamClient dbstreamClient(libname, DB_HOST, DB_USER, DB_PASS, DB_NAME);
    if(!dbstreamClient.IsValid())
        return 1;
    
    dbstreamClient.Describe();
    dbstreamClient.Lookup();

    // Write all files from the current user Download directory 
    struct passwd* pwd = getpwuid(getuid());
    string writeDir(pwd ? pwd->pw_dir : ".");
    writeDir += "/Downloads";
    if(access(writeDir.c_str(), F_OK | R_OK) == 0)
    {
        dbstreamClient.WriteDir(writeDir.c_str());
    }
    else
    {
        cout << "The directory \"" << writeDir << "\" doesn't exist or isn't readable" << endl;
    }

    // Write largeDataFile - expected to be the current directory
    string largeFile("./largeDataFile");
    if(access(largeFile.c_str(), F_OK | R_OK) == 0)
    {
        dbstreamClient.WriteFileLarge(largeFile.c_str());
    }
    else
    {
        cout << "The file \"" << largeFile << "\" doesn't exist or isn't readable" << endl;
    }

    dbstreamClient.Read();
    dbstreamClient.Lookup();

    dbstreamClient.Delete();
    dbstreamClient.Lookup();

    cout << "Done!" << endl;
    return 0;
}
