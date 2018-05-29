//
// main.cpp
//
#include <stdlib.h>
#include <iostream>     // std::cout
#include <fstream>      // std::ifstream
#include <algorithm>    // std::replace
#include <dirent.h>
#include <sys/stat.h>   // stat
#include <sys/time.h>   // gettimeofday
#include <dlfcn.h>      // dlopen
#include <libgen.h>     // For dirname
#include <limits.h>     // PATH_MAX
#include <string.h>
#include <unistd.h>     // sleep
#include <pwd.h>        // for getpwuid

#include "dbstream.h"

using namespace std;

//
// Implementation of DBStreamLogger interface
//
class StreamLogger : public DBStreamLogger
{
    //
    // Implementation of DBStreamLogger interface
    //
    virtual void OnLogInfo(const char* msg) { cout << msg << endl; }
    virtual void OnLogError(const char* err) { cout << err << endl; }

public:
    virtual ~StreamLogger() { /**/ }
};


//
// Write all files from the given directory to DBStream
//
bool Write(DBStream* dbStream, const char* dir)
{
    int number = 0;
    uint64_t total_size = 0;

    DIR* dpdf = opendir(dir);
    if(dpdf == NULL)
    {
        cout << "Cannot open directory=" << dir << endl;
        return false;
    }

    struct dirent* epdf = NULL;
    while((epdf = readdir(dpdf)) != NULL)
    {
        string path = dir + string("/") + epdf->d_name;

        // Note: We are going to call stat since dirent::d_type
        // is not available on Linux  struct stat sb
        struct stat sb;
        if(stat(path.c_str(), &sb) != 0 || S_ISREG(sb.st_mode) == false)
            continue;

        std::ifstream fs(path.c_str(), std::ifstream::in | std::ifstream::binary);
        if(!fs.is_open())
        {
            cout << "Error opening file '" << path << "'" << endl;
            continue;
        }

        // test - ignore big files
        //uint64_t max_size = 1024 * 1024 * 10;   // 10MB
        //uint64_t max_size = 1024 * 1024;        // 1 MB
        //if(sb.st_size > max_size)
        //    continue;

        number++;
        total_size += sb.st_size;
        // test end

        // Generate timestamp (in ms)
        struct timeval  tv;
        gettimeofday(&tv, NULL);
        uint64_t timestamp = (tv.tv_sec * 1000 + tv.tv_usec / 1000);

        // Escape description string (file name)
        string descr = epdf->d_name;
        std::replace(descr.begin(), descr.end(), '\'', '_'); 
        std::replace(descr.begin(), descr.end(), ' ', '_'); 

        StreamHeader hdr;
        hdr.descr = descr.c_str();
        hdr.type = (sb.st_size < 1024 ? 0 : sb.st_size < 1024*64 ? 1 : 2);
        hdr.timestamp = timestamp;
        hdr.size = sb.st_size;

        cout << __func__
             << ": descr='" << hdr.descr << "'"
             << ", size=" << hdr.size << " ..." << endl;

        if(dbStream->Write(&hdr, fs))
        {
            cout << __func__
                 << ": descr='" << hdr.descr << "'"
                 << ", size=" << hdr.size << " - succeeded, id=" << hdr.id << endl;
        }
        else
        {
            cout << __func__
                 << ": descr='" << hdr.descr << "'"
                 << ", size="  << hdr.size << " - failed" << endl;
        }

        fs.close();

        //break; // test
    }

    closedir(dpdf);

    cout << "Write: " << number << " files, total size " << total_size << " bytes" << endl;
    return true;
}

//
// Usage example for DBStream writer
//
int main(int argc, const char** argv)
{
    cout << boolalpha;

    StreamLogger streamLogger;

    // Get the canonicalized absolute pathname
    char libname[PATH_MAX]{};
    realpath(argv[0], libname);
    const char* dir = dirname(libname);
    libname[strlen(dir)] = '\0';
    strcat(libname, "/libmysqlstream.so");

    // Load MySQL library
#if defined(sun) || defined(__sun)
    void* mysqlLib = dlopen(libname, RTLD_NOW | RTLD_GROUP);
#else
    void* mysqlLib = dlopen(libname, RTLD_NOW);
#endif

    if(mysqlLib == nullptr)
    {
        cout << "ERROR: dlopen() failed because of " << dlerror() << endl;
        return 1;
    }

    // Get the CreateDBStream() function
    CreateDBStreamPtr pfCreateDBStream =
            (CreateDBStreamPtr)dlsym(mysqlLib, CREATE_DB_STREAM_FUNC_NAME);

    if(pfCreateDBStream == nullptr)
    {
        cout << "ERROR: dlsym() failed because of " << dlerror() << endl;
        return 1;
    }

    // Create DB Stream 
    const char* DB_HOST = "tcp://localhost:3309";
    const char* DB_NAME = "StreamDB";
    const char* DB_USER = "Loader";
    const char* DB_PASS = "Loader";

    DBStream* dbStream = (*pfCreateDBStream)(
            DB_HOST, DB_USER, DB_PASS, DB_NAME, NULL, &streamLogger);

    cout << __func__ << ": " << "dbStream=" << dbStream << endl;

    if(!dbStream || !dbStream->IsValid())
        return 1;

    // Write all files from the current user Download directory
    struct passwd* pwd = getpwuid(getuid());
    string writeDir(pwd ? pwd->pw_dir : ".");
    writeDir += "/Downloads";
    if(access(writeDir.c_str(), F_OK | R_OK) != 0)
    {
        cout << "The directory \"" << writeDir << "\" doesn't exist or isn't readable" << endl;
        return 1;
    }

    for(int i=0; ; i++)
    //for(int i=0; i<30; i++)
    //while(true)
    {
        //cout << "i=" << i << endl;
        
        // Delete
        cout << endl;
        cout << "DeleteAll" << endl;
        cout << endl;

        dbStream->DeleteAll();

        if(!Write(dbStream, writeDir.c_str()))
        {
            sleep(1); // test
        }
    }

    dbStream->Destroy();

    cout << "Done!" << endl;
    return 0;
}
