//
// streambuf.hpp
//

#ifndef _STREAMBUF_H_
#define _STREAMBUF_H_

#include <stdlib.h>
#include <sstream>

//
// Helper class to initialize std::istream from a binary stream
//
class StreamBuf
{
public:
    StreamBuf(const unsigned char* ptr, size_t size) : buffer(ptr, size), stream(&buffer) {}
    operator std::istream*() { return &stream; }
    operator std::istream&() { return stream; }

private:
    // Wrapper class to access protected setg() method
    struct Buffer : public std::streambuf
    {
        Buffer(const unsigned char* ptr, size_t size)
        {
            char* charPtr = const_cast<char*>(reinterpret_cast<const char*>(ptr));
            std::streambuf::setg(charPtr, charPtr, charPtr + size);
        }
    };

    Buffer buffer;
    std::istream stream;
};

#endif // _STREAMBUF_H_

