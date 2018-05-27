#ifndef __BOOSTMOD_HPP__
#define __BOOSTMOD_HPP__

#include <typeinfo>

#define BOOST_CHECK_THROW2( statement, exception ) \
    try { statement; BOOST_ERROR( "exception "#exception" is expected" ); } \
    catch( exception const& ) { \
        BOOST_CHECK_MESSAGE( true, "exception "#exception" is caught" ); \
    } \
    catch( ... ) { \
        BOOST_ERROR( "caught an unexpected exception. " \
                     "exception "#exception" was expected." ); \
    }

#define BOOST_CHECK_NOTHROW( statement, exception ) \
    try { statement; \
        BOOST_CHECK_MESSAGE( true, "exception "#exception" was not thrown" ); } \
    catch( exception const& e ) { \
        BOOST_ERROR( "exception " << typeid( e ).name() << \
                     " should not have been thrown" ); \
    } \
    catch( ... ) { \
        BOOST_ERROR( "no exceptions should have been thrown." ); \
    }

#define BOOST_DISPLAY_EXCEPTION( statement, exceptionbase ) \
    try { statement; } \
    catch ( exceptionbase const&e ) { \
        std::cerr << "Caught exception " << typeid( e ).name(); }

#endif



