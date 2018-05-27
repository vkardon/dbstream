
# Target(s) to build
TARGET_LIB     = libmysqlstream.so
TARGET_READER  = reader
TARGET_WRITER  = writer
TARGET_TESTAPP = testapp

# Sources
PROJECT_HOME = .
SRC_DIR = $(PROJECT_HOME)/src
OBJ_DIR = $(PROJECT_HOME)/_obj

BOOST_HOME = ../boost_1_57_0
MYSQL_HOME = ../mysql_install
MYSQL_INC  = $(MYSQL_HOME)/inc/mysql-connector-c++-1.1.4

SRCS_LIB     = $(SRC_DIR)/mysqlstream.cpp
SRCS_READER  = $(SRC_DIR)/reader.cpp
SRCS_WRITER  = $(SRC_DIR)/writer.cpp
SRCS_TESTAPP = $(SRC_DIR)/testapp.cpp

# Detect operating system
OS = $(shell uname -s)

ifeq "$(OS)" "Linux"
  MYSQL_LIBS = $(MYSQL_HOME)/lib/linux/libmysqlcppconn-static.a \
               $(MYSQL_HOME)/lib/linux/libmysqlclient.a
else ifeq "$(OS)" "SunOS"
  MYSQL_LIBS = $(MYSQL_HOME)/lib/solaris/libmysqlcppconn-static.a \
               $(MYSQL_HOME)/lib/solaris/libmysqlclient.a
else
  MYSQL_LIBS = $(MYSQL_HOME)/lib/mac/libmysqlcppconn-static.a \
               $(MYSQL_HOME)/lib/mac/libmysqlclient.a
endif

# Include directories
INCS = -I$(MYSQL_INC) -I$(BOOST_HOME)

# Libraries
ifeq "$(OS)" "Darwin"
  LIBS = -ldl
else
  LIBS = -ldl -lrt
endif

# Objective files to build
OBJS_LIB     = $(addprefix $(OBJ_DIR)/, $(addsuffix .o, $(basename $(notdir $(SRCS_LIB)))))
OBJS_READER  = $(addprefix $(OBJ_DIR)/, $(addsuffix .o, $(basename $(notdir $(SRCS_READER)))))
OBJS_WRITER  = $(addprefix $(OBJ_DIR)/, $(addsuffix .o, $(basename $(notdir $(SRCS_WRITER)))))
OBJS_TESTAPP = $(addprefix $(OBJ_DIR)/, $(addsuffix .o, $(basename $(notdir $(SRCS_TESTAPP)))))

# Compiler and linker to use
ifeq "$(OS)" "Linux"
  CC = g++
  CCFLAGS = -std=c++11 -D NDEBUG -O3 -Wall -fPIC
  LD = $(CC)
  LDFLAGS = 
else ifeq "$(OS)" "SunOS"
  CC = CC5.13
  CCFLAGS = -std=c++11 -D NDEBUG -O2 -KPIC -m64
  LD = $(CC)
  LDFLAGS = -std=c++11 -KPIC -m64 
else 
  CC = g++
  CCFLAGS = -std=c++11 -D NDEBUG -O3 -Wall -fPIC
  LD = $(CC)
  LDFLAGS = 
endif

# Build target(s)
all: $(TARGET_LIB) $(TARGET_READER) $(TARGET_WRITER) $(TARGET_TESTAPP)

$(TARGET_LIB): $(OBJS_LIB)
ifeq "$(OS)" "SunOS"
	$(LD) $(LDFLAGS) -o $(TARGET_LIB) $(OBJS_LIB) $(MYSQL_LIBS) -G -lstdc++ -lCrunG3 -lrt -lsocket
else
	$(LD) $(LDFLAGS) -o $(TARGET_LIB) $(OBJS_LIB) $(MYSQL_LIBS) -shared
endif

$(TARGET_READER): $(OBJS_READER) $(TARGET_LIB)
	$(LD) $(LDFLAGS) -o $(TARGET_READER) $(OBJS_READER) $(LIBS)

$(TARGET_WRITER): $(OBJS_WRITER) $(TARGET_LIB)
	$(LD) $(LDFLAGS) -o $(TARGET_WRITER) $(OBJS_WRITER) $(LIBS)

$(TARGET_TESTAPP): $(OBJS_TESTAPP) $(TARGET_LIB)
	$(LD) $(LDFLAGS) -o $(TARGET_TESTAPP) $(OBJS_TESTAPP) $(LIBS)

# Compile source files
# Add -MP to generate dependency list
# Add -MMD to not include system headers
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp Makefile   
	-mkdir -p $(OBJ_DIR)
	$(CC) -c -MP -MMD $(CCFLAGS) $(INCS) -o $(OBJ_DIR)/$*.o $<
	
# Delete all intermediate files
clean: 
#	@echo MYSQL_HOME = $(MYSQL_HOME)
#	@echo MYSQL_LIB = $(MYSQL_LIB)
#	@echo OBJS_LIB = $(OBJS_LIB)
#	@echo OBJS_READER = $(OBJS_READER) 
#	@echo OBJS_WRITER = $(OBJS_WRITER)
#	@echo OBJS_TESTAPP = $(OBJS_TESTAPP)
	rm -rf $(TARGET_LIB) $(TARGET_READER) $(TARGET_WRITER) $(TARGET_TESTAPP) $(OBJ_DIR) 

#
# Read the dependency files.
# Note: use '-' prefix to don't display error or warning
# if include file do not exist (just remade it)
#
-include $(OBJS_LIB:.o=.d)
-include $(OBJS_READER:.o=.d)
-include $(OBJS_WRITER:.o=.d)
-include $(OBJS_TESTAPP:.o=.d)


