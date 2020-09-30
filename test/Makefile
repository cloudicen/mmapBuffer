ver = debug
CXX = g++

MACROS = 

ifeq ($(ver), debug)
CFLAGS = -std=c++17 -Wall -pthread -O0 -g
MACROS += -D DEBUG
else
CFLAGS = -std=c++17 -Wall -pthread -O2 
MACROS += -D NDEBUG
endif

TARGET = mmapBufferTest
OBJS = ../code/*.cpp \
       mmapBufferTest.cpp

all: $(OBJS)
	$(CXX) $(CFLAGS) $(OBJS) $(MACROS) -o $(TARGET)