CXX = g++
CFLAGS = -std=c++17 -Wall -g -pthread -O2

TARGET = mmapBufferTest
OBJS = ../code/*.cpp \
       mmapBufferTest.cpp

all: $(OBJS)
	$(CXX) $(CFLAGS) $(OBJS) -o $(TARGET)  