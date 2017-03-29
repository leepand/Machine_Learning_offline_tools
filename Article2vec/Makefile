CC = c++

all: article2vec

SRC = article2vec.cpp
LIBS = -lglog -lgflags
FLAGS = -std=c++11 -pthread -O3 -g

article2vec:
	$(CC) -o $@.bin $(SRC) $(INCLUDE) $(LIBS) $(FLAGS)

clean:
	rm -rf *.bin *.bin.*
