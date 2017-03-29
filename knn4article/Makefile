all: article_knn

CC = c++
COMM_DIR = ../../common
API_SERVER_DIR = ../../api_server

SRC = $(shell find . -type f -name '*.cpp')
LIBS = -lglog -lgflags
FLAGS = -std=c++11 -pthread -O3 -g

article_knn:
	$(CC) -o $@.bin $(SRC) $(INCLUDE) $(LIBS) $(FLAGS)

clean:
	rm -rf *.bin *.bin.*
