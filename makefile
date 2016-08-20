DIR_INC = ./include
DIR_SRC = ./src
DIR_OBJ = ./obj

SRC = $(wildcard ${DIR_SRC}/*.cpp)
DIR = $(notdir $(SRC))
OBJ = $(patsubst %.cpp, ${DIR_OBJ}/%.o, $(notdir $(SRC)))

CC = g++
CFLAGS = -g -Wall -I${DIR_INC}

#ALL:
#	@echo $(DIR)
#	@echo $(SRC)
#	@echo $(OBJ)
	
dedup:${OBJ}
	$(CC) $(OBJ) -o $@

${DIR_OBJ}/%.o: ${DIR_SRC}/%.cpp
	$(CC) $(CFLAGS) -c $< -o $@

.PHONY:clean
clean :
	find ${DIR_OBJ} -name *.o -exec rm -rf {}
