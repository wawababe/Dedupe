/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#ifndef LISTDB_H
#define LISTDB_H

#include <fstream>
#include <iostream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define DEFAULT_CACHE_SZ 4194304 //4MB
#define DEFAULT_SWAP_SZ 4096  //4KB

using namespace std;

typedef struct list_entry {
    bool iscached; //cached or not
    uint64_t file_offset; //offset in file
    uint32_t cache_offset; //offset in cache
} LIST_ENTRY;
#define LIST_ENTRY_SZ sizeof(LIST_ENTRY)

class ListDB
{
    public:
        ListDB(unsigned int unit_sz, unsigned int cache_sz, unsigned int swap_sz);
        int opendb(const char *path);
        int closedb();
        int setvalue(const unsigned int index, void *value);
        int getvalue(const unsigned int index, void *value);
        int unlinkdb();
        virtual ~ListDB();
    protected:
        int swapin(unsigned int pos, unsigned long file_off);
        int swapout(unsigned int pos);
        int value_set_get(unsigned int index, void *val, uint8_t op);
    private:
        char *dbname;
        //int fd;
        fstream db_file;
        unsigned int unit_size;
        unsigned int cache_group_nr;
        unsigned int swap_size;
        unsigned int cache_size;
        LIST_ENTRY *le_array;
        void *cache;
};

enum VALUE_OP {
    VALUE_SET = 0,
    VALUE_GET
};
#endif // LISTDB_H
