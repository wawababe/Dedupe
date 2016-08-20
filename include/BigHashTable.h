#ifndef BIGHASHTABLE_H
#define BIGHASHTABLE_H

#include <iostream>
#include <fstream>

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "HashDB.h"
#include "hashfunc.h"

using namespace std;

#ifndef PATH_MAX_LEN
#define PATH_MAX_LEN 255
#endif //PATH_MAX_LEN

class BigHashTable
{
    public:
        BigHashTable(const char *dbname = 0, const char *bfname = 0);
        virtual ~BigHashTable();
        void insert(const void *key, const void *data, const int datasz);
        void* getvalue(const void *key, int &valuesize);
        inline bool contain(const void *key){
            int valuesize = 0;
            return (getvalue(key, valuesize)) ? true : false;
        }
    protected:
    private:
        HashDB *db;
        bool isnewdb;
};

#endif // BIGHASHTABLE_H
