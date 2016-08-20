/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
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
