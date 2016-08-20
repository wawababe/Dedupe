/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#ifndef HASHDB_H
#define HASHDB_H

#include <iostream>
#include <fstream>
#include <limits>

#include <stdint.h>
#include <unistd.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <string.h> //strdup
#include <stdio.h>

#include "BloomFilter.h"
//#include "utils.h"

#define HASHDB_KEY_MAX_SZ 256
#define HASHDB_VALUE_MAX_SZ 128
#define HASHDB_DEFAULT_TNUM	100000
#define HASHDB_DEFAULT_BNUM	16384 //131072 //2^(17) 0.5MB //40970
#define HASHDB_DEFAULT_CNUM	16384 //131072//2^(17) //40970 //3717

#ifndef PATH_MAX_LEN
#define PATH_MAX_LEN 256
#endif

using namespace std;

typedef struct hash_entry
{
    bool iscached;
    char *key;
    void *value;
    uint32_t ksize; //key size
    uint32_t vsize; //value size
    uint32_t tsize; //total size of the entry
    uint32_t shash; //second hash value
    uint64_t off; //offset of the entry
    uint64_t left; //offset of the left child
    uint64_t right; //offset of the right child
} HASH_ENTRY;
#define HASH_ENTRY_SZ sizeof(HASH_ENTRY)

typedef struct hash_bucket
{
    uint64_t off; //bucket中的第一个hash entry的地址ff
} HASH_BUCKET;
#define HASH_BUCKET_SZ sizeof(HASH_BUCKET)

#define HASHDB_MAGIC 20160415

typedef uint32_t (*hashfunc_t)(const char*);

typedef struct hashdb_header
{
    uint32_t magic; //用来标志是hashdb
    uint32_t cnum; //number of cached items
    uint32_t bnum; // number of hash buckets
    uint64_t tnum; // number of total items
   // uint64_t bfoff; // offset of the bloom filter
    uint64_t hbucket_off; //offset of hash buckets
    uint64_t hentry_off; //offset of hash values
} HASHDB_HDR;
#define HASHDB_HDR_SZ sizeof(HASHDB_HDR)

class HashDB
{
public:
    HashDB(uint64_t tnum, uint32_t bnum,
           uint32_t cnum, hashfunc_t hfunc1,
           hashfunc_t hfunc2);
    virtual ~HashDB();
    int openDB(const char* dbname, const char* bfname, bool isnewdb);
    int closeDB(int flash = 1);
    int setDB(const char* key, const void* value, const int vsize);
    int getDB(const char* key, void* value, int &vsize);
    int unlinkDB();

    const char* getdbpath() { return dbpath; }
private:
    int swapout(const uint32_t hash1, const uint32_t hash2, HASH_ENTRY* he);
    int swapin (const char* key, uint32_t hash1, uint32_t hash2, HASH_ENTRY* he);
    int read2fillcache(fstream &db_file);

private:

    char dbpath[PATH_MAX_LEN]; //hashdb file path
    char bfpath[PATH_MAX_LEN]; //bloom filter path
    HASHDB_HDR header; // hashdb header
    BloomFilter* bloom;
    HASH_BUCKET* bucket; // hash buckets
    HASH_ENTRY* cache; //hash item cache
    hashfunc_t hfunc1; // hash function for hash bucket
    hashfunc_t hfunc2;
    // hash function for btree in the hash bucket
};

#endif // HASHDB_H
