/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "BigHashTable.h"

BigHashTable::BigHashTable(const char *dbname, const char *bfname)
{
    db = new HashDB(HASHDB_DEFAULT_TNUM, HASHDB_DEFAULT_BNUM, HASHDB_DEFAULT_CNUM,
        HashFunctions::APHash, HashFunctions::JSHash);
    isnewdb = true;
    char hashdb_dbname[PATH_MAX_LEN] = {0};
    char hashdb_bfname[PATH_MAX_LEN] = {0};
    char dbtmplate[] = "BigHashTable/hashdb_XXXXXX";
    char bftmplate[] = "BigHashTable/hashdb_XXXXXX";
    if (dbname){
        sprintf(hashdb_dbname, "%s", dbname);
    }else{
        sprintf(hashdb_dbname, "data/%s_%d.db", mktemp(dbtmplate), getpid());
    }
    if (bfname){
        sprintf(hashdb_bfname, "%s", bfname);
    }else{
        sprintf(hashdb_bfname, "data/%s_%d.bf", mktemp(bftmplate), getpid());
    }
	mkdir("data", 766);
    mkdir("data/BigHashTable", 766);
    fstream db_file, bf_file;
    db_file.open(hashdb_dbname, ios::binary | ios::in);
    bf_file.open(hashdb_bfname, ios::binary | ios::in);
    isnewdb = (db_file.is_open() && bf_file.is_open()) ? false : true;
    db_file.close();
    bf_file.close();
    int ret = 0;
    ret = db->openDB(hashdb_dbname, hashdb_bfname, isnewdb);
    if (ret == -1){
        delete db;
        db = 0;
        fprintf(stderr,"Error: HashDB::open() %s, %s in BigHashTable::BigHashTable()\n", hashdb_dbname, hashdb_bfname);
    }
}

BigHashTable::~BigHashTable()
{
    if(db){
        db->closeDB(0);
        db->unlinkDB();
        delete db;
        db = 0;
    }
}

void BigHashTable::insert(const void *key, const void *data, const int datasz)
{
    db->setDB((const char *)key, data, datasz);
   /* if (-1 == db->setDB((const char*)key, data, datasz)){
        fprintf(stderr, "Error: HashDB::setDB in BigHashTable::insert \n");
        fprintf(stderr, "...   key = %s, data size = %d\n", (char *)key, datasz);
        _exit(-1);
    }
    */
}

void* BigHashTable::getvalue(const void *key, int &valuesize)
{
    char value[HASHDB_VALUE_MAX_SZ] = {0};
    void *vp = 0;
    valuesize = 0;
    int datasz = 0, ret = 0;

    if (0 != (ret = db->getDB((const char*)key, value, datasz)) ){
        if (ret == -1){
            fprintf(stderr, "Error: HashDB::getDB() in BigHashTable::getvalue()\n");
            _exit(-1);
        }
        return 0;
    }

    if (0 == (vp = malloc(datasz)))
        return 0;
    memcpy(vp, value, datasz);
    valuesize = datasz;
    /*
    cout << "In BigHashTable::getvalue(), the value is " << (char *)value
         << " , value size is "<< datasz << endl;
    cout << "In BigHashTable::getvalue(), the returned value is " << (char *)vp << endl;
    */
    return vp;
}

//#define BIGHASHTABLE_TEST
#ifdef BIGHASHTABLE_TEST
#include <string>
#include <stdio.h>
using namespace std;
int main()
{
    BigHashTable btab("data/bighaa.db", "data/bighaa.bf");
    string keys[] = {"Beauty Food", "suit", "Heal", "Orange", "range",
                     "red", "Contemporary", "OSDF", "loadandrun","campus",
                     "skin food", "suit", "suit"};
    string values[] = {"1st_newfill", "2nd", "3rd", "4th", "5th",
                       "6th", "7th", "8TH......", "9sdfthhhh", "10th......",
                      "11th********", "12f", "13so"};
    const char* key = 0;
    const char* data = 0;
    int datasz = 0;

 /*   for(int i = 0; i < 13; i++){
        key = keys[i].c_str();
        data = values[i].c_str();
        datasz = values[i].size();
        btab.insert(key, data, datasz);
    }
    btab.insert(key, data, datasz);
*/
    key = "beauty Food";
    char *value = 0;
    int value_sz = 0;
    value = (char*)btab.getvalue(key, value_sz);
    if (btab.contain(key)){
        value = (char *)realloc(value, value_sz + 1);
        value[value_sz] = 0;
        printf("Get value(%s) from Big HashTable: %s\n", key, value);
        cout << "value size: " << strlen(value) << endl;
    }else
        cout << "The BigHashTable does not contain key : " << key << endl;
    if (value){
        free(value);
        value = 0;
    }
    return 0;
}
#endif // BIGHASHTABLE_TEST
