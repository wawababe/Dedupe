/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "ListDB.h"

ListDB::ListDB(unsigned int unit_sz, unsigned int cache_sz, unsigned int swap_sz)
{
    dbname = 0;
    unit_size = unit_sz;
    cache_size = cache_sz;
    swap_size = swap_sz;

    cache_group_nr = cache_size / swap_size;
    cache = (void *)malloc(cache_size);
    le_array = (LIST_ENTRY *)malloc(LIST_ENTRY_SZ * cache_group_nr);
    if (!cache || !le_array){
        if(cache){
            free(cache);
            cache = 0;
        }
        if(le_array){
            free(le_array);
            le_array = 0;
        }
        fprintf(stderr, "Error: malloc list entry array in ListDB::ListDB()\n");
        _exit(-1);
    }
    memset(cache, 0, cache_size);
    memset(le_array, 0, LIST_ENTRY_SZ * cache_group_nr);
}

int ListDB::opendb(const char *path)
{
    bool isnewdb = false;
    dbname = strdup(path);
    db_file.open(path, ios::binary | ios::in);
    if (!db_file.is_open()){
        fprintf(stderr, "Warning: listDB %s not exist, expect to create a new one in ListDB::opendb(...)\n", path);
        db_file.open(path, ios::binary | ios::out);
        if (!db_file.is_open()){
            fprintf(stderr, "Error: create DB failed in ListDB::opendb(...)\n");
            return -1;
        }
        isnewdb = true;
        db_file.close();
    }
    db_file.open(path, ios::binary | ios::in);
    db_file.seekg(0, ios::beg);
    if (!db_file.is_open()){
        fprintf(stderr, "Error: open ListDB in read and write mode in ListDB::opendb(...)");
        return -1;
    }
    // read ahead to fill up cache
    unsigned int rsize = 0;
    db_file >> noskipws;
    db_file.read((char *)cache, cache_size);
    rsize = db_file.gcount();
    int cache_count = rsize / swap_size;
    for (int i = 0; i < cache_count; i++){
        fprintf(stderr, "Info: set le_array[%d] as cached in ListDB::openDB(...)\n", i);
        le_array[i].iscached = true;
        le_array[i].file_offset = i * swap_size;
        le_array[i].cache_offset = i * swap_size;
    }
    db_file.close();
    return 0;
}

int ListDB::closedb()
{
    db_file.open(dbname, ios::binary | ios::out | ios::in);
    if (!db_file.is_open()){
        fprintf(stderr, "Error: open ListDB in ListDB::closedb(...)\n");
        return -1;
    }
    // flush cached data into file
    if (le_array && cache){
        for (unsigned int i = 0; i < cache_group_nr; i++){
            if(le_array[i].iscached){
                db_file.seekp(le_array[i].file_offset, ios::beg);
                db_file.write((const char*)cache + le_array[i].cache_offset, swap_size);
            }
        }
    }

    db_file.close();
    if (le_array){
        free(le_array);
        le_array = 0;
    }
    if (cache){
        free(cache);
        cache = 0;
    }

    return 0;
}

int ListDB::setvalue(const unsigned int index, void *value)
{
    return value_set_get(index, value, VALUE_SET);
}

int ListDB::getvalue(const unsigned int index, void *value)
{
    return value_set_get(index, value, VALUE_GET);
}

int ListDB::unlinkdb()
{
    if(dbname){
        unlink(dbname);
        free(dbname);
        dbname = 0;
    }
    return 0;
}

ListDB::~ListDB()
{
}


int ListDB::swapin(unsigned int pos, unsigned long file_off)
{
    if (!cache || !le_array){
        fprintf(stderr, "Error: cache or le_array not exist in ListDB::swapin(...)\n");
        return -1;
    }
    if (le_array[pos].iscached){
        fprintf(stderr, "Error: ListDB::le_array[%d] has cached items in ListDB::swapin(...)\n", pos);
        return -1;
    }

    struct stat stat_buf;
    stat(dbname, &stat_buf);
    if (stat_buf.st_size == 0){
      //  fprintf(stderr, "Warning: the ListDB is empty in ListDB::swapin(...)\n");
        return -2;
    }


    db_file.open(dbname, ios::binary | ios::in);
    if (!db_file.is_open()){
        fprintf(stderr, "Error: ListDB %s not open in ListDB::swapin(...)\n", dbname);
        return -1;
    }
    db_file >> noskipws;

    le_array[pos].file_offset = file_off;
    le_array[pos].cache_offset = pos * swap_size;
    memset((char *)cache + le_array[pos].cache_offset, '\n', swap_size); // 没有设置值的标志

    db_file.seekg(file_off, ios::beg);
    int cur_offset = db_file.tellg();
    if (cur_offset != file_off){
        cout << "Error: 1.file's cur_offset is " << (unsigned long )cur_offset << endl;
        fprintf(stderr, "   2. db_file.seekg to offset %lu failed in ListDB::swapin(...)\n", file_off);
        db_file.close();
        return -1;

    }

    unsigned int rsize = 0;
    db_file.read((char *)cache+le_array[pos].cache_offset, swap_size);
    rsize = db_file.gcount();
    if (rsize == 0){
        fprintf(stderr, "Warning: read nothing from ListDB in ListDB::swapin(...)\n");
        db_file.close();
        return -1;
    }
    le_array[pos].iscached = true;

    db_file.close();
    return 0;
}

int ListDB::swapout(unsigned int pos)
{
    if (!cache || !le_array){
        fprintf(stderr, "Error: cache or le_array not exist in ListDB::swapout(...)\n");
        return -1;
    }
    if (!le_array[pos].iscached){
        fprintf(stderr, "Error: ListDB::le_array[%d] does not have cached items in ListDB::swapout(...)\n", pos);
        return -1;
    }

    db_file.open(dbname, ios::binary | ios::out | ios::in);
    if (!db_file.is_open()){
        fprintf(stderr, "Error: ListDB %s not open in ListDB::swapout(...)\n", dbname);
        return -1;
    }

    db_file.seekp(le_array[pos].file_offset, ios::beg);
    unsigned long cur_offset = db_file.tellp();
    if (cur_offset != le_array[pos].file_offset){
        cout << "Error: db.file.seekp to offset " << (unsigned long)le_array[pos].file_offset << " failed in ListDB::swapout(...)" << endl;
        db_file.close();
        return -1;

    }
    db_file.write((const char*)cache + le_array[pos].cache_offset, swap_size);
    le_array[pos].iscached = false;

    if (db_file.is_open()){
        db_file.close();
    }
    return 0;
}

int ListDB::value_set_get(unsigned int index, void *val, uint8_t op)
{
    if (!cache || !le_array){
        fprintf(stderr, "Error: null cache or le_array in ListDB::value_set_get(...)\n");
        return -1;
    }

    //cache swapin() / swapout() with setvalue() associative
    unsigned long file_off = index * unit_size;
    file_off = (file_off / swap_size) * swap_size;
    unsigned int pos = (file_off / swap_size) % cache_group_nr; //第几组cache小分块

    if (le_array[pos].iscached){
        if (le_array[pos].file_offset != file_off){
            if (-1 == swapout(pos)){
                fprintf(stderr, "Error: swapout cache in le_array[%d] in ListDB::value_set_get(...)\n", pos);
                return -1;
            }
        }
    }

    if (!le_array[pos].iscached){
        if (-1 == swapin(pos, file_off)){
            cout << "Error: swapin from file offset = " << file_off << " into le_array[" << pos <<"] failed in ListDB::value_set_get(...)" << endl;
            return -1;
        }
    }

    unsigned int cache_off = (index * unit_size) % swap_size;
    switch (op){
    case VALUE_SET:
        le_array[pos].file_offset = file_off;
        le_array[pos].iscached = true;
        le_array[pos].cache_offset = pos * swap_size;
        memcpy((char *)cache + le_array[pos].cache_offset + cache_off, val, unit_size);
        break;
    case VALUE_GET:
        //check whether the specified value has been set
        int notset = 1;
        for (unsigned int i = 0; i < unit_size; i++){
            if ('\n' !=  *( (char *)cache + le_array[pos].cache_offset + cache_off + i ) )
                notset = 0;
        }
        if (notset){
            fprintf(stderr, "Warning: get value at index=%d failed, since value has not been set before in ListDB::value_set_get(...)\n", index);
            return -2;
        }
        memcpy(val, (char *)cache + le_array[pos].cache_offset + cache_off, unit_size);
        break;
    }

    return 0;
}


//#define LISTDB_TEST
#ifdef LISTDB_TEST

#include <stdint.h>
// 参数设置例子： data/listdb.txt 10
int main(int argc, char *argv[])
{
    char *dbname = argv[1];
    uint32_t maxn = atol(argv[2]);
    uint32_t i, value, ret;

    ListDB *db = new ListDB(sizeof(uint32_t), DEFAULT_CACHE_SZ, DEFAULT_SWAP_SZ);
    if (!db){
        fprintf(stderr, "ListDB constructor failed\n");
        exit(-1);
    }

    if (-1 == db->opendb(dbname)){
        fprintf(stderr, "ListDB::opendb() failed\n");
        exit(-2);
    }

    //set values
    for (i = 0; i < maxn; i++){
        value = i+10;
        if ( -1 == db->setvalue(i, &value)){
            fprintf(stderr, "ListDB::setvalue() failed\n");
            exit(-3);
        }
        fprintf(stdout, "set %d value = %d\n", i, value);
    }

    //get values
	for (i = 0; i <= maxn; i++) {
		ret = db->getvalue(i, &value);
		switch (ret) {
		case -2:
			fprintf(stderr, "The value of #%d has not been set yet.\n", i);
			break;
		case -1:
			fprintf(stderr, "ListDB::getvalue() failed\n");
			exit(-4);
		case  0:
			fprintf(stdout, "get %d value = %d\n", i, value);
		}
	}

	db->closedb();
	//db->unlinkdb();
	delete db;
    return 0;
}
#endif // LISTDB_TEST
