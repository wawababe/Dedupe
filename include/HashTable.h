/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#ifndef HASHTABLE_H
#define HASHTABLE_H

#include <iostream>
#include <list>
#include <cstring>
#include "hashfunc.h"

typedef void* ht_key;
typedef void* ht_data;
typedef struct ht_HashEntry ht_elem;
typedef std::list<ht_elem> ht_chain;


struct ht_HashEntry{
    void* key;
    void* data;
  //  unsigned int key_sz;
   // unsigned int data_sz;
};

int cmpKey(void* k1, void* k2);
unsigned int hashKey(ht_key k, int m);


class HashTable
{
    public:
        HashTable(int init_size = 997,
                  int (*cmp)(ht_key k1, ht_key k2) = cmpKey,
                  unsigned int (*hash)(ht_key k, int m) = hashKey
                  );
        virtual ~HashTable();

        void insert(ht_elem e);
        template<class T>
        inline void insert(T *str){
            ht_elem e;
            e.key = str;
            e.data = (void *)"1";
            insert(e);
        }
        ht_data search(ht_key k);
        void remove(ht_key k);
        inline bool exist(ht_key k){ return (search(k)== 0) ? false : true;}

        int size() const {return num_elems;}
        int capacity() const {return tableSize;}
        bool isEmpty() const {return !num_elems;}

        void resize();

    protected:
        struct HT_bad_init_table_size{
            HT_bad_init_table_size(){}
        };
    private:
        void resize(int cap);

    private:
        int tableSize; //m
        int num_elems; //n
        ht_chain* table;
        int (*cmp)(ht_key k1, ht_key k2); //comparing keys
        unsigned int (*hash)(ht_key, int m);   //hashing keys

    public:

};



#endif // HASHTABLE_H
