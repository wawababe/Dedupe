#include "HashTable.h"
#include <iostream>



HashTable::HashTable
(int init_size,
 int (*cmp)(ht_key k1, ht_key k2),
 unsigned int (*hash)(ht_key k, int m)
)
{
    if(init_size <= 1)
        throw HT_bad_init_table_size();
    tableSize = init_size;
    num_elems = 0;
    this->cmp = cmp;
    this->hash = hash;
    table = new ht_chain[tableSize];
}

HashTable::~HashTable()
{
    delete[] table;
}


void HashTable::insert(ht_elem e)
{
    ht_key k = e.key;
    unsigned int i = hash(k, tableSize);
    for(ht_chain::iterator it = table[i].begin(); it != table[i].end(); it++){
        if (0 == cmp(k, it->key)){
                it->key = e.key;
                it->data = e.data;
        }
    }
    table[i].push_front(e);
    num_elems++;
#ifdef HASHTABLE_TEST
    std::cout << "Debug HashTable::insert: " << (const char*)table[i].front().key << std::endl;
#endif
    if(num_elems >= tableSize/2)
        resize(2*tableSize);
}

ht_data HashTable::search(ht_key k)
{
    unsigned int i = hash(k, tableSize);
    for(ht_chain::const_iterator cit = table[i].begin(); cit != table[i].end(); cit++){
        #ifdef HASHTABLE_TEST
        std::cout << "Debug HashTable::search: " << (const char*)(cit->key) << std::endl;
        #endif
        if ( 0 == cmp(k, cit->key) ){
            #ifdef HASHTABLE_TEST
            std::cout << "Debug HashTable::search: " << (const char*)(cit->key) << std::endl;
            #endif
            return cit->data;
        }
    }
    return 0;
}

void HashTable::remove(ht_key k)
{
    unsigned int i = hash(k, tableSize);
    for(ht_chain::iterator it = table[i].begin(); it != table[i].end(); it++){
        if (0 == cmp(k, it->key)){
            table[i].erase(it);
            num_elems--;
            break;
        }
    }
    if(num_elems > 0 && num_elems <= tableSize / 8)
        resize(tableSize/2);
}

void HashTable::resize(int cap)
{
    HashTable* ht;
    ht = new HashTable(cap);
    for(int i = 0; i < tableSize; ++i){
        for(ht_chain::const_iterator ci = table[i].begin(); ci != table[i].end(); ci++){
            ht_elem e;
            e.key = ci->key;
            e.data = ci->data;
            ht->insert(e);
        }
    }
    tableSize = ht->tableSize;
    num_elems = ht->num_elems;
    cmp = ht->cmp;
    hash = ht->hash;
    delete[] table;
    table = ht->table;
}

int cmpKey(void* k1, void* k2)
{
    return strcmp((char*)(k1), (char*)(k2));
}

unsigned int hashKey(ht_key k, int m)
{
    return HashFunctions::RSHash((const char*)k) % m;
}

//#define HASHTABLE_TEST
#ifdef HASHTABLE_TEST

#include <string>
#include <iostream>
using namespace std;

int main()
{
    HashTable ht;
    ht_elem pe;
    string str = "abcdef";
    const char* cstr = str.c_str();
    string str1 = "1233";
    const char* cstr1 = str1.c_str();
    string str3 = "abcdef";
    const char* cstr3 = str3.c_str();
    string str4 = "str4~";
    const char* cstr4 = str4.c_str();
    pe.key = (void*)cstr;
    pe.data = (void*)cstr1;

    cout << "Before insert, Hash Table Size: " << ht.capacity() << endl;
    ht.insert(pe);
    pe.key = (void*)cstr4;
    ht.insert(pe);
    cout << "After insert, Hash Table Size: " << ht.capacity() << endl;

    ht_data p = ht.search((void*)cstr3);
    cout << "HashTable::search: " << (char*)(p) << endl;

    ht.remove(pe.key);
    cout << "After remove, Hash Table Size: " << ht.capacity() << '\n'
         << "      number of elements     : " << ht.size() << endl;
   // ht.insert(pe);
    if( ! ht.search(pe.key) )
        cout << "Delete Key: " << (char*) pe.key << endl;

    return 0;
}


#endif // HASHTABLE_TEST
