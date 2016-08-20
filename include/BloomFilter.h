/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <limits>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <fstream>

#include <sys/types.h>
#include <unistd.h>
#include <string.h>

//#include "utils.h"

using namespace std;

//#define BASIC_BLOOM_SZ 48
#define BASIC_BLOOM_SZ sizeof(unsigned int)*2 + \
                       sizeof(unsigned long long int) * 4 +\
                       sizeof(double)

static const std::size_t BITS_PER_CHAR = 0x08; // 8 bits in 1 unsigned char
static const unsigned char bit_mask[BITS_PER_CHAR] = {
    0x01, 0x02, 0x04, 0x08,
    0x10, 0x20, 0x40, 0x80
};

class BloomParameters{
public:
    BloomParameters();
    virtual ~BloomParameters(){}

    inline bool operator! ();
    virtual bool computeOptPara();//计算哈希函数个数，最小的存储位数

private:
    unsigned long long int minsize; //Bloom Filter的最小位数
    unsigned long long int maxsize;

    unsigned int minhash; //所允许的哈希函数的最小数目
    unsigned int maxhash;

public:
    unsigned long long int projected_element_count;//插入bloom filter的元素的数目,按数量级来改，默认为10000
    double fpp; // false positive probability --the default is the reciprocal of the projected_element_count.
    unsigned long long int randseed;


    struct OptParametersT {
        unsigned int numhash;
        unsigned long long int tablesize;
        OptParametersT(): numhash(0), tablesize(0){}
    };

    OptParametersT optpara;
};

typedef struct _Bloom_Header{
     unsigned int saltcount_;
     unsigned long long int tablesize_;    //per bit
     unsigned long long int rawtablesize_; //per byte
     unsigned long long int projected_element_count_; //the number of expected inserted element
     unsigned int inserted_element_count_;// the number of effective inserted element
     unsigned long long int randseed_;
     double desiredfpp_;
}BloomHeader;
#define BLOOM_HDR_SZ (sizeof(BloomHeader))

class BloomFilter
{
    public:
        BloomFilter();
        BloomFilter(const BloomParameters&);
        BloomFilter(const BloomFilter&);
        virtual ~BloomFilter();

        inline virtual unsigned long long int size() const { return bf_hdr.tablesize_;}
        inline unsigned int elementCount() const { return bf_hdr.inserted_element_count_;}
        inline double effectiveFPP() const; //The effective false positive probability
        inline const unsigned char* table() const { return bittable_;}
        inline unsigned int hashCount() { return salt_.size(); }
        inline void clear();//清空BF的位表bittable_，但是并不删除存储空间

        inline BloomFilter& operator= (const BloomFilter&);
        inline BloomFilter& operator&= (const BloomFilter&); // intersection
        inline BloomFilter& operator|= (const BloomFilter&); // union
        inline BloomFilter& operator^= (const BloomFilter&); // difference
        inline bool friend operator== (const BloomFilter&, const BloomFilter&);
        inline bool friend operator! (const BloomFilter&);

        //insert an element into the BloomFilter
        void insert(const unsigned char* key_begin, const unsigned int len);
        void insert(const char* data, const unsigned int len);
        void insert(const std::string& key);
        template<class InputIter>
        inline void insert(const InputIter begin, const InputIter end);

        //query whether an element exists in the BloomFilter
        virtual bool contains(const unsigned char* key_begin, const unsigned int len) const;
        bool contains(const char* data, const unsigned int len) const;
        bool contains(const std::string& key) const;

        friend int writebf(ofstream &des_file, BloomFilter* bf);
        friend int readbf(ifstream &src_file, BloomFilter *bf);

    protected:
        inline virtual void computeIndices(const unsigned int& hashval, std::size_t& bit_index, std::size_t& bit) const;
        void genUniqueSalt();//利用andom seeds来生成唯一的哈希函数
        inline unsigned int hashAP(const unsigned char* begin, unsigned int rlen, unsigned int hashval) const; //哈希函数

    public:
        std::vector<unsigned int> salt_; //存储用于哈希函数的初始哈希值
        unsigned char* bittable_;
        BloomHeader bf_hdr;
};
bool operator== (const BloomFilter&, const BloomFilter&);
bool operator! (const BloomFilter&);

int writebf(ofstream &des_file, BloomFilter *bf);
int readbf(ifstream &src_file, BloomFilter *bf);

/** Bloom Filter 在磁盘文件中对应的存储结构为
        unsigned int saltcount_;
        unsigned long long int tablesize_;    //per bit
        unsigned long long int rawtablesize_; //per byte
        unsigned long long int projected_element_count_; //the number of expected inserted element
        unsigned int inserted_element_count_;// the number of effective inserted element
        unsigned long long int randseed_;
        double desiredfpp_;
        bittable_[0..rawtablesize_] //bloom filter的位集合
        salt_[0..saltcount_] //各个哈希函数的哈希种子
**/

inline bool operator!= (const BloomFilter&, const BloomFilter&);
inline BloomFilter operator& (const BloomFilter&, const BloomFilter&);
inline BloomFilter operator| (const BloomFilter&, const BloomFilter&);
inline BloomFilter operator^ (const BloomFilter&, const BloomFilter&);
std::ostream& operator<< (std::ostream&, const BloomParameters::OptParametersT&);
std::ostream& operator<< (std::ostream&, const BloomParameters&);


/*
class CompressibleBF : public BloomFilter{
public:
    CompressibleBF(const BloomParameters& p);
};
*/



#endif // BLOOMFILTER_H
