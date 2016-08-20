/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "BloomFilter.h"

BloomParameters::BloomParameters():
    minsize(1),
    maxsize(std::numeric_limits<unsigned long long int>::max()),
    minhash(1),
    maxhash(std::numeric_limits<unsigned int>::max()),
    projected_element_count(10000),
    fpp(1.0/projected_element_count),
    randseed(0xA5A5A5A55A5A5A5AULL)
    {
    }

inline bool BloomParameters::operator!()
{
    return (minsize > maxsize) ||
           (minhash > maxhash) ||
           (minhash < 1)       ||
           (0 == maxhash)      ||
           (0 == projected_element_count) ||
           (fpp < 0.0)         ||
           (std::numeric_limits<double>::infinity() == std::abs(fpp)) ||
           (0 == randseed)      ||
           (0xFFFFFFFFFFFFFFFFULL == randseed);
}

bool BloomParameters::computeOptPara()
{
    /*The following will attempt to find the number of hash functions
    and minimum amount of storage bits required to construct a bloom
    filter consistent with the user defined false positive probability
    and estimated element insertion count.
    */
    if(!(*this)) return false;
    double min_m = std::numeric_limits<double>::infinity();
    double min_k = 0.0;
    double cur_m = 0.0;
    double k = 1.0;

    /** m--bit数组的宽度（bit数）， n--加入其中的key数,
        k--使用的hash函数的个数，   f-- false positive的比率
        根据公式：f = (1 - e^(-k*n/m) )^k      (1)
            可推出：m = -k*n / (ln(1-f^(1/k)));
        根据公式：k = -ln(f) / ln(2)           (2)
            可算出最优的k值
    给定最大的f(fpp)和n(projected_element_count),求解比较小的
    m(tablesize: the bit size )和k(numhash -- number of hash functions)
    **/
    const double upboundk = -std::log(fpp) / std::log(2) + 2;
    while ( k < upboundk){
        double numerator = (-k * projected_element_count);
        double denominator = std::log(1.0 - std::pow(fpp, 1.0/k));
        cur_m = numerator / denominator;
        if(cur_m < min_m){
            min_m = cur_m;
            min_k = k;
        }
        k += 1.0;
    }

    optpara.numhash = static_cast<unsigned int>(min_k);
    optpara.tablesize = static_cast<unsigned long long int>(min_m);
    //保证tablesize是8bit的倍数
    optpara.tablesize += ( (optpara.tablesize % BITS_PER_CHAR) != 0 ) ?
                         (BITS_PER_CHAR - optpara.tablesize % BITS_PER_CHAR) : 0;

    if(optpara.numhash < minhash) optpara.numhash = minhash;
    else if (optpara.numhash > maxhash) optpara.numhash = maxhash;

    if(optpara.tablesize < minsize) optpara.tablesize = minsize;
    else if (optpara.tablesize > maxsize) optpara.tablesize = maxsize;

    return true;
}


/******************BLOOM FILTER CLASS *****************/
BloomFilter::BloomFilter() :
    bittable_(0)
{
    bf_hdr.saltcount_ = 0;
    bf_hdr.tablesize_ = 0;
    bf_hdr.rawtablesize_ = 0;
    bf_hdr.projected_element_count_ = 0;
    bf_hdr.inserted_element_count_ = 0;
    bf_hdr.randseed_ = 0xA5A5A5;
    bf_hdr.desiredfpp_ = 0.001;
}

BloomFilter::BloomFilter(const BloomParameters& bp)
{
    bf_hdr.saltcount_ = bp.optpara.numhash;
    bf_hdr.tablesize_ = bp.optpara.tablesize;
    bf_hdr.rawtablesize_ = bf_hdr.tablesize_ / BITS_PER_CHAR;
    bf_hdr.projected_element_count_ = bp.projected_element_count;
    bf_hdr.inserted_element_count_ = 0;
    bf_hdr.randseed_ = bp.randseed * 0xA5A5A5A5 + 1;
    bf_hdr.desiredfpp_ = bp.fpp;
    genUniqueSalt();
    bittable_ = new unsigned char[static_cast<std::size_t>(bf_hdr.rawtablesize_)];
    std::fill_n(bittable_, bf_hdr.rawtablesize_, 0x00);
}

BloomFilter::BloomFilter(const BloomFilter& bf)
{
    if(this != &bf){
            this->operator=(bf);
    }
}


BloomFilter::~BloomFilter()
{
    delete[] bittable_;
}

inline double BloomFilter::effectiveFPP() const
{
    //f = ( 1 - e^(-k*n/m) )^ k
    /*
        Note:
        The effective false positive probability is calculated using the
        designated table size and hash function count in conjunction with
        the current number of inserted elements - not the user defined
        predicated/expected number of inserted elements.
    */
    return std::pow(1.0 - std::exp(-1.0*salt_.size() * bf_hdr.inserted_element_count_ / size()),
                    1.0 * salt_.size() );
}


inline void BloomFilter::clear()
{
    std::fill_n(bittable_, bf_hdr.rawtablesize_, 0x00);
    bf_hdr.inserted_element_count_ = 0;
}

inline BloomFilter& BloomFilter::operator= (const BloomFilter& bf)
{
    if(this != &bf){ //防止自复制
        memcpy(&bf_hdr,&bf.bf_hdr, BLOOM_HDR_SZ);
        delete[] bittable_;
        bittable_ = new unsigned char[static_cast<std::size_t>(bf_hdr.rawtablesize_)];
        std::copy(bf.bittable_, bf.bittable_ + bf_hdr.rawtablesize_, bittable_);
        salt_ = bf.salt_;
    }

    return *this;
}


inline BloomFilter& BloomFilter::operator&= (const BloomFilter& bf)
{
    //intersection
    if( (bf_hdr.saltcount_ == bf.bf_hdr.saltcount_) &&
        (bf_hdr.tablesize_ == bf.bf_hdr.tablesize_) &&
        (bf_hdr.randseed_  == bf.bf_hdr.randseed_ )
      ){
        for (std::size_t i = 0; i < bf_hdr.rawtablesize_; ++i)
            bittable_[i] &= bf.bittable_[i];
    }
    return *this;
}

inline BloomFilter& BloomFilter::operator|= (const BloomFilter& bf)
{
    //union
    if( (bf_hdr.saltcount_ == bf.bf_hdr.saltcount_) &&
        (bf_hdr.tablesize_ == bf.bf_hdr.tablesize_) &&
        (bf_hdr.randseed_  == bf.bf_hdr.randseed_ )
      ){
        for (std::size_t i = 0; i < bf_hdr.rawtablesize_; ++i)
            bittable_[i] |= bf.bittable_[i];
    }
    return *this;
}

inline BloomFilter& BloomFilter::operator^= (const BloomFilter& bf)
{
    //difference
    if( (bf_hdr.saltcount_ == bf.bf_hdr.saltcount_) &&
        (bf_hdr.tablesize_ == bf.bf_hdr.tablesize_) &&
        (bf_hdr.randseed_  == bf.bf_hdr.randseed_ )
      ){
        for (std::size_t i = 0; i < bf_hdr.rawtablesize_; ++i)
            bittable_[i] ^= bf.bittable_[i];
    }
    return *this;
}


/************Basic operations of the BloomFilter: insert && query****************/

void BloomFilter::insert
(const unsigned char* key_begin, const unsigned int len)
{
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for(std::size_t i = 0; i < salt_.size(); ++i){
        computeIndices(hashAP(key_begin, len, salt_.at(i)), bit_index, bit);
        bittable_[bit_index/BITS_PER_CHAR] |= bit_mask[bit];
    }
    ++bf_hdr.inserted_element_count_;
}

void BloomFilter::insert(const char* data, const unsigned int len)
{
    insert(reinterpret_cast<const unsigned char*>(data), len);
}

void BloomFilter::insert(const std::string& key)
{
    insert(reinterpret_cast<const unsigned char*>(key.c_str()), key.length());
}

template<class InputIter>
inline void BloomFilter::insert(const InputIter begin, const InputIter end)
{
    InputIter itr = begin;
    while (end != itr)
        insert(*(itr++));
}

bool BloomFilter::contains
(const unsigned char* key_begin, const unsigned int len) const
{
    std::size_t bit_index = 0;
    std::size_t bit = 0;
    for(std::size_t i = 0; i < salt_.size(); ++i){
        computeIndices(hashAP(key_begin, len, salt_.at(i)), bit_index, bit);
        if((bittable_[bit_index/BITS_PER_CHAR] & bit_mask[bit]) != bit_mask[bit] )
            return false;
    }
    return true;
}

bool BloomFilter::contains(const char* data, const unsigned int len) const
{
    return contains(reinterpret_cast<const unsigned char*>(data), len);
}

bool BloomFilter::contains(const std::string& key) const
{
    return contains(reinterpret_cast<const unsigned char*>(key.c_str()), key.size());
}

/**********The protected class function of the Bloom Filter************/
inline void BloomFilter::computeIndices
(const unsigned int& hashval, std::size_t& bit_index, std::size_t &bit) const
{
    bit_index = hashval % bf_hdr.tablesize_;
    bit = bit_index % BITS_PER_CHAR;

}

void BloomFilter::genUniqueSalt()
/*产生哈希函数的unique随机种子，存在BoomFilter::salt_容器中
Note:
    A distinct hash function need not be implementation-wise
    distinct. In the current implementation "seeding" a common
    hash function with different values seems to be adequate.
*/
{
    const unsigned int predef_salt_count = 128;
    static const unsigned int predef_salt[predef_salt_count] = {
        0xAAAAAAAA, 0x55555555, 0x33333333, 0xCCCCCCCC,
        0x66666666, 0x99999999, 0xB5B5B5B5, 0x4B4B4B4B,
        0xAA55AA55, 0x55335533, 0x33CC33CC, 0xCC66CC66,
        0x66996699, 0x99B599B5, 0xB54BB54B, 0x4BAA4BAA,
        0xAA33AA33, 0x55CC55CC, 0x33663366, 0xCC99CC99,
        0x66B566B5, 0x994B994B, 0xB5AAB5AA, 0xAAAAAA33,
        0x555555CC, 0x33333366, 0xCCCCCC99, 0x666666B5,
        0x9999994B, 0xB5B5B5AA, 0xFFFFFFFF, 0xFFFF0000,
        0xB823D5EB, 0xC1191CDF, 0xF623AEB3, 0xDB58499F,
        0xC8D42E70, 0xB173F616, 0xA91A5967, 0xDA427D63,
        0xB1E8A2EA, 0xF6C0D155, 0x4909FEA3, 0xA68CC6A7,
        0xC395E782, 0xA26057EB, 0x0CD5DA28, 0x467C5492,
        0xF15E6982, 0x61C6FAD3, 0x9615E352, 0x6E9E355A,
        0x689B563E, 0x0C9831A8, 0x6753C18B, 0xA622689B,
        0x8CA63C47, 0x42CC2884, 0x8E89919B, 0x6EDBD7D3,
        0x15B6796C, 0x1D6FDFE4, 0x63FF9092, 0xE7401432,
        0xEFFE9412, 0xAEAEDF79, 0x9F245A31, 0x83C136FC,
        0xC3DA4A8C, 0xA5112C8C, 0x5271F491, 0x9A948DAB,
        0xCEE59A8D, 0xB5F525AB, 0x59D13217, 0x24E7C331,
        0x697C2103, 0x84B0A460, 0x86156DA9, 0xAEF2AC68,
        0x23243DA5, 0x3F649643, 0x5FA495A8, 0x67710DF8,
        0x9A6C499E, 0xDCFB0227, 0x46A43433, 0x1832B07A,
        0xC46AFF3C, 0xB9C8FFF0, 0xC9500467, 0x34431BDF,
        0xB652432B, 0xE367F12B, 0x427F4C1B, 0x224C006E,
        0x2E7E5A89, 0x96F99AA5, 0x0BEB452A, 0x2FD87C39,
        0x74B2E1FB, 0x222EFD24, 0xF357F60C, 0x440FCB1E,
        0x8BBE030F, 0x6704DC29, 0x1144D12F, 0x948B1355,
        0x6D8FD7E9, 0x1C11A014, 0xADD1592F, 0xFB3C712E,
        0xFC77642F, 0xF9C4CE8C, 0x31312FB9, 0x08B0DD79,
        0x318FA6E7, 0xC040D23D, 0xC0589AA7, 0x0CA5C075,
        0xF874B172, 0x0CF914D5, 0x784D3280, 0x4E8CFEBC,
        0xC569F575, 0xCDB2A091, 0x2CC016B4, 0x5C5F4421
    };
    if (bf_hdr.saltcount_ <= predef_salt_count)
    {
        std::copy(predef_salt, predef_salt+bf_hdr.saltcount_,
                  std::back_inserter(salt_));
        for (std::size_t i = 0; i < salt_.size(); ++i)
        {
            //整合用户定义的随机种子randseed_
            salt_.at(i) = salt_.at(i) * salt_.at( (i+3)% salt_.size() ) +
                          static_cast<unsigned int>(bf_hdr.randseed_);
        }
    }else{
        std::copy(predef_salt, predef_salt+predef_salt_count,
                  std::back_inserter(salt_));
        srand(static_cast<unsigned int>(bf_hdr.randseed_));
        while (salt_.size() < bf_hdr.saltcount_){
                unsigned int current_salt = static_cast<unsigned int>(
                                    rand()*static_cast<unsigned int>(rand())
                                                                           );
                if (0 == current_salt) continue;
                if(salt_.end() == std::find(salt_.begin(), salt_.end(), current_salt)){
                    salt_.push_back(current_salt);
                }
        }
    }
}

inline unsigned int BloomFilter::hashAP
(const unsigned char* begin, unsigned int rlen, unsigned int hashval) const
{
    const unsigned char* itr = begin;
    while(rlen >= 8){
        const unsigned int& i1 = *(reinterpret_cast<const unsigned int*>(itr));
        itr += sizeof(unsigned int);
        const unsigned int& i2 = *(reinterpret_cast<const unsigned int*>(itr));
        itr += sizeof(unsigned int);
        hashval ^= (hashval << 7) ^ i1 * (hashval >> 3) ^
                    ( ~((hashval << 11) + (i2 ^ (hashval >> 5))) );
        rlen -= 8;
    }

    unsigned int loop = 0;
    if(rlen){
        if(rlen >= 4){
            const unsigned int& i = *(reinterpret_cast<const unsigned int*>(itr));
            if(loop & 0x01)
                hashval ^= (hashval << 7) ^ i * (hashval >> 3);
            else
                hashval ^= ( ~( (hashval << 7) + (i^(hashval >> 5)) ) );
            ++loop;
            rlen -= 4;
            itr += sizeof(unsigned int);
        }
        if(rlen >= 2){
            const unsigned short& i = *(reinterpret_cast<const unsigned short*>(itr));
            if(loop & 0x01)
                hashval ^= (hashval << 7) ^ i * (hashval >> 3);
            else
                hashval ^= ( ~( (hashval << 7) + (i^(hashval >> 5))) );
            ++loop;
            rlen -= 2;
            itr += sizeof(unsigned short);
        }
        if(rlen){
            hashval += ( (*itr) ^ (hashval * 0xA5A5A5A5) ) + loop;
        }
    }
    return hashval;
}

/****************友元 或 协助函数************************/
inline bool operator== (const BloomFilter& bfx, const BloomFilter& bfy)
{
    if(&bfx != &bfy){
      /*  return bfx.bf_hdr.saltcount_ == bfy.bf_hdr.saltcount_ &&
               bfx.bf_hdr.tablesize_ == bfy.bf_hdr.tablesize_ &&
               bfx.bf_hdr.rawtablesize_ == bfy.bf_hdr.rawtablesize_ &&
               bfx.bf_hdr.projected_element_count_ == bfy.bf_hdr.projected_element_count_ &&
               bfx.bf_hdr.inserted_element_count_ == bfy.bf_hdr.inserted_element_count_ &&
               bfx.bf_hdr.randseed_ == bfy.bf_hdr.randseed_ &&
               bfx.bf_hdr.desiredfpp_ == bfy.bf_hdr.desiredfpp_ &&
               bfx.salt_ == bfy.salt_;
        */
        return  (0 == memcmp(&bfx.bf_hdr, &bfy.bf_hdr, BLOOM_HDR_SZ)) && (bfx.salt_ == bfy.salt_);

    }
    return true;
}

inline bool operator! (const BloomFilter& bf)
{
    return (0 == bf.bf_hdr.tablesize_);
}

inline bool operator!= (const BloomFilter& bfx, const BloomFilter& bfy)
{
    return !(bfx == bfy);
}

inline BloomFilter operator& (const BloomFilter& bfx, const BloomFilter& bfy)
{
    BloomFilter result = bfx;
    return result &= bfy;
}

inline BloomFilter operator| (const BloomFilter& bfx, const BloomFilter& bfy)
{
    BloomFilter result = bfx;
    return result |= bfy;
}

inline BloomFilter operator^ (const BloomFilter& bfx, const BloomFilter& bfy)
{
    BloomFilter result = bfx;
    return result ^= bfy;
}

std::ostream& operator<< (std::ostream& s, const BloomParameters::OptParametersT& optp)
{
    return s << "The Optimal Parameters \n"
             << "    <1>. The optimal number of hash functions:         " << optp.numhash   << '\n'
             << "    <2>. The optimal number of bits of bloom filter:   " << optp.tablesize;
}

std::ostream& operator<< (std::ostream& s, const BloomParameters& bp)
{
    return s << "-------------The Parameters of the Bloom Filter--------------\n"
             << " 1. The number of elements to be inserted into the BF: " << bp.projected_element_count << '\n'
             << " 2. The false positive probability of the bloom filter:" << bp.fpp << '\n'
             << " 3. " << bp.optpara;
}

int writebf(ofstream &des_file, BloomFilter *bf)
//将BlooomFilter 写到文件fd
{
    if(!des_file.is_open()){
        cout << "Error: invalid argument -- ofstream des_file not open in BloomFilter::writebf " << endl;
        return -1;
    }
    int wsize = 0;
    des_file.write((const char *)(&(bf->bf_hdr)), BLOOM_HDR_SZ);
    wsize = BLOOM_HDR_SZ;
    cout << "write the bloom filter header size = " << wsize << endl;

    des_file.write((const char*)bf->bittable_, bf->bf_hdr.rawtablesize_);

    for(vector<unsigned int>::const_iterator ci = bf->salt_.begin(); ci != bf->salt_.end(); ci++){
            unsigned int k = *ci;
            des_file.write((const char*)(&k), sizeof(unsigned int));
    }
    return BLOOM_HDR_SZ + bf->bf_hdr.rawtablesize_ + bf->bf_hdr.saltcount_ * sizeof(unsigned int);
}


int readbf(ifstream &src_file, BloomFilter *bf)
{
    unsigned int rsize = 0;
    src_file.read((char*)(&(bf->bf_hdr)), BLOOM_HDR_SZ);
    rsize = src_file.gcount();
    unsigned char* table = new unsigned char[static_cast<size_t>(bf->bf_hdr.rawtablesize_)];
    memset(table, 0, bf->bf_hdr.rawtablesize_);
    src_file.read((char *)table, bf->bf_hdr.rawtablesize_);
    rsize += src_file.gcount();
    if ( (rsize - BLOOM_HDR_SZ) != bf->bf_hdr.rawtablesize_){
        cout << "Error: read table in BloomFilter::read bf" << endl;
    }
    delete[] bf->bittable_;
    bf->bittable_ = table;
    table = 0;

    for(unsigned int i = 0; i < bf->bf_hdr.saltcount_; i++ ){
            unsigned int k = 0;
            src_file.read((char *)(&k), sizeof(unsigned int));
            if(i < bf->salt_.size())
                bf->salt_.at(i) = k;
            else
                bf->salt_.push_back(k);
    }
    rsize += bf->bf_hdr.saltcount_ * sizeof(unsigned int);
    return rsize;
}


//#define BLOOMFILTER_TEST
#ifdef BLOOMFILTER_TEST

#include <iostream>
#include <fstream>

using namespace std;

int main()
{
    cout << "BloomParameter and BloomFilter Class Test:" << endl;
    BloomParameters pmt;
    pmt.projected_element_count = 1000; // expect to insert 1000 elements into the filter
    pmt.fpp = 0.0001; //maximum tolerable false positive probability
    pmt.randseed = 0xA5A5A5A5;

    if(!pmt){
        cout << "Error - Invalid set of bloom filter parameters!" << endl;
        return 1;
    }
    pmt.computeOptPara();

    cout << pmt << endl;

    BloomFilter bf(pmt);
    BloomFilter bf2;
    cout << ((bf == bf2) ? "bf and bf2 are same." : "bf and bf2 are different.") << endl;

    string str_list[] = {"Fill", "Search", "DEsigned"};
    //insert into BloomFilter
    {
        for(size_t i = 0; i < (sizeof(str_list) / sizeof(string)); ++i){
            bf.insert(str_list[i]);
        }
    }

    //query BloomFilter
    {
        for(std::size_t i = 0; i < (sizeof(str_list) / sizeof(string)); ++i){
            if(bf.contains(str_list[i]))
                std::cout << "BF contains: " << str_list[i] << endl;
        }
    }

    string invalid_str_list[] = {"word", "suit", "layout", "designed", "sEarch"};
    //query the existence of invalid strings
    for (size_t i = 0; i < ( sizeof(invalid_str_list) / sizeof(string) ); ++i){
        if(bf.contains(invalid_str_list[i]))
            cout << "BF falsely contains: " << invalid_str_list[i] << endl;
    }

    char tmpname[] = "data/bloom.bf";
    /*
    ofstream des_file;
    des_file.open(tmpname, ios::binary);
    writebf(des_file, &bf);
    des_file.close();
    */

    ifstream src_file;
    src_file.open(tmpname, ios::binary);
    BloomFilter rbloom;
    readbf(src_file, &rbloom);

    if(rbloom.contains(str_list[1]))
            cout << "read_bloom contains: " << str_list[1] << endl;
    if(! rbloom.contains(invalid_str_list[0]))
            cout << "read_bloom does not contain: " << invalid_str_list[0] << endl;
    src_file.close();

    return 0;
}
#endif // BLOOMFILTER_TEST
