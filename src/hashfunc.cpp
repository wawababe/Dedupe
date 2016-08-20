#include "hashfunc.h"
#include<cstring>

namespace HashFunctions
{
    unsigned int APHash(const char* str)
    {
        //@brief AP Hash Function
        //@inventor: Aash Partow
        register unsigned int hashval = 0xAAAAAAAA;
        unsigned int ch;
        for (long i = 0; ch = (unsigned int)*str++; i++)
        {
            hashval ^= ((i & 1) == 0) ? ((hashval << 7) ^ ch * (hashval >> 3)) :
            ( ~( (hashval << 11) + (ch ^ (hashval >> 5)) ) );
        }
        return hashval;

    }

    unsigned int BKDRHash(const char *str)
    {
        //@brief BKDR Hash Function
        //@detail 出自 BrainKrnighan 与 Dnnis Ritchie <<The C Programming Language>>
        unsigned int seed = 131; //可选累乘因子： 31 131 1313 13131 131313 etc.
        register unsigned int hashval = 0;
        while (unsigned int ch = (unsigned int)*str++)
        {
            hashval = hashval * seed + ch;
        }
        return hashval;
    }

    unsigned int BPHash  (const char* str)
    {
        unsigned int hashval = 0;
        while(unsigned int ch = (unsigned int)*str++)
        {
            hashval = hashval << 7 ^ ch;
        }
        return hashval;
    }

    unsigned int DEKHash(const char* str)
    {
        //@brief DEK Function
        //@detail 源于Donald E. Knuth 的 Art of Computer Programming 卷三
        if(!*str)
            return 0;
        register unsigned int hashval = 1315423911;
        //unsigned int hashval = static_cast<unsigned int>(str.length());
        while (unsigned int ch = (unsigned int)*str++)
            hashval = ( (hashval << 5) ^ ( hashval >> 27)) ^ ch;
        return hashval;
    }

    unsigned int DJBHash(const char* str)
    {
        //@brief DJB Hash Function
        //@detail 源于Dniel J. Bernstein

        register unsigned int hashval = 5381;
        while (unsigned int ch = (unsigned int)*str++)
            hashval = ((hashval << 5) + hashval) + ch;

        return hashval;
    }

    unsigned int DJB2Hash(const char* str)
    {
        //@brief DJB Hash Function 2
        //@detail 源于Dniel J. Bernstein
        register unsigned int hashval = 5381;
        while (unsigned int ch = (unsigned int)*str++)
            hashval = hashval * 33 ^ ch;

        return hashval;

    }

    unsigned int ELFHash(const char* str)
    {
        //@brief ELF Hash Function
        //@detail 源于Unix的Etended Library Function, 实质是PJW Hash 的变体
        unsigned int hashval = 0;
        unsigned int magic = 0;
        while (unsigned int ch = (unsigned int)*str++)
        {
            hashval = ( hashval << 4 ) + ch;
            if ( (magic = hashval & 0xF0000000L) != 0)
            {
                //如果最高的四位不为0，
                hashval ^= (magic >> 24);
            }
            hashval &= ~magic;
        }
        return hashval;
    }

    unsigned int FNVHash(const char* str)
    {
        //@brief FNV Hash Function
        //@detail 源于Unix 系统
        const unsigned int fnv_prime = 0x811C9DC5;
        register unsigned int hashval = 0;
        while ( unsigned int ch = (unsigned int)*str++)
        {
            hashval *= fnv_prime;
            hashval ^= ch;
        }
        return hashval;
    }

    unsigned int JSHash(const char* str)
    {
        //@brief JS Hash Function
        //@detail 源于 Jstin Sobel
        register unsigned int hashval = 1315423911;
        while (unsigned int ch = (unsigned int)*str++)
            hashval ^= ( (hashval << 5) + ch + (hashval >> 2));
        return hashval;

    }

    unsigned int PJWHash(const char* str)
    {
        //@brief PJW Hash Function
        //@detail 源于AT&T Bell 实验室的 Pter J. Weinberger
        unsigned int BitsinUnsignedInt = (unsigned int)sizeof(unsigned int) * 8;
        unsigned int ThreeQuaters     = (unsigned int)BitsinUnsignedInt * 3 / 4;
        unsigned int OneEighth       = (unsigned int)BitsinUnsignedInt / 8;
        unsigned int HighBits       = (unsigned int)(0xFFFFFFFF ) << (BitsinUnsignedInt - OneEighth);
        unsigned int hashval       = 0;
        unsigned int magic        = 0;
        while (unsigned int ch = (unsigned int)*str++)
        {
            hashval = ( hashval << OneEighth ) + ch;
            if ( (magic = hashval & HighBits) != 0)
            {
                hashval = (hashval ^ (magic >> ThreeQuaters)) & (~HighBits);
            }
        }
        return hashval;
    }

    unsigned int RSHash(const char* str)
    {
        //@brief RS Hash Function;
        //@detail 源于 Robert Sedgwicks 的 Algorithms in C
        register unsigned int hashval = 0;
        unsigned int a = 63689;
        unsigned int b = 378551;
        while (unsigned int ch = (unsigned int)*str++)
        {
            hashval = hashval * a + ch;
            a *= b;
        }
        return hashval;
    }

    unsigned int SDBMHash(const char* str)
    {
        //@brief SDBM Hash Function
        //@detail
        register unsigned int hashval = 0;
        while (unsigned int ch = (unsigned int)*str++)
        {
            hashval = hashval * 65599 + ch;
            //hashval = ch + (hashval << 6) + (hashval <<16) - hashval;
        }
        return hashval;
    }

    unsigned int CRCHash(const char* str)
    {
        //@brief CRC Hash Function
        //@detail CRC校验
        unsigned int nleft   = strlen(str);
        unsigned long long  sum     = 0;
        unsigned short int *w       = (unsigned short int *)str;
        unsigned short int  answer  = 0;
        while ( nleft > 1 )
        {
            sum += *w++;
            nleft -= 2;
        }
        if ( 1 == nleft )
        {
            *( unsigned char * )( &answer ) = *( unsigned char * )w ;
            sum += answer;
        }
        sum = ( sum >> 16 ) + ( sum & 0xFFFF );
        sum += ( sum >> 16 );
        answer = ~sum;

        return (answer & 0xFFFFFFFF);
    }
}

/**************************************
    Hash Function Test
**************************************/
//#define HASH_TEST
#ifdef HASH_TEST
#include <iostream>
#include <string>
#include "include/hashfunc.h"

using namespace std;
using namespace HashFunctions;
int main()
{

    string str = "fill";
    const char key[] = "fill";
    cout << "Hash Function Algorithms Test" << endl;
    cout << "Key: "                         << key             << endl;
    cout << " 0. AP-Hash Function Value:   " << APHash(str.c_str()) << endl;
    cout << " 1. AP-Hash Function Value:   " << APHash(key)    << endl;
    cout << " 2. BKDR-Hash Function Value: " << BKDRHash(key)  << endl;
    cout << " 3. BP-Hash Function Value:   " << BPHash(key)    << endl;
    cout << " 4. DJB-Hash Function Value:  " << DJBHash(key)   << endl;
    cout << " 5. DJB2-Hash Function Value: " << DJB2Hash(key)  << endl;
    cout << " 6. DEK-Hash Function Value:  " << DEKHash(key)   << endl;
    cout << " 7. ELF-Hash Function Value:  " << ELFHash(key)   << endl;
    cout << " 8. FNV-Hash Function Value:  " << FNVHash(key)   << endl;
    cout << " 9. JS-Hash Function Value:   " << JSHash(key)    << endl;
    cout << "10. PJW-Hash Function Value:  " << PJWHash(key)   << endl;
    cout << "11. RS-Hash Function Value:   " << RSHash(key)    << endl;
    cout << "12. SDBM-Hash Function Value: " << SDBMHash(key)  << endl;
    cout << "13. CRC-Hash Function Value:  " << CRCHash(key)   << endl;
    return 0;
}


#endif // HASH_TEST

