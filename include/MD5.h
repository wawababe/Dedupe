#ifndef MD5_H
#define MD5_H

#include <string>
#include <fstream>
#include <cstddef>
#include <cstring>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using std::string;
using std::ifstream;
using std::fstream;

typedef unsigned char byte;
typedef unsigned long ulong;

/** Message Digest 5 class **/
class MD5
{
    public:
        MD5();
        MD5(const void* input, unsigned int len);
        MD5(const string &str);
        MD5(ifstream &in);
        void reset();
        virtual ~MD5(){};

        const byte* getDigest(){return _digest;}
        string toString();
        void toCString(unsigned char* md5val);
        void showMD5();
        string operator() (const string &str);

        static void message_digest_func(const char *str, const unsigned int len, unsigned char *md5val);
        static void message_digest_func(fstream& file, unsigned char *md5val);
    private:
        void update(const byte* input, unsigned int len);
        void final();
        void transform(const byte block[64]);
        void encode(const ulong* input, byte* output, unsigned int len);
        void decode(const byte* input, ulong* output, unsigned int len);
        string bytesToHexString(const byte* input, unsigned int len);

        /* class uncopyable */
        MD5(const MD5&);
        MD5& operator=(const MD5&);

    private:
        ulong _state[4]; /* state (ABCD) */
        ulong _count[2]; /* number of bits,mod 2^64, low-order word first */
        byte _buffer[64]; /* input buffer */
        byte _digest[16]; /* message digest */

        static const byte _PADDING[64];
        static const char _HEX_SET[16];
        static const unsigned int _BUFFER_SIZE = 1024;

    public:
        class _bad_ifstream{};
};

#endif // MD5_H
