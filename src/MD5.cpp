#include "MD5.h"


const byte MD5::_PADDING[64] = { 0x80 };
const char MD5::_HEX_SET[16] = {
    '0', '1', '2', '3',
    '4', '5', '6', '7',
    '8', '9', 'A', 'B',
    'C', 'D', 'E', 'F'
};//此种方式的定义更加快捷

//const char* MD5::_hex = "0123456789ABCDEF";
void MD5::reset()
{
    /* reset number of bits. */
    _count[0] = _count[1] = 0;

    /* Load magic initialization constants*/
    /* _state 保存MD5计算结果，总共128位，初始化为由低到高：0123456789 ABCDEF FEDCBA 9876543210*/
    _state[0] = 0x67452301;
    _state[1] = 0xEFCDAB89;
    _state[2] = 0x98BADCFE;
    _state[3] = 0x10325476;
}

MD5::MD5()
{
    reset();
}
/* Construct a MD5 object with an input buffer. */
MD5::MD5(const void* input, unsigned int len)
{
    reset();
    update((const byte*)input, len);
    final();
}

/* Construct a MD5 object with a string */
MD5::MD5(const string &str)
{
    reset();
    update((const byte*)str.c_str(), str.length());
    final();
}

/* Construct a MD5 object with a file */
MD5::MD5(ifstream &in)
{
    reset();
    // update the context from a file
    if (!in)
        throw _bad_ifstream();
    std::streamsize len;
    char buffer[_BUFFER_SIZE]; //需要实验确定用来容纳文件流的缓冲器的最佳大小
    while (!in.eof()){
        in.read(buffer, _BUFFER_SIZE);
        len = in.gcount();
        if (len > 0)
            update((byte *)buffer, len);
    }
    in.close();
    final();
}

string MD5::operator() (const string &str)
{
    reset();
    update((const byte*)str.c_str(), str.length());
    final();
    return toString();
}


/** MD5 block update operation. Continues an MD5
message-digest operation, processing another message
block, and updating the context.
**/
void MD5::update(const byte* input, unsigned int len)
{
    unsigned int i = 0, index = 0, partLen = 0;
    //_finished = false;

    /*compute number of bytes mod 64 */
    index = (_count[0] >> 3) & 0x3F; //(_count[0] / 8)bytes % 64 == (_count[0] /8 )bytes & 63

    partLen = 64 - index; //_buffer剩余的空闲空间

    /* update number of bits */
    _count[0] += len << 3;  // _count[0] += len * 2^3 bit
    if (_count[0] < (len << 3) ) _count[1]++;
    _count[1] += len >> 29; // _count[1] += (len * 2^3 bit) / 2^(32);

    /* transform as many times as possible. */
    if (len >= partLen){
        memcpy(&_buffer[index], input, partLen);
        transform(_buffer);
        for(i = partLen; i+64 <= len; i += 64){
            transform(&input[i]);
        }
        index = 0;
    }else{
        i = 0;
    }

    /* Buffer remaining input */
    memcpy(&_buffer[index], &input[i], len-i);
}


/** MD5 finalization. Ends an MD5 message-digest operation,
writing the message-digest and zeroing the context.
**/
void MD5::final()
{
    byte bits[8];
    unsigned int index = 0, padLen = 0;

    /* Save number of bits */
    encode(_count, bits, 8); //将数据的位数编码为小端存储格式bits

    /* Pad out to 56 mod 64. */
    index = (_count[0] >> 3) & 0x3F;
    padLen = (index < 56) ? (56 - index) : (120 - index);
    update(_PADDING, padLen);

    /* Append length (before padding) */
    update(bits, 8);
    /* Store state in digest */
    encode(_state, _digest, 16);

    // still have some codes
    /* Zeroize sensitive information. */
    // MD5_memset ( (POINTER)context, 0, sizeof(*context) );
}


/* Encodes input into output (unsigned char). 输出为小端存储，
即低位在内存的低地址
Assumes len is a multiple of 4.
[A0 A1 A2 A3] [B0 B1 B2 B3] [C0 C1 C2 C3] [D0 D1 D2 D3]
---->>> [A3 A2 A1 A0] [ B3 B2 B1 B0] [C3 C2 C1 C0] [D3 D2 D1 D0]
*/
void MD5::encode(const ulong* input, byte* output, unsigned int len)
{
    unsigned int i = 0, j = 0;
    while(j < len){
        output[j] = (byte)(input[i] & 0xFF);
        output[j+1] = (byte)((input[i] >> 8) & 0xFF);
        output[j+2] = (byte)((input[i] >> 16) & 0xFF);
        output[j+3] = (byte)((input[i] >> 24) & 0xFF);
        j += 4;
        ++i;
    }
}

/* Decodes input (unsigned char) into output (unsigned long).
Assumes len is a multiple of 4.
*/
void MD5::decode(const byte* input, ulong* output, unsigned int len)
{
    unsigned int i = 0, j = 0;
    while( j < len){
        output[i] = ( (ulong)input[j]
                      | (ulong)(input[j+1] << 8)
                      | (ulong)(input[j+2] << 16)
                      | (ulong)(input[j+3] << 24)
                     );
        ++i;
        j += 4;
    }
}

/* MD5 basic transormation.
Transforms state based on block.
*/
/* Constants for MD5::transform routine */
#define S11 7
#define S12 12
#define S13 17
#define S14 22
#define S21 5
#define S22 9
#define S23 14
#define S24 20
#define S31 4
#define S32 11
#define S33 16
#define S34 23
#define S41 6
#define S42 10
#define S43 15
#define S44 21


/* F, G, H and I are basic MD5 functions. */
#define F(x, y, z) ( ((x) & (y)) | ((~x) & (z)) )
#define G(x, y, z) ( ((x) & (z)) | ((y) & (~z)) )
#define H(x, y, z) ( (x) ^ (y) ^ (z) )
#define I(x, y, z) ( (y) ^ ((x) | (~z)) )


/* ROTATE_LEFT rotates x left n bits. */
#define ROTATE_LEFT(x, n) ( ((x) << (n)) | ((x) >> (32 - (n))) )

/* FF, GG, HH and II transformations for rounds 1, 2, 3, and 4.
Rotation is separate from addition to prevent re-computation.
*/
#define FF(a, b, c, d, x, s, ac) {\
    (a) += F((b), (c), (d)) + (x) + (ac);\
    (a) = ROTATE_LEFT((a),(s));\
    (a) += (b);\
}

#define GG(a, b, c, d, x, s, ac) {\
    (a) += G((b), (c), (d)) + (x) + (ac);\
    (a) = ROTATE_LEFT((a),(s));\
    (a) += (b);\
}

#define HH(a, b, c, d, x, s, ac) {\
    (a) += H((b), (c), (d)) + (x) + (ac);\
    (a) = ROTATE_LEFT((a),(s));\
    (a) += (b);\
}

#define II(a, b, c, d, x, s, ac) {\
    (a) += I((b), (c), (d)) + (x) + (ac);\
    (a) = ROTATE_LEFT((a),(s));\
    (a) += (b);\
}
void MD5::transform(const byte block[64])
{
    //声明4个中间变量，保存MD5缓冲器的值
    ulong a = _state[0], b = _state[1],
          c = _state[2], d = _state[3];

    ulong x[16];

    /* 将每一512字节细分成16个小组,存放在x[0..15]，
    x[i] 表示第 i+1 个分组, 每个小组32位（4个字节）.
    */
    decode(block, x, 64);

    //下面将开始四轮运算

    /* Round 1 */
        /* Let [abcd k s i] denote the operation
          a = b + ((a + F(b,c,d) + X[k] + T[i]) <<< s).

        其中：T[1 ... 64] is constructed from the sine function.
        Let T[i] denote the i-th element of the table, which is equal
        to the integer part of 4294967296 times abs(sin(i)), where i
        is in radians.
        */

        /* Do the following 16 operations.
        [ABCD  0  7  1]  [DABC  1 12  2]  [CDAB  2 17  3]  [BCDA  3 22  4]
        [ABCD  4  7  5]  [DABC  5 12  6]  [CDAB  6 17  7]  [BCDA  7 22  8]
        [ABCD  8  7  9]  [DABC  9 12 10]  [CDAB 10 17 11]  [BCDA 11 22 12]
        [ABCD 12  7 13]  [DABC 13 12 14]  [CDAB 14 17 15]  [BCDA 15 22 16]
        */
    FF (a, b, c, d, x[ 0], S11, 0xD76AA478); //1
    FF (d, a, b, c, x[ 1], S12, 0xE8C7B756); //2
    FF (c, d, a, b, x[ 2], S13, 0x242070DB); //3
    FF (b, c, d, a, x[ 3], S14, 0xC1BDCEEE); //4

    FF (a, b, c, d, x[ 4], S11, 0xF57C0FAF); //5
    FF (d, a, b, c, x[ 5], S12, 0x4787C62A); //6
    FF (c, d, a, b, x[ 6], S13, 0xA8304613); //7
    FF (b, c, d, a, x[ 7], S14, 0xFD469501); //8

    FF (a, b, c, d, x[ 8], S11, 0x698098D8); //9
    FF (d, a, b, c, x[ 9], S12, 0x8B44F7AF); //10
    FF (c, d, a, b, x[10], S13, 0xFFFF5BB1); //11
    FF (b, c, d, a, x[11], S14, 0x895CD7BE); //12

    FF (a, b, c, d, x[12], S11, 0x6B901122); //13
    FF (d, a, b, c, x[13], S12, 0xFD987193); //14
    FF (c, d, a, b, x[14], S13, 0xA679438E); //15
    FF (b, c, d, a, x[15], S14, 0x49b40821); //16


    /* Round 2. */
        /* Let [abcd k s i] denote the operation
          a = b + ((a + G(b,c,d) + X[k] + T[i]) <<< s).
         Do the following 16 operations.
        [ABCD  1  5 17]  [DABC  6  9 18]  [CDAB 11 14 19]  [BCDA  0 20 20]
        [ABCD  5  5 21]  [DABC 10  9 22]  [CDAB 15 14 23]  [BCDA  4 20 24]
        [ABCD  9  5 25]  [DABC 14  9 26]  [CDAB  3 14 27]  [BCDA  8 20 28]
        [ABCD 13  5 29]  [DABC  2  9 30]  [CDAB  7 14 31]  [BCDA 12 20 32]
        */
    GG (a, b, c, d, x[ 1], S21, 0xF61E2562); //17
    GG (d, a, b, c, x[ 6], S22, 0xC040B340); //18
    GG (c, d, a, b, x[11], S23, 0x265E5A51); //19
    GG (b, c, d, a, x[ 0], S24, 0xE9B6C7AA); //20

    GG (a, b, c, d, x[ 5], S21, 0xD62F105D); //21
    GG (d, a, b, c, x[10], S22,  0x2441453); //22
    GG (c, d, a, b, x[15], S23, 0xD8A1E681); //23
    GG (b, c, d, a, x[ 4], S24, 0xE7D3FBC8); //24

    GG (a, b, c, d, x[ 9], S21, 0x21E1CDE6); //25
    GG (d, a, b, c, x[14], S22, 0xC33707D6); //26
    GG (c, d, a, b, x[ 3], S23, 0xF4D50D87); //27
    GG (b, c, d, a, x[ 8], S24, 0x455A14ED); //28

    GG (a, b, c, d, x[13], S21, 0xA9E3E905); //29
    GG (d, a, b, c, x[ 2], S22, 0xFCEFA3F8); //30
    GG (c, d, a, b, x[ 7], S23, 0x676F02D9); //31
    GG (b, c, d, a, x[12], S24, 0x8D2A4C8A); //32

    /* Round 3. */
        /* Let [abcd k s t] denote the operation
          a = b + ((a + H(b,c,d) + X[k] + T[i]) <<< s). */
        /* Do the following 16 operations.
        [ABCD  5  4 33]  [DABC  8 11 34]  [CDAB 11 16 35]  [BCDA 14 23 36]
        [ABCD  1  4 37]  [DABC  4 11 38]  [CDAB  7 16 39]  [BCDA 10 23 40]
        [ABCD 13  4 41]  [DABC  0 11 42]  [CDAB  3 16 43]  [BCDA  6 23 44]
        [ABCD  9  4 45]  [DABC 12 11 46]  [CDAB 15 16 47]  [BCDA  2 23 48]
        */
    HH (a, b, c, d, x[ 5], S31, 0xFFFA3942); //33
    HH (d, a, b, c, x[ 8], S32, 0x8771F681); //34
    HH (c, d, a, b, x[11], S33, 0x6D9D6122); //35
    HH (b, c, d, a, x[14], S34, 0xFDE5380C); //36

    HH (a, b, c, d, x[ 1], S31, 0xA4BEEA44); //37
    HH (d, a, b, c, x[ 4], S32, 0x4BDECFA9); //38
    HH (c, d, a, b, x[ 7], S33, 0xF6BB4B60); //39
    HH (b, c, d, a, x[10], S34, 0xBEBFBC70); //40

    HH (a, b, c, d, x[13], S31, 0x289B7EC6); //41
    HH (d, a, b, c, x[ 0], S32, 0xEAA127FA); //42
    HH (c, d, a, b, x[ 3], S33, 0xD4EF3085); //43
    HH (b, c, d, a, x[ 6], S34,  0x4881D05); //44

    HH (a, b, c, d, x[ 9], S31, 0xD9D4D039); //45
    HH (d, a, b, c, x[12], S32, 0xE6DB99E5); //46
    HH (c, d, a, b, x[15], S33, 0x1FA27CF8); //47
    HH (b, c, d, a, x[ 2], S34, 0xC4AC5665); //48

    /* Round 4. */
        /* Let [abcd k s t] denote the operation
          a = b + ((a + I(b,c,d) + X[k] + T[i]) <<< s).
         Do the following 16 operations.
        [ABCD  0  6 49]  [DABC  7 10 50]  [CDAB 14 15 51]  [BCDA  5 21 52]
        [ABCD 12  6 53]  [DABC  3 10 54]  [CDAB 10 15 55]  [BCDA  1 21 56]
        [ABCD  8  6 57]  [DABC 15 10 58]  [CDAB  6 15 59]  [BCDA 13 21 60]
        [ABCD  4  6 61]  [DABC 11 10 62]  [CDAB  2 15 63]  [BCDA  9 21 64]
        */
    II (a, b, c, d, x[ 0], S41, 0xF4292244); //49
    II (d, a, b, c, x[ 7], S42, 0x432AFF97); //50
    II (c, d, a, b, x[14], S43, 0xAB9423A7); //51
    II (b, c, d, a, x[ 5], S44, 0xFC93A039); //52

    II (a, b, c, d, x[12], S41, 0x655B59C3); //53
    II (d, a, b, c, x[ 3], S42, 0x8F0CCC92); //54
    II (c, d, a, b, x[10], S43, 0xFFEFF47D); //55
    II (b, c, d, a, x[ 1], S44, 0x85845DD1); //56

    II (a, b, c, d, x[ 8], S41, 0x6fA87E4F); //57
    II (d, a, b, c, x[15], S42, 0xFE2CE6E0); //58
    II (c, d, a, b, x[ 6], S43, 0xA3014314); //59
    II (b, c, d, a, x[13], S44, 0x4E0811A1); //60

    II (a, b, c, d, x[ 4], S41, 0xF7537E82); //61
    II (d, a, b, c, x[11], S42, 0xBD3AF235); //62
    II (c, d, a, b, x[ 2], S43, 0x2AD7D2BB); //63
    II (b, c, d, a, x[ 9], S44, 0xEB86D391); //64

    //每轮循环后，将A, B, C, D分别加上a, b, c, d, 然后进入下一循环
    _state[0] += a;
    _state[1] += b;
    _state[2] += c;
    _state[3] += d;

}

string MD5::bytesToHexString(const byte* input, unsigned int len)
{
    string str;
    str.reserve(len << 1);
    for(unsigned int i = 0; i < len; i++){
        unsigned int a = (unsigned int)input[i] >> 4; // div 16
        unsigned int b = (unsigned int)input[i] & 0xF; // mod 16
        str.append(1, _HEX_SET[a]);
        str.append(1, _HEX_SET[b]);
    }
    return str;
}

string MD5::toString(){
    return bytesToHexString(_digest,16);
}

void MD5::toCString(unsigned char* md5_checksum)
{
    //unsigned char* md5_checksum = (unsigned char *)malloc(33);
    static char *hex = "0123456789ABCDEF";
    char md5_str[33] = {0};
    for (int i = 0, j = 0; i < 16; i++){
        md5_str[j++] = hex[((0xf0 & _digest[i]) >> 4)];
        md5_str[j++] = hex[0x0f & _digest[i]];
    }
  //  md5_str[j] = '\0';
    memcpy(md5_checksum, md5_str, 33);
}

void MD5::showMD5()
{
    //show md5 in hex mode
    for (int i = 0; i < 16; i++){
        fprintf(stdout, "%02x", _digest);
    }
    fprintf(stdout, "\n");
}

void MD5::message_digest_func(const char *str, const unsigned int len, unsigned char *md5val)
{
    MD5 md;
    md.reset();
    md.update( (const byte*)str, len);
    md.final();
    md.toCString(md5val);
}

void MD5::message_digest_func(fstream& file, unsigned char *md5val)
{
    MD5 md;
    md.reset();
    // update the context from a file
    char buf[_BUFFER_SIZE]; //需要实验确定用来容纳文件流的缓冲器的最佳大小
    unsigned int rsize = 0;
    while (!file.eof()){
        file.read(buf, _BUFFER_SIZE);
        rsize = file.gcount();
        if (rsize > 0)
            md.update((byte *)buf, rsize);
    }
    md.final();
    md.toCString(md5val);
}
//#define MD5_TEST
#ifdef MD5_TEST

#include <fstream>
#include <iostream>
#include <functional>
#include <exception>
#include <stdexcept>

#include <stack>
#include <list>
#include <vector>
#include <map>

using namespace std;


class Nocase_less { //不区分大小写的比较
public:
    bool operator() (const string&, const string&) const;
};

bool Nocase_less::operator()(const string& x, const string& y) const
    //若x按照字典序小于y，则返回true，不考虑大小写
{
    string::const_iterator cix = x.begin();
    string::const_iterator ciy = y.begin();
    while( cix != x.end() && ciy != y.end() && toupper(*cix) == toupper(*ciy)){
        ++cix;
        ++ciy;
    }

    if( cix == x.end() ) return ciy != y.end();
    if( ciy == y.end() ) return false;
    return toupper(*cix) < toupper(*ciy);
}

int main()
{
    cout << "-----------MD5 Algorithm Test----------" << endl;
    unsigned char md5val[33] = {0};
    MD5::message_digest_func("abc", 3, md5val);
    cout << "The md5 value of abc: " << md5val << endl;
    char src_file[] = "J:/lbfs.pdf";
    fstream file;
    file.open(src_file, ios::binary | ios::in);

    if (!file.is_open()){
        fprintf(stderr, "Error: open %s in MD5::main(...)\n", src_file);
        exit(errno);
    }
    file.seekg(0, ios::beg);
    file >> noskipws;
    MD5::message_digest_func(file, md5val);
    file.close();
    cout << "The md5 of file \"" << src_file << "\" : " << md5val << endl;
    string cstr("message digest");
    MD5 md5f(cstr);
    cout << cstr << ": " <<  md5f.toString() << endl;
    md5f.showMD5();
    cout << "Please input another string: ";
    cin >> cstr;
    cout << "The md5 of " << cstr << ": " << md5f(cstr) << endl;

    map<string, int, Nocase_less> ma;
    ma["first"] = 10;
    ma["word"] = 23;
    ma[md5f(cstr)] = 4;
    ma["FIRST"]++;
    ma["GONE"] = 7;
    ma.insert(make_pair(string("time"), 0320));
    map<string, int, Nocase_less> mb;
    mb.insert( make_pair(string("mapB"),1) );
    ma.swap(mb);
    map<string, int>::const_iterator mci;
    for(mci = ma.begin(); mci != ma.end(); mci++)
        cout << mci->first << ": " << mci->second << '\n';

    try{
        cout << "Reverse the input string: ";
        for(string::const_reverse_iterator crp = cstr.rbegin(); crp != cstr.rend(); ++crp)
            cout << *crp;
        cout << '\n';
        string file;
        cout << "please input file name: ";
        cin >> file;
        ifstream in(file.c_str(), ios::binary);
        MD5 mfile(in);
        cout << file << ": " << mfile.toString() << endl;
    }
    catch(MD5::_bad_ifstream){
        cout << "The file does not exist !" << endl;
    }
    catch(out_of_range){
        cout << "cstr out of range!" << endl;
    }

    return 0;
}

/*MD5 test suite:
MD5 ("") = d41d8cd98f00b204e9800998ecf8427e
MD5 ("a") = 0cc175b9c0f1b6a831c399e269772661
MD5 ("abc") = 900150983cd24fb0d6963f7d28e17f72
MD5 ("message digest") = f96b697d7cb7938d525a2f31aaf161d0
MD5 ("abcdefghijklmnopqrstuvwxyz") = c3fcd3d76192e4007dfb496cca67e13b
MD5 ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789") =
d174ab98d277d9f5a5611c2c9f419d9f
MD5 ("123456789012345678901234567890123456789012345678901234567890123456
78901234567890") = 57edf4a22be3c955ac49da2e2107b67a
*/

#endif // MD5_TEST
