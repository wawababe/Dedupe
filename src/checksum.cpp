/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "checksum.h"

/*
Mark Adler's Adler-32 checksum -- 32 bit checksum
a = 1 + d1 + d2 + ... + dn (mod 65521)
b = (1+d1) + (1+d1+d2) + ... + (1+d1+d2+...+dn) (mod 65521)
*/
#define MOD_ADLER 65521;
unsigned int adler32(const char *buf, int len)
{
    unsigned int a = 1, b = 0;

    for (int i = 0; i < len; ++i){
        a = ( a + buf[i]) % MOD_ADLER;
        b = ( b + a) % MOD_ADLER;
    }

    return (b << 16) | (a & 0xffff);
}


/*
a simple 32 bit checksum,
inspired by Mark Adler's Adler-32 checksum.
*/
unsigned int adler32_rsync(const char *buf, int len)
{
    int i;
    unsigned int s1, s2;

    s1 = s2 = 0;
    for (i = 0; i < (len - 4); i += 4) {
        s2 += 4 * (s1 + buf[i]) + 3 * buf[i+1] + 2 * buf[i+2] + buf[i+3] +
          10 * CHAR_OFFSET;
        s1 += (buf[i+0] + buf[i+1] + buf[i+2] + buf[i+3] + 4 * CHAR_OFFSET);
    }
    for (; i < len; i++) {
        s1 += (buf[i]+CHAR_OFFSET);
		s2 += s1;
    }

    return (s1 & 0xffff) + (s2 << 16);
}

/*
 已知： cksum == adler32(X0, ..., Xn),c1 == X0, c2 == Xn+1
 则 adler32(X1, ..., Xn+1) = adler32_rollying(cksum, n+1, X0, Xn+1);
 原理：
 Adler32 的定义：
    a(k, t) = sum(Xk, Xk+1, ..., Xt) mod M;
    b(k, t) = sum( (t-i+1)Xi ) mod M;
    adler(k, t) = a(k, t) + b(k, t) << 16;
所以，
    a(k+1, t+1) = ( a(k, t) - Xk + Xt+1 ) mod M;
    b(k+1, t+1) = ( b(k, t) - (t-k+1)*Xk + a(k+1, t+1) ) mod M;
so,
    adler(k+1, t+1) = a(k+1, t+1) + b(k+1, t+1) << 16;
*/
unsigned int adler32_rolling(unsigned int cksum, int len, char c1, char c2)
{
    unsigned int a = 0, b = 0;
    a = cksum & 0xffff;
    b = cksum >> 16;
    a -= (c1 - c2);
    b -= (len * c1 - a);
    return (b << 16) | (a & 0xffff);
}


/***********************************************
checksum test procedure
To test the checksum, please add the following
code in this page or makefile:
#define CHECKSUM_TEST
***********************************************/

#ifdef CHECKSUM_TEST

#include <iostream>
#include <string>
#include "checksum.h"
#include <ctime>
using namespace std;

int main()
{
    string s1(5000,0);
    cout << "please input string:" << endl;
    cin >> s1;

    time_t start, finish;
    time(&start);
    unsigned int t = adler32(s1.c_str(), s1.length());
    time(&finish);
    cout << "Time of original Adler 32bit checksum algorithm: "
         << double(finish - start)*1000 << "sec(s), Adler32 checksum: " << t << endl;

    time(&start);
    unsigned int t2 = adler32_rsync(s1.c_str(), s1.length());
    time(&finish);

    cout << "Time of modified Adler32 in Rsync: "
         << finish - start << "sec(s), Adler32_rsync checksum: " << t2 << endl;

    return 0;
}
#endif // CHECKSUM_TEST

