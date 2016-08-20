/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#ifndef CHECKSUM_H_INCLUDED
#define CHECKSUM_H_INCLUDED

#include <cstddef>

/** Adler 32bit checksum algorithm **/
#define MOD_ADLER 65521;
unsigned int adler32(const char*, int);

/** a simple checksum function, inspired by adler-32bit checksum algorithm **/
#define CHAR_OFFSET 0
unsigned int adler32_rsync(const char *, int);

/** calculate adler32(Xi+1...Xj+1, j-i+1),
given adler32(Xi...Xj, j-i+1), Xi, Xj+1.
**/
unsigned int adler32_rolling(unsigned int cksum, int len, char c1, char c2);


#endif // CHECKSUM_H_INCLUDED
