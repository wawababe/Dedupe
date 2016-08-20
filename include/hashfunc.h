/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#ifndef HASHFUNCION_H
#define HASHFUNCION_H

#include <string>
namespace HashFunctions
{
    typedef unsigned int (*HashFuncPT) (const char*);

    unsigned int APHash  (const char*);
    unsigned int BKDRHash(const char*);
    unsigned int BPHash  (const char*);

    unsigned int DJBHash (const char*);
    unsigned int DJB2Hash(const char*);//
    unsigned int DEKHash (const char*);

    unsigned int ELFHash (const char*);
    unsigned int FNVHash (const char*);
    unsigned int JSHash  (const char*);

    unsigned int PJWHash (const char*);
    unsigned int RSHash  (const char*);
    unsigned int SDBMHash(const char*);

    unsigned int CRCHash (const char*);//
}

#endif // HASHFUNC_H
