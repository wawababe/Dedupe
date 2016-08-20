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
