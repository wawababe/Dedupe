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
