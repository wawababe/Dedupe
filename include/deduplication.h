/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#ifndef DEDUPLICATION_H_INCLUDED
#define DEDUPLICATION_H_INCLUDED

#include <iostream>
#include <fstream>
#include <string>

#include <sys/stat.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <dirent.h>
#include <utime.h>
#include <errno.h>
#include <stdarg.h>
#include <time.h>

#include "hashfunc.h"
#include "RabinHash.h"
#include "checksum.h"
#include "MD5.h"

#include "BigHashTable.h"
#include "ListDB.h"
#include "utils.h"
#include "FileType.h"

using namespace std;


typedef unsigned int block_id_t;
#define BLOCK_ID_SIZE (sizeof(block_id_t))
#define BLOCK_ID_ALLOC_INC 20 //increment metadata's size by 20
#define BLOCK_SIZE 2048//4096 //4096  //4K bytes 定长分块、滑动窗口分块的默认长度，

/*CDC chunk 参数*/
#define BLOCK_MIN_SIZE  2048 //2048  //24576 //12288 //6144  //2048  //6144   //2048 //24576 //12288 //6144 //3072 //2560  //2048
#define BLOCK_MAX_SIZE  6144 //6144 //40960 //20480 //10240 //10240 //6144 //40960 //20480 //9216 //16384 //32768
//#define BLOCK_MAX_SIZE 32768//32K bytes
#define BLOCK_WIN_SIZE 30 //14 //48

#define BUF_MAX_SIZE 131072 //128K bytes
//#define BUF_MAX_SIZE 65536 //64KB

#ifndef PATH_MAX_LEN
#define PATH_MAX_LEN 255
#endif //PATH_MAX_LEN

#define DEDUP_MAGIC_NUM 0x160427
typedef struct _dedup_package_header{
    unsigned int magic_nr; //magic number for package header
    unsigned int files_nr;  //该存储系统里面包含的文件总数
    unsigned int ublocks_nr; //the number of unique blocks
    unsigned int fsp_block_sz;
    unsigned int sb_block_sz;
    unsigned int blockid_sz;
    unsigned long long ublocks_len;

    unsigned long long ldata_offset; // the offset of logic blocks
    unsigned long long mdata_offset; // the offset of file metadata
} D_Package_Header;
#define D_PKG_HDR_SZ (sizeof(D_Package_Header))

typedef struct _dedup_logic_block_entry{
    unsigned long long ublock_off; //the offset of the unique block in the deduped package
    unsigned int ublock_len;
    unsigned char block_md5[33];
} D_Logic_Block_Entry;
#define D_LOGIC_BLOCK_ENTRY_SZ (sizeof(D_Logic_Block_Entry))

//the header of the file metadata
typedef struct _dedup_file_entry{
    unsigned int fentry_sz; // the file entry size == D_FILE_ENTRY_SZ + fname_len + fblocks_nr *BLOCK_ID_SZ + last_block_sz;
    unsigned int fname_len;  // the length of the file name
    unsigned int fblocks_nr; // the number of file blocks
    unsigned int last_block_sz;
    unsigned long long org_file_sz; //未被重复删除前的文件大小
    int mode;
    time_t atime; //last access time
    time_t mtime; // last modified time
} D_File_Entry;
#define D_FILE_ENTRY_SZ (sizeof(D_File_Entry))

//deduplication operations
enum DEDUP_OPERATIONS{
    DEDUP_CREAT = 0,
    DEDUP_EXTRACT,
    DEDUP_APPEND,
    DEDUP_REMOVE,
    DEDUP_LIST,
    DEDUP_STAT
};

//deduplication chunking algorithms
#define CHUNK_FSP_NAME "FSP" //fixed sized partition
#define CHUNCK_CDC_NAME "CDC" //content-defined chunking
#define CHUNCK_SB_NAME "SB"   //sliding block chunking
#define CHUNCK_AAC_NAME "AAC"  //application aware chunking
enum D_CHUNK_ALG{
    D_CHUNK_FSP = 0,
    D_CHUNK_CDC,
    D_CHUNK_SB,
    D_CHUNK_AAC
};
//#define CHUNK_CDC_D 4096  //cdc divisor
//#define CHUNK_CDC_R 13   //CDC 的分界窗口的哈希值 (hashvalue(chunk win_buf) % CHUNK_CDC_D)


typedef struct _cdc_chunk_hashfun {
    char hashfunc_name[16];
    unsigned int (*hashfunc)(const char *str);
} D_CDC_hashfun;

/*CDC chunking hash functions */
#define D_ROLLING_HASH "AdlerHash"
static const D_CDC_hashfun CDC_HASHFUN[] =
{
    {"APHash", HashFunctions::APHash},
    {"BKDRHash", HashFunctions::BKDRHash},
    {"BPHash", HashFunctions::BPHash},
    {"DJBHash", HashFunctions::DJBHash},
    {"DJB2Hash", HashFunctions::DJB2Hash},
    {"DEKHash", HashFunctions::DEKHash},

    {"ELFHash", HashFunctions::ELFHash},
    {"FNVHash", HashFunctions::FNVHash},
    {"JSHash", HashFunctions::JSHash},
    {"PJWHash", HashFunctions::PJWHash},
    {"RSHash", HashFunctions::RSHash},
    {"SDBMHash", HashFunctions::SDBMHash},

    {"CRCHash", HashFunctions::CRCHash},
    {"RabinHash", RabinHashFunc}
};

//magic file name for temporary files
#define MAGIC_TMP_FILE_NAME "DCBA123TMP"


class Dedupe{

public:
    Dedupe(bool vbose = false);
    ~Dedupe();

    int set_chunk_alg(const char *cname);
    int set_cdc_hashfun(const char *hashfunc_name);
    int create_package(const char *pkg_name);

    int insert_files(const char *pkg_name, int files_nr, char **src_files);
    int remove_files(const char *pkg_name, int files_nr, char **files_remove);
    int extract_all_files(const char *pkg_name, int files_nr, char **files_extract, char *dest_dir);

    int show_pkg_header(const char *pkg_name);
    int show_package_files(const char *pkg_name);
    int package_stat(const char *pkg_name);

private:
    void clean_tmpfiles();
    int blocks_cmp(char *buf, unsigned int len,
              fstream &ldata_file, fstream &bdata_file, unsigned int block_id);
    int register_block(char *block_buf, unsigned int block_len, unsigned char *md5val,
                       fstream &ldata_file, fstream &bdata_file,
                       unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata);

    int chunk_fsp(ifstream& src_file, fstream &ldata_file, fstream &bdata_file,
                unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata,
                unsigned int &last_block_len, char *last_block);
    int chunk_cdc(ifstream& src_file, fstream &ldata_file, fstream &bdata_file,
                unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata,
                unsigned int &last_block_len, char *last_block);
    int chunk_sb(ifstream& src_file, fstream &ldata_file, fstream &bdata_file,
                unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata,
                unsigned int &last_block_len, char *last_block);
    int chunk_aac(ifstream& src_file, fstream &ldata_file, fstream &bdata_file,
                unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata,
                unsigned int &last_block_len, char *last_block);

    int register_file(char *fullpath, int prepos, fstream &ldata_file, fstream &bdata_file, fstream &mdata_file);
    int register_dir(char *fullpath, int prepos, fstream &ldata_file, fstream &bdata_file, fstream &mdata_file);

    int prepare_insert(ifstream &pkg_file, fstream &ldata_file, fstream &bdata_file, fstream &mdata_file);

    int extract_file(ifstream &pkg_file, D_File_Entry fentry, char *dest_dir);

private:

    D_Package_Header d_pkg_hdr;
    BigHashTable *d_htab_pathname; //hashtable for path names
    BigHashTable *d_htab_sb_csum; //hashtable for SB file chunking
    BigHashTable *d_htab_bindex; // hashtable for chunking blocks index

    enum D_CHUNK_ALG d_chunk_alg; //chunking algorithms
    /*CDC chunking Hash Function*/
    unsigned int (*d_cdc_hashfun)(const char *str);
    unsigned int d_cdc_hash_mod;
    unsigned int d_cdc_chunk_mark;

    /*SB chunking parameter*/
    unsigned int d_sb_block_sz; //specify the size of the sliding block
    unsigned int d_fsp_block_sz;

    bool d_rolling_hash; // default as adler32_rolling
    char d_pkg_name[PATH_MAX_LEN];
    char d_ldata_name[PATH_MAX_LEN];
    char d_bdata_name[PATH_MAX_LEN];
    char d_mdata_name[PATH_MAX_LEN];

    bool verbose;
};


#endif // DEDUPLICATION_H_INCLUDED

/* The Structure of the De-duplication Storage System
 _____________________________________________________
|                                                     |
|   de-duplicated files' package header               |
|   ------------------------------------------------  |
|   unique blocks (physical data)                     |
|   ------------------------------------------------  |
|   logic blocks  (index and length of                |
|               the unique blocks)                    |
|   ------------------------------------------------  |
|     file 1 metadata:                                |
|       （重复删除后的文件对应的存储信息）            |
|       1.the header of file 2                        |
|       2.file 2原先的文件名（包含存储路径）          |
|       3.entry data:file 2的各个块的块号             |
|     --------------------------------------          |
|     file 2 metadata: 具体如下                       |
|     ---------------------------------------         |
|             .                                       |
|             .                                       |
|             .                                       |
|     ----------------------------------------        |
|     file n metadata                                 |
|_____________________________________________________|
*/

