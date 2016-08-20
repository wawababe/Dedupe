#include "HashDB.h"

/** HashDB的结构
Hshdb_header:
    magic
    cnum
    bnum
    tnum
    hoff
    voff
Hash Buckets:
    bucket[i]
Hash Entries:
    ith hash_entry
    ith key
    ith value
**/

HashDB::HashDB( uint64_t tnum, uint32_t bnum,  uint32_t cnum,
               hashfunc_t hf1, hashfunc_t hf2)
{

    header.tnum = tnum;
    header.bnum = bnum;
    header.cnum = cnum;
    hfunc1 = hf1;
    hfunc2 = hf2;

    header.magic = HASHDB_MAGIC;
    header.hbucket_off = HASHDB_HDR_SZ;
    header.hentry_off = HASHDB_HDR_SZ + header.bnum * HASH_BUCKET_SZ;

    memset(dbpath, 0, PATH_MAX_LEN);
    memset(bfpath, 0, PATH_MAX_LEN);
    bloom = 0;
    bucket = 0;
    cache = 0;
}


HashDB::~HashDB()
{
    if (bloom){
        delete bloom;
        bloom = 0;
    }
    if (bucket){
        free(bucket);
        bucket = 0;
    }
    if (cache){
        free(cache);
        cache = 0;
    }
    unlinkDB();
}


int HashDB::openDB(const char *dbname, const char *bfname, bool isnewdb)
//打开已经存在的hashdb，初始化内存中的hashdb
//或者创建初始化一个新的hashdb(此时需要写到磁盘文件中)，
{
    if (!dbname || !bfname)
        return -1;
    sprintf(dbpath, "%s", dbname);
    sprintf(bfpath, "%s", bfname);
    fstream db_file;
    int rwsize = 0, ret = 0;
    if (isnewdb){ //new hashdb
        //根据要存储的数目header.tnum来确定bloomfilter的最优参数，
        //并根据该参数新建一个bloom
        BloomParameters pmt;
        pmt.projected_element_count = header.tnum; // expect to insert 1000 elements into the filter
        pmt.fpp = 0.0001; //maximum tolerable false positive probability
        pmt.randseed = 0xA5A5A5A5;
        if(!pmt)
        {
            cout << "Error: Invalid set of bloom filter parameters!" << endl;
            return -1;
        }
        pmt.computeOptPara();
        bloom = new BloomFilter(pmt);

        ofstream obf_file;
        db_file.open(dbpath, ios::binary | ios::out);
        obf_file.open(bfpath, ios::binary);
        if (!obf_file.is_open()){
            cout << "Error: open bloom filter file in HashDB::openDB(...)" << endl;
            return -1;
        }
        writebf(obf_file, bloom);
        obf_file.close();

    }else{ //existed hashdb
        ifstream bf_file_i;
        db_file.open(dbpath, ios::binary | ios::in);
        bf_file_i.open(bfpath, ios::binary);
        db_file.seekg(0, ios::beg);
        bf_file_i.seekg(0, ios::beg);
        bf_file_i >> noskipws;
        db_file >> noskipws;

        bloom = new BloomFilter;
        readbf(bf_file_i, bloom);
        bf_file_i.close();

        db_file.read((char *)(&header), HASHDB_HDR_SZ);
        rwsize = db_file.gcount();
        if (rwsize != HASHDB_HDR_SZ){
            cout << "Error: read header in HashDB::openDB(...)" << endl;
            ret = -1;
            goto _OPENDB_EXIT;
        }
        if (header.magic != HASHDB_MAGIC)
        {
            cout << "Error: wrong hashdb magic number in HashDB::openDB(...)" << endl;
            ret = -1;
            goto _OPENDB_EXIT;
        }
    }

    if(! (bucket = (HASH_BUCKET *)malloc(header.bnum * HASH_BUCKET_SZ)) ){
       ret = -1;
       cout << "Error: malloc hash buckets in HashDB::openDB(...)" << endl;
       goto _OPENDB_EXIT;
    }
    for (uint64_t i = 0; i < header.bnum; i++)
        bucket[i].off = 0;

    if(! (cache = (HASH_ENTRY *)malloc(header.cnum * HASH_ENTRY_SZ)) ){
        ret = -1;
        cout << "Error: malloc cache in HashDB::openDB(...)" << endl;
        goto _OPENDB_EXIT;
    }
    for(uint64_t i = 0; i < header.cnum; i++){
        cache[i].iscached = false;
        cache[i].off = 0;
        cache[i].left = 0;
        cache[i].right = 0;
        cache[i].key = 0;
        cache[i].value = 0;
        cache[i].ksize = 0;
        cache[i].vsize = 0;
        cache[i].shash = 0;
        cache[i].tsize = 0;
    }

    if (isnewdb){//for non-existed hashdb, 将新建的hashdb写到磁盘文件中
        db_file.write((const char *)(&header), HASHDB_HDR_SZ);
        db_file.write((const char *)(bucket), header.bnum * HASH_BUCKET_SZ);
    }else { //for existed hashdb, read data from it to fill up cache
        db_file.read( (char *)bucket, header.bnum * HASH_BUCKET_SZ);
        rwsize = db_file.gcount();
        if (rwsize != header.bnum * HASH_BUCKET_SZ){
            cout << "Error: read hash buckets in HashDB::openDB(...)" << endl;
            ret = -1;
            goto _OPENDB_EXIT;
        }

        //read hash_entries from HashDB file db_file to fill up HashDB::cache
        if (-1 == read2fillcache(db_file)) {
            cout << "Error: read Hash Entries in HashDB::openDB(...)" << endl;
            ret = -1;
            goto _OPENDB_EXIT;
        }
        db_file.seekg(0, ios::end);
    }
    db_file.close();
    return 0;

_OPENDB_EXIT:
    if (isnewdb){ //对于异常新创建的文件
        if (db_file.is_open())
            db_file.close();
        unlink(dbpath);
        unlink(bfpath);
    }else{
        if (db_file.is_open())
            db_file.close();
    }
    if (bloom){
        delete bloom;
        bloom = 0;
    }
    if (bucket){
        free(bucket);
        bucket = 0;
    }
    if (cache){
        free(cache);
        cache = 0;
    }
    return ret;
}


int HashDB::read2fillcache(fstream &db_file)
/** for循环里面的read不安全
**/
{
    uint32_t maxnum, pos;
    char key[HASHDB_KEY_MAX_SZ] = {0};
    char value[HASHDB_VALUE_MAX_SZ] = {0};
    HASH_ENTRY hentry;
    memset(&hentry, 0, HASH_ENTRY_SZ);
    int rsize = 0;

    if(!bloom || !bucket || !cache)
        return -1;

  //  maxnum = (header.cnum < header.bnum ) ? header.cnum : header.bnum;
    for (uint32_t i = 0; i < header.bnum; i++){
        if (bucket[i].off == 0)
            continue;

        memset(key, 0, HASHDB_KEY_MAX_SZ);
        memset(value, 0, HASHDB_VALUE_MAX_SZ);

        db_file.seekg(bucket[i].off, ios::beg);

        db_file.read((char *)(&hentry), HASH_ENTRY_SZ);
        rsize = db_file.gcount();
        if (rsize != HASH_ENTRY_SZ ){
            cout << "Error: read hash entry in HashDB::openDB()::read2fillcache(...)" << endl;
            return -1;
        }

        db_file.read((char *)key, HASHDB_KEY_MAX_SZ);
        rsize = db_file.gcount();
        if (rsize != HASHDB_KEY_MAX_SZ){
            cout << "Error: read hash_entry key in HashDB::openDB()::read2fillcache(...)" << endl;
            return -1;
        }

        db_file.read((char *)value, HASHDB_VALUE_MAX_SZ);
        rsize = db_file.gcount();
        if (rsize != HASHDB_VALUE_MAX_SZ){
            cout << "Error: read hash_entry value in HashDB::openDB()::read2fillcache(...)" << endl;
            return -1;
        }
        pos = hfunc1(key) % header.cnum;
        if (cache[pos].iscached)
            continue;
        memcpy(&cache[pos], &hentry, HASH_ENTRY_SZ);
        cache[pos].key = strdup(key);

        ///!
        if (0 == (cache[pos].value = malloc(hentry.vsize) ) ){
            cout << "Error: malloc cache value in HashDB::openDB()::read2fillcache(...)" << endl;
            return -1;
        }
        memset(cache[pos].value, 0, hentry.vsize);
        memcpy(cache[pos].value, value, hentry.vsize);
        cache[pos].iscached = true;
    }
    return 0;
}


int HashDB::closeDB(int flash)
//先将hashdb db中缓存的hash_entry全部写到db->fd指向的文件中，
//然后依次写入db的bloomfilter/bucket
//默认flash = 1, 需要将cached hash_entries写入磁盘
{
    if ( !bloom || !bucket || !cache)
        return -1;

    uint32_t hash1, hash2;
    ssize_t wsize = 0;
    int ret = 0;
    ofstream db_file, bf_file;

    if (flash <= 0) //不需要将缓冲区的hashdb写入磁盘文件，直接关掉hashdb
        goto _CLOSE_EXIT;

    //flash the cached hash_entries into the computer disk file
    //flush cached data to file
    for (uint64_t i = 0; i < header.cnum; i++){
        if (! cache[i].iscached)
            continue;
        hash1 = hfunc1(cache[i].key);
        hash2 = cache[i].shash;
        if (-1 == swapout(hash1, hash2, &cache[i])){
            ret = -1;
            goto _CLOSE_EXIT;
        }
    }

    db_file.open(dbpath, ios::binary | ios::in);
    bf_file.open(bfpath, ios::binary);
    bf_file.seekp(0, ios::beg);
    writebf(bf_file, bloom);

    db_file.write((const char*)(&header), HASHDB_HDR_SZ);
    db_file.write((const char*)bucket, HASH_BUCKET_SZ * header.bnum);

    db_file.close();
    bf_file.close();
    return 0;

_CLOSE_EXIT:
    if (db_file.is_open()){
        db_file.close();
    }
    if (bf_file.is_open()){
        bf_file.close();
    }
    if (bloom){
        delete bloom;
        bloom = 0;
    }
    if (bucket){
        free(bucket);
        bucket = 0;
    }
    if (cache) {
        for (uint64_t i = 0; i < header.cnum; i++){
            if (cache[i].key)
                free(cache[i].key);
            if (cache[i].value)
                free(cache[i].value);
        }
        free (cache);
        cache = 0;
    }
    return ret;
}


int HashDB::setDB(const char* key, const void* value, const int vsize)
/** 1)从cache中查找是否存在含有关键字key的hash_entry,
若无，则进行2)， 若cache[pos]处已存有hash_entry old，则需写入磁盘先
    2)从hashdb对应的磁盘文件中载入含有关键字的hash_entry，
若无，则进行3)，
    3) 创建含有关键字key的新的hash_entry
In sum, 均要重新设置value值
**/
{
    if (!key || !value)
        return -1;

    int pos = 0;
    uint32_t hash1, hash2;
    uint32_t he_hash1, he_hash2;

    hash1 = hfunc1(key);
    hash2 = hfunc2(key);

    pos = hash1 % header.cnum;
    /*swap out the hash entry in cache[pos] into disk hashdb file first
    */
    if (cache[pos].iscached && ( (hash2 != cache[pos].shash) || (strcmp(key, cache[pos].key))!= 0 ) ){
        he_hash1 = hfunc1(cache[pos].key);
        he_hash2 = cache[pos].shash;
        if (-1 == swapout(he_hash1, he_hash2, &cache[pos])){
            cout<< "Error: swap out the hash entry in cache[pos] into disk in HashDB::setDB\n" << endl;
            return -1;
        }
    }

    /*swap the hash entry specified by key from disk hashdb file into cache[pos]
    */
    if (!cache[pos].iscached && (bloom->contains(key, strlen(key))) ){
        if ( -1 == swapin(key, hash1, hash2, &cache[pos]) )
            return -1;
    }
    if ( (strlen(key) > HASHDB_KEY_MAX_SZ) || (vsize > HASHDB_VALUE_MAX_SZ) ){
        cout << "Error: key's max length is 256, value max size is 128" << endl;
        return -1;
    }
    /* fill up cache hash entry */
    if (cache[pos].key){
        free(cache[pos].key);
        cache[pos].key = 0;
    }
    if (cache[pos].value){
        free(cache[pos].value);
        cache[pos].value = 0;
    }
    cache[pos].key = strdup(key);
    cache[pos].ksize = strlen(key);
    if (0 == (cache[pos].value = malloc(vsize) ) ){
        fprintf(stderr, "Error: malloc %d cache[%d].value in HashDB::setDB\n", vsize, pos);
        return -1;
    }
    memcpy(cache[pos].value, value, vsize);
    cache[pos].vsize = vsize;
    cache[pos].tsize = HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ + HASHDB_VALUE_MAX_SZ;
  //  cache[pos].tsize = HASH_ENTRY_SZ + cache[pos].ksize + cache[pos].vsize;
    cache[pos].shash = hash2;
    if (! cache[pos].iscached){
        //new hash entry, hashdb对应磁盘文件中还木有存在含有该关键字key的hash_entry
        cache[pos].off = 0;
        cache[pos].left = 0;
        cache[pos].right = 0;
        bloom->insert(key, cache[pos].ksize);
        cache[pos].iscached = true;
    }

    return 0;
}


int HashDB::getDB(const char* key, void* value, int &vsize)
//获取关键字为key的hash_entry的value值及其大小
//若成功，返回0；
//若不含有该关键字key的hash_entry，则返回-2；
//若其他异常失败，则返回-1;
{
    if (!key)
        return -1;

    int pos, ret;
    uint32_t hash1, hash2;
    uint32_t he_hash1, he_hash2;

    hash1 = hfunc1(key);
    hash2 = hfunc2(key);

    if (!bloom->contains(key, strlen(key))){
        return -2;
    }

    pos = hash1 % header.cnum;
    if (cache[pos].iscached && (hash2 != cache[pos].shash || 0 != strcmp(key, cache[pos].key))){
        he_hash1 = hfunc1(cache[pos].key);
        he_hash2 = cache[pos].shash;
        if (-1 == swapout(he_hash1, he_hash2, &cache[pos]) )
            return -1;
    }

    if (!cache[pos].iscached){
        if (0 != (ret = swapin(key, hash1, hash2, &cache[pos])) )
            return ret;
    }
    //value 必须是指向一段空间的实地址
    memcpy(value, cache[pos].value, cache[pos].vsize);
    vsize = cache[pos].vsize;

    return 0;
}


int HashDB::unlinkDB()
{
    if (dbpath)
        unlink(dbpath);
    if (bfpath)
        unlink(bfpath);
    return 0;
}


int HashDB::swapout(const uint32_t hash1, const uint32_t hash2,
                    HASH_ENTRY* he)
/**将hash_entry he 写入hashdb的磁盘文件中，
其中：
  第一个hash1确定第几个bucket，
  第二个hash2确定bucket[hash1%bnum]下he的parent(保持二叉树的结构),
  以便将he及它的key、value写入到hashdb的末尾
**/
{
    if (!he || !he->iscached)
        return 0;

    char key[HASHDB_KEY_MAX_SZ] = {0};
    char value[HASHDB_VALUE_MAX_SZ] = {0};
    uint64_t root;
    uint32_t pos;
    int cmp, lr = 0;

    int hebuf_sz = HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ + HASHDB_VALUE_MAX_SZ;
    void *hebuf = 0; // point to (hash_entry + key + value)
    char *hkey = 0;
    void *hvalue = 0;
    HASH_ENTRY* hentry;

    HASH_ENTRY parent;
    ssize_t rwsize = 0;

    fstream db_file;
    db_file.open(dbpath, ios::binary | ios::in | ios::out );
    db_file >> noskipws;

    if (he->off == 0){
        //he is a new hash_entry, 需写入磁盘文件
        hebuf_sz  = HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ + HASHDB_VALUE_MAX_SZ;
        if (0 == (hebuf = (void*)malloc(hebuf_sz)) ){
            cout << "Error: malloc buffer for (hash entry, key, value) in HashDB::swapout(...)" << endl;
            db_file.close();
            return -1;
        }
        //从根节点开始找到新hash_entry he的插入位置
        pos = hash1 % header.bnum;
        root = bucket[pos].off;
        parent.off = 0;
        while (root){
            //读出root所指向的(hashentry, key, value)值到hebuf中
            db_file.seekg(root, ios::beg);
            //memset(hebuf, 0, hebuf_sz);
           /* memset(&hentry, 0, HASH_ENTRY_SZ);
            memset(hkey, 0, 256);
            memset(hvalue, 0, 128);
            */
            db_file.read((char *)hebuf, hebuf_sz);
            rwsize = db_file.gcount();
            if (hebuf_sz != rwsize){
                cout << "Error: read hash entry, key, value in HashDB::swapout(...)" << endl;
                db_file.close();
                return -1;
            }

            hentry = (HASH_ENTRY *)hebuf;
            hkey = (char *)hebuf + HASH_ENTRY_SZ;
            hvalue = (void *)((char *)hebuf + HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ);
            memcpy(&parent, hentry, HASH_ENTRY_SZ);

            //二叉搜索待插入节点hash_entry he的插入位置
            if ( hash2 < hentry->shash ){
                root = hentry->left;
                lr = 0;
            }else if (hash2 > hentry->shash){
                root = hentry->right;
                lr = 1;
            }else {//哈希冲突下，则比较字符
                cmp = strcmp(he->key, hkey);
                if (cmp < 0){
                    root = hentry->left;
                    lr = 0;
                }else if (cmp < 0){
                    root = hentry->right;
                    lr = 1;
                }
                else    //will never happen
                {
                }
            }// if... else if... else
        }//while(root)


        if (hebuf){
            free(hebuf);
            hebuf = 0;
        }

        /*找到新的hash_entry he 的插入位置（文件末尾）, 修改父节点*/
        db_file.seekp(0, ios::end);
        he->off = db_file.tellp();
        if (!bucket[pos].off){
            bucket[pos].off = he->off;
            db_file.seekp(HASHDB_HDR_SZ + pos * HASH_BUCKET_SZ, ios::beg);//还需将bucket[pos]写入bucket[pos] in disk hasdb file
            db_file.write((const char *)(&bucket[pos]), HASH_BUCKET_SZ);
        }
        if (parent.off) { //若有父节点，则需修改父节点
            (lr == 0) ? (parent.left = he->off) : (parent.right = he->off);
            db_file.seekp(parent.off, ios::beg);
            db_file.write((const char *)(&parent), HASH_ENTRY_SZ);
        }
    } //if (he->off == 0) : new hash entry

    /*flush hash_entry he from memory to disk file */
    db_file.seekp(he->off, ios::beg);
    db_file.write((const char *)(he), HASH_ENTRY_SZ);
    sprintf(key, "%s", he->key);
    db_file.write((const char *)key, HASHDB_KEY_MAX_SZ);///!20160502改
    memcpy(value, he->value, he->vsize);
    db_file.write((const char *)value, HASHDB_VALUE_MAX_SZ); ///!20160502改

    db_file.close();

    if (he->key)
    {
        free(he->key);
        he->key = 0;
    }
    if (he->value)
    {
        free(he->value);
        he->value = 0;
    }
    he->off = 0;
    he->left = 0;
    he->right = 0;
    he->ksize = 0;
    he->vsize = 0;
    he->tsize = 0;
    he->shash = 0;
    he->iscached = false;

    return 0;
}


int HashDB::swapin(const char* key, uint32_t hash1, uint32_t hash2, HASH_ENTRY* he)
//查找以bucket[hash1%header.bnum]为根节点的
//二叉树中具有hash2或者key的hash_entry，
//返回到he中，其中置he->iscached = true
// 0 : succeed
// -1 : unexpected error
// -2 : the hash entry specified by key does not exist in the disk hashdb file
{
    if (!key || he->iscached)
        return -1;
    uint32_t pos;
    uint64_t root;
    int cmp, hebuf_sz;
    void *hebuf = 0;
    char *hkey = 0;
    void *hvalue = 0;
    HASH_ENTRY *hentry = 0;
    ssize_t rsize = 0;

    hebuf_sz = HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ + HASHDB_VALUE_MAX_SZ;
    if (0 == (hebuf = (void *)malloc(hebuf_sz) ) )
        return -1;
    pos = hash1 % header.bnum;
    root = bucket[pos].off;
    ifstream db_file;
    db_file.open(dbpath, ios::binary);
    db_file >> noskipws;
    while(root) {
        db_file.seekg(root, ios::beg);
        memset(hebuf, 0, hebuf_sz);
        db_file.read((char *)hebuf, hebuf_sz);
        rsize = db_file.gcount();
        if (rsize != hebuf_sz){
            free(hebuf);
            hebuf = 0;
            db_file.close();
            cout << "Error: read hash entry root in HashDB::swapin(..)" << endl;
            return -1;
        }

        hentry = (HASH_ENTRY *)hebuf;
        hkey = (char *)hebuf + HASH_ENTRY_SZ;
        hvalue = (void *)((char *)hebuf + HASH_ENTRY_SZ + HASHDB_KEY_MAX_SZ);
        if (hash2 < hentry->shash) root = hentry->left;
        else if (hash2 > hentry->shash) root = hentry->right;
        else {
            cmp = strcmp(key, hkey);
            if (cmp == 0){ //find the entry
                memcpy(he, hebuf, HASH_ENTRY_SZ);
                he->key = strdup(hkey);
                if (0 == (he->value = malloc(he->vsize) ) ){
                    db_file.close();
                    return -1;
                }
                memcpy(he->value, hvalue, he->vsize);
                he->iscached = true;
                free(hebuf);
                hebuf = 0;
                db_file.close();
                return 0;
            }else if (cmp < 0) root = hentry->left;
            else root = hentry->right;
        }
    }

    if (hebuf){
        free(hebuf);
        hebuf = 0;
    }
    db_file.close();
    return -2;
}

//#define HASHDB_TEST
#ifdef HASHDB_TEST

#include <iostream>
#include <string>
#include "hashfunc.h"
#include "HashDB.h"

using namespace std;
int main()
{

    HashDB db(HASHDB_DEFAULT_TNUM,HASHDB_DEFAULT_BNUM, HASHDB_DEFAULT_CNUM, HashFunctions::APHash, HashFunctions::JSHash);
    char db_name[256] = {0};
    char bf_name[256] = {0};
    sprintf(db_name, "data/.hashdb_7936.db", getpid());
    sprintf(bf_name, "data/.hashdb_7936.bf", getpid());
    db.openDB(db_name, bf_name, false);
    string keys[] = {"fill", "suit", "Heal", "Orange", "range", "circle", "right",
                     "red", "Contemporary", "OSDF", "loadandrun","campus",
                     "skin food", "suit", "suit", "OH, my god"};
    string values[] = {"1st", "2nd", "3rd", "4th", "5th", "circle value", "right value",
                       "6th", "7th", "8TH......", "9sdfthhhh", "10th......",
                     "11th********", "12f", "so", "Hi, my god"};
    for (int i = 0; i < 16; i ++)
        db.setDB(keys[i].c_str(), values[i].c_str(), values[i].size());
    db.setDB(keys[6].c_str(), values[6].c_str(), values[6].size());

    const char key[] = "range";
    void *value = malloc(HASHDB_VALUE_MAX_SZ);
    memset(value, 0, HASHDB_VALUE_MAX_SZ);
    int vsize = 0;
    int ret = db.getDB(key, value, vsize);
    if ( 0 == ret )
        cout << "key: " << key << ", value: " << (const char*)value << endl;
    else if (-2 == ret)
        cout << db.getdbpath() << " does not contain key-" << key << endl;
    else
        cout << "unexpected error!" << endl;
   // db.unlinkDB();
    db.closeDB();
    return 0;

}
#endif // HASHDB_TEST
