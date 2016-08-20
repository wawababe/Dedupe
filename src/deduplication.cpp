#include "deduplication.h"

Dedupe::Dedupe(bool vbose)
{
    memset(&d_pkg_hdr, 0, D_PKG_HDR_SZ);
    d_htab_pathname = 0; //hashtable for path names
    d_htab_sb_csum = 0; //hashtable for SB file chunking
    d_htab_bindex = 0; // hashtable for chunking blocks index

    d_chunk_alg = D_CHUNK_FSP;
    d_cdc_hashfun = HashFunctions::APHash; // default as adler32_rolling
    d_rolling_hash = false;
    d_cdc_hash_mod = 4096; //8192;//16384; //BLOCK_SIZE;
    d_cdc_chunk_mark = 13;

    d_sb_block_sz = 4096; //4096; //default size  of the sliding block as 4096 bytes
    d_fsp_block_sz = 4096;
    verbose = vbose;
    memset(d_pkg_name, 0, PATH_MAX_LEN);
    memset(d_ldata_name, 0, PATH_MAX_LEN);
    memset(d_bdata_name, 0, PATH_MAX_LEN);
    memset(d_mdata_name, 0, PATH_MAX_LEN);

    pid_t pid = getpid();
    sprintf(d_pkg_name,   "data/.dedup_%s_%d", MAGIC_TMP_FILE_NAME, pid);
    sprintf(d_ldata_name, "data/.ldata_%s_%d", MAGIC_TMP_FILE_NAME, pid);
    sprintf(d_bdata_name, "data/.bdata_%s_%d", MAGIC_TMP_FILE_NAME, pid);
    sprintf(d_mdata_name, "data/.mdata_%s_%d", MAGIC_TMP_FILE_NAME, pid);

    char tabname[PATH_MAX_LEN] = {0};
    char bloomname[PATH_MAX_LEN]  = {0};
    sprintf(tabname, "data/BigHashTable/.hashdb_pathname_%d.db", pid);
    sprintf(bloomname, "data/BigHashTable/.hashdb_pathname_%d.bf", pid);
    d_htab_pathname = new BigHashTable(tabname, bloomname);

    if (d_chunk_alg == D_CHUNK_SB){
        memset(tabname, 0, PATH_MAX_LEN);
        memset(bloomname, 0, PATH_MAX_LEN);
        sprintf(tabname, "data/BigHashTable/.hashdb_sbcsum_%d.db", pid);
        sprintf(bloomname, "data/BigHashTable/.hashdb_sbcsum_%d.bf", pid);
        d_htab_sb_csum = new BigHashTable(tabname, bloomname);
    }
}

Dedupe::~Dedupe()
{
    if (d_htab_bindex){
        delete d_htab_bindex;
        d_htab_bindex = 0;
    }
    if (d_htab_sb_csum){
        delete d_htab_sb_csum;
        d_htab_sb_csum = 0;
    }
    if (d_htab_pathname){
        delete d_htab_pathname;
        d_htab_pathname = 0;
    }
    clean_tmpfiles();
}

void Dedupe::clean_tmpfiles()
{
   // unlink(d_bdata_name);
    unlink(d_mdata_name);
    unlink(d_ldata_name);
}
int Dedupe::set_cdc_hashfun(const char *hashfunc_name)
{
    if (0 == strcmp(hashfunc_name, D_ROLLING_HASH)){
        d_rolling_hash = true;
        if(verbose)
            cout << "Info: set cdc chunk hash function as " << hashfunc_name << " in Dedupe::set_cdc_hashfun(...)"<< endl;
        return 0;
    }
    int num = sizeof(CDC_HASHFUN) / sizeof(CDC_HASHFUN[0]);
    for (int i = 0; i < num; i++){
        if (0 == strcmp(hashfunc_name, CDC_HASHFUN[i].hashfunc_name)){
            if(verbose)
                cout << "Info: set cdc chunk hash function as " << hashfunc_name << " in Dedupe::set_cdc_hashfun(...)"<< endl;
            d_cdc_hashfun = CDC_HASHFUN[i].hashfunc;
            d_rolling_hash = false;
            return 0;
        }
    }
    fprintf(stderr, "Error: set cdc hash as %s in Dedupe::set_cdc_hashfun(...)\n", hashfunc_name);
    return -1;
}

int Dedupe::create_package(const char *pkg_name)
{
    fstream pkg_file;
    D_Package_Header pkg_hdr;
    memset(&pkg_hdr, 0, D_PKG_HDR_SZ);
    pkg_hdr.magic_nr = DEDUP_MAGIC_NUM;
    pkg_hdr.fsp_block_sz = d_fsp_block_sz;
    pkg_hdr.sb_block_sz = d_sb_block_sz;
    pkg_hdr.blockid_sz = BLOCK_ID_SIZE;
    pkg_hdr.ublocks_len = 0;
    pkg_hdr.ldata_offset = D_PKG_HDR_SZ + pkg_hdr.ublocks_len;
    pkg_hdr.mdata_offset = pkg_hdr.ldata_offset + D_LOGIC_BLOCK_ENTRY_SZ * pkg_hdr.ublocks_nr;

    pkg_file.open(pkg_name, ios::binary | ios::out);
    if (!pkg_file.is_open()){
        fprintf(stderr, "Error: can't open package file %s in Dedupe::create_package(...)\n", pkg_name);
        return -1;
    }
    pkg_file.write((const char *)(&pkg_hdr), D_PKG_HDR_SZ);
    pkg_file.close();
    return 0;
}

int Dedupe::show_pkg_header(const char *pkg_name)
{
    D_Package_Header pkg_hdr;
    fstream pkg_file;
    pkg_file.open(pkg_name, ios::binary | ios::in);
    if (!pkg_file.is_open()){
        fprintf(stderr, "Error: can't open package file %s in Dedupe::show_pkg_header(...)\n", pkg_name);
        return -1;
    }
    pkg_file.seekg(0, ios::beg);
    pkg_file >> noskipws;
    pkg_file.read((char *)(&pkg_hdr), D_PKG_HDR_SZ);
    int rsize = pkg_file.gcount();
    pkg_file.close();
    if (D_PKG_HDR_SZ != rsize){
        fprintf(stderr, "Error: read package header from %s in Dedupe::show_pkg_header(...)\n", pkg_name);
        return -1;
    }
    cout << "--------------- In Dedupe::show_pkg_header--------------"  << endl;
    cout << "1. magic number:            " << pkg_hdr.magic_nr << endl;
    cout << "2. files number:            " << pkg_hdr.files_nr << endl;
    cout << "3. unique blocks number:    " << pkg_hdr.ublocks_nr << endl;
    cout << "4. fsp chunk block size:    " << pkg_hdr.fsp_block_sz << endl;
    cout << "5. sb chunk win block size: " << pkg_hdr.sb_block_sz << endl;
    cout << "6. block_id_t type size:    " << pkg_hdr.blockid_sz << endl;
    cout << "7. unique blocks's length:  " << pkg_hdr.ublocks_len << endl;
    cout << "8. logic data offset:       " << pkg_hdr.ldata_offset << endl;
    cout << "9. file metadata offset:    " << pkg_hdr.mdata_offset << endl;
    return 0;
}


int Dedupe::remove_files(const char *pkg_name, int files_nr, char **files_remove)
{
    int ret = 0;
    D_Package_Header pkg_hdr;
    D_File_Entry fentry;
    D_Logic_Block_Entry lbentry;

    unsigned int rsize = 0;
    unsigned int remove_blocks_nr = 0, remove_files_nr = 0, remove_bytes = 0;
    char buf[BLOCK_MAX_SIZE] = {0};
    char *block_buf = 0;
    ListDB *lookup_table = 0;
    block_id_t *metadata = 0;
    block_id_t TOBE_REMOVED = 0;
    block_id_t value = 0;
    unsigned long long offset = 0;
    char pathname[PATH_MAX_LEN] = {0};
    char listdb_name[PATH_MAX_LEN] = {0};

    fstream pkg_file, ldata_file, bdata_file, mdata_file;

    pkg_file.open(pkg_name, ios::binary | ios::in);
    if (!pkg_file.is_open()){
        fprintf(stderr, "Error: open deduped package %s in Dedupe::remove_files(...)\n", pkg_name);
        return -1;
    }
    pkg_file.seekg(0, ios::beg);
    pkg_file >> noskipws;
    pkg_file.read((char *)(&pkg_hdr), D_PKG_HDR_SZ);
    rsize = pkg_file.gcount();
    if (D_PKG_HDR_SZ != rsize){
        fprintf(stderr, "Error: read deduped package header in Dedupe::remove_files(...)\n");
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }
    if (DEDUP_MAGIC_NUM != pkg_hdr.magic_nr){
        fprintf(stderr, "Error: wrong magic number for deduped package in Deedupe::remove_files(...)\n");
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }

    memcpy(&d_pkg_hdr, &pkg_hdr, D_PKG_HDR_SZ);
    TOBE_REMOVED = d_pkg_hdr.ublocks_nr;

    ldata_file.open(d_ldata_name, ios::binary | ios::out);
    if (!ldata_file.is_open()){
        fprintf(stderr, "Error: create temporary logic data file %s in Dedupe::remove_files(...)\n", d_ldata_name);
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }

    bdata_file.open(d_bdata_name, ios::binary | ios::out);
    if (!bdata_file.is_open()){
        fprintf(stderr, "Error: create temporary unique block data file %s in Dedupe::remove_files(...)\n", d_bdata_name);
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }

    mdata_file.open(d_mdata_name, ios::binary | ios::out);
    if (!mdata_file.is_open()){
        fprintf(stderr, "Error: create temporary file entry file d%s in Dedupe::remove_files(...)\n", d_mdata_name);
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }

    /*traverse file metadata in the package to build up lookup_table */
    lookup_table = new ListDB(BLOCK_ID_SIZE, DEFAULT_CACHE_SZ, DEFAULT_SWAP_SZ);
    if (0 == lookup_table){
        fprintf(stderr, "Error: malloc lookup table in Dedupe::remove_files(...)\n");
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }
    sprintf(listdb_name, "data/ListDB/lookup_table_%d.listdb", getpid());
	mkdir("data",766);
    mkdir("data/ListDB",766);
    if (-1 == lookup_table->opendb(listdb_name)){
        fprintf(stderr, "Error: open listdb \"%s\" in Dedupe::remove_files(...)\n", listdb_name);
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }

    value = 0;
    for (unsigned int i = 0; i < d_pkg_hdr.ublocks_nr; i++){
        if (-1 == lookup_table->setvalue(i, &value)){
            fprintf(stderr, "Error: set ListDB item's value as 0 in Dedupe::remove_files(...)\n");
            ret = -1;
            goto _REMOVE_FILES_EXIT;
        }
    }

    offset = d_pkg_hdr.mdata_offset;
    for (unsigned int i = 0; i < d_pkg_hdr.files_nr; i++){
        pkg_file.seekg(offset, ios::beg);
        pkg_file.read((char *)(&fentry), D_FILE_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_FILE_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read %uth file entry in Dedupe::remove_files(...)\n", i);
            ret = -1;
            goto _REMOVE_FILES_EXIT;
        }
        memset(pathname, 0, PATH_MAX_LEN);
        pkg_file.read(pathname, fentry.fname_len);
        rsize = pkg_file.gcount();
        if (rsize != fentry.fname_len){
            fprintf(stderr, "Error: read %uth file entry's file path name in Dedupe::remove_files(...)\n", i);
            ret = -1;
            goto _REMOVE_FILES_EXIT;
        }

        //discard those files which are meant to be removed sooner or later
        if (!is_file_in_list(pathname, files_nr, files_remove)){
            metadata = (block_id_t *)malloc(BLOCK_ID_SIZE * fentry.fblocks_nr);
            if (0 == metadata){
                fprintf(stderr, "Error: malloc metadata of %uth file entry in Dedupe::remove_files(..)\n", i);
                ret = -1;
                goto _REMOVE_FILES_EXIT;
            }
            memset(metadata, 0, BLOCK_ID_SIZE * fentry.fblocks_nr);
            pkg_file.read((char *)metadata, BLOCK_ID_SIZE * fentry.fblocks_nr);
            rsize = pkg_file.gcount();
            if (BLOCK_ID_SIZE * fentry.fblocks_nr != rsize ){
                fprintf(stderr, "Error: read metadata of %uth file entry in Dedupe::remove_files(...)\n", i);
                ret = -1;
                goto _REMOVE_FILES_EXIT;
            }

            for (unsigned int j = 0; j < fentry.fblocks_nr; j++){
                value = 0;
                if (0 != lookup_table->getvalue(metadata[j], &value)){
                    fprintf(stderr, "Error: get %uth ublock's reference numbers from listdb lookup table in Dedupe::remove_files(...)\n",metadata[j]);
                    ret = -1;
                    goto _REMOVE_FILES_EXIT;
                }
                value++;
                if (-1 == lookup_table->setvalue(metadata[j], &value)){
                    fprintf(stderr, "Error: set %uth ublock's referenced numbers from listdb lookup table in Dedupe::remove_files(...)\n", metadata[j]);
                    ret = -1;
                    goto _REMOVE_FILES_EXIT;
                }
            }
            if (metadata){
                free(metadata);
                metadata = 0;
            }
        }//process each file entry, which does not belong to the files to be removed

        offset += fentry.fentry_sz;
    }//traverse file metadata in the deduped package, prepare for removing files

    remove_blocks_nr = 0;
    block_buf = (char *)malloc(BLOCK_MAX_SIZE);
    if (0 == block_buf){
        fprintf(stderr, "Error: malloc block buf in Dedupe::remove_files(...)\n");
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }

    bdata_file.seekp(0, ios::beg);
    bdata_file.write((const char *)(&pkg_hdr), D_PKG_HDR_SZ);
    ldata_file.seekp(0, ios::beg);
    offset = pkg_hdr.ldata_offset;
    /*start to rebuild unique blocks, logic block entries, file metadata
    into bdata_file, ldata_file and mdata_file
    */
    for(unsigned int i = 0; i < pkg_hdr.ublocks_nr; i++){
        pkg_file.seekg(offset, ios::beg);
        pkg_file.read((char *)(&lbentry), D_LOGIC_BLOCK_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_LOGIC_BLOCK_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read %uth logic block in Dedupe::remove_files(...)\n", i);
            ret = -1;
            goto _REMOVE_FILES_EXIT;
        }
        value = 0;
        if (0 != lookup_table->getvalue(i, &value)){
            fprintf(stderr, "Error: get %uth block's reference numbers via ListDB::getvalue in Dedupe::remove_files(...)\n", i);
            ret = -1;
            goto _REMOVE_FILES_EXIT;
        }
        if (0 == value){
            value = TOBE_REMOVED;
            if (-1 == lookup_table->setvalue(i, &value)){
                fprintf(stderr, "Error: set %uth block's value as TOBE_REMOVED via ListDB::getvalue in Dedupe::remove_files(...)\n", i);
                ret = -1;
                goto _REMOVE_FILES_EXIT;
            }

            remove_blocks_nr++;
            remove_bytes += lbentry.ublock_len;
        }else{
            value = i - remove_blocks_nr; //!tricky: set org block id i as i-remove_blocks_nr;
            if (-1 == lookup_table->setvalue(i, &value)){
                fprintf(stderr, "Error: set %uth ublock's new id as %d via ListDB::setvalue in Dedupe::remove_files(..)\n", i, value);
                ret = -1;
                goto _REMOVE_FILES_EXIT;
            }
            memset(block_buf, 0, BLOCK_MAX_SIZE);
            pkg_file.seekg(lbentry.ublock_off, ios::beg);
            pkg_file.read(block_buf, lbentry.ublock_len);
            rsize = pkg_file.gcount();
            if (rsize != lbentry.ublock_len){
                fprintf(stderr, "Error: read unique block, org_id=%u, new_id=%u in Dedupe::remove_files(...)\n ", i, value);
                ret = -1;
                goto _REMOVE_FILES_EXIT;
            }
            lbentry.ublock_off -= remove_bytes;
            ldata_file.write((const char *)(&lbentry),D_LOGIC_BLOCK_ENTRY_SZ); //!might need to seekp
            bdata_file.write(block_buf, lbentry.ublock_len);
        }

        offset += D_LOGIC_BLOCK_ENTRY_SZ;
    }


    /*start to rebuild file metadata */
    remove_files_nr = 0;
    offset = pkg_hdr.mdata_offset;
    mdata_file.seekp(0, ios::beg);
    for (unsigned int i = 0; i < pkg_hdr.files_nr; i++){
        pkg_file.seekg(offset, ios::beg);
        pkg_file.read((char*)(&fentry), D_FILE_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_FILE_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read %uth file entry from deduped package in Dedupe::remove_files(...)\n", i);
            ret = -1;
            goto _REMOVE_FILES_EXIT;
        }
        memset(pathname, 0, PATH_MAX_LEN);
        pkg_file.read(pathname, fentry.fname_len);
        rsize = pkg_file.gcount();
        if (rsize != fentry.fname_len){
            ret = -1;
            goto _REMOVE_FILES_EXIT;
        }
        if (metadata){
            free(metadata);
            metadata = 0;
        }
        if (!is_file_in_list(pathname, files_nr, files_remove)){ // need to write the metadata into mdata
            metadata = (block_id_t *)malloc(BLOCK_ID_SIZE * fentry.fblocks_nr);
            if (0 == metadata){
                fprintf(stderr, "Error: malloc metadata for file \"%s\" in Dedupe::remove_files(...)\n", pathname);
                ret = -1;
                goto _REMOVE_FILES_EXIT;
            }
            pkg_file.read((char *)metadata, BLOCK_ID_SIZE * fentry.fblocks_nr);
            rsize = pkg_file.gcount();
            if (rsize != BLOCK_ID_SIZE * fentry.fblocks_nr){
                fprintf(stderr, "Error: read metadata of file \"%s\" in Dedupe::remove_files(...)\n", pathname);
                ret = -1;
                goto _REMOVE_FILES_EXIT;
            }
            for (unsigned int j = 0; j < fentry.fblocks_nr; j++){
                value = 0;
                if (0 != lookup_table->getvalue(metadata[j], &value)){
                    fprintf(stderr, "Error: modify %uth file entry's metadata[%u] in Dedupe::remove_files(...)\n", i, j);
                    ret = -1;
                    goto _REMOVE_FILES_EXIT;
                }
                metadata[j] = value;
            }
            memset(block_buf, 0, BLOCK_MAX_SIZE);
            pkg_file.read(block_buf, fentry.last_block_sz);
            rsize = pkg_file.gcount();
            if (rsize != fentry.last_block_sz){
                fprintf(stderr, "Error: read last block of file entry %u in Dedupe::remove_files(...)\n", i);
                ret = -1;
                goto _REMOVE_FILES_EXIT;
            }

            mdata_file.write((const char *)(&fentry), D_FILE_ENTRY_SZ);
            mdata_file.write(pathname, fentry.fname_len);
            mdata_file.write((const char *)metadata, BLOCK_ID_SIZE * fentry.fblocks_nr);
            mdata_file.write(block_buf, fentry.last_block_sz);

            if (metadata){
                free(metadata);
                metadata = 0;
            }
        }else
            remove_files_nr++;
        offset += fentry.fentry_sz;
    }
    pkg_file.close();
    ldata_file.close();
    mdata_file.close();

    /*rebuild package header */
    d_pkg_hdr.files_nr -= remove_files_nr;
    d_pkg_hdr.ublocks_nr -= remove_blocks_nr;
    d_pkg_hdr.ublocks_len -= remove_bytes;
    d_pkg_hdr.ldata_offset = D_PKG_HDR_SZ + d_pkg_hdr.ublocks_len;
    d_pkg_hdr.mdata_offset = d_pkg_hdr.ldata_offset + D_LOGIC_BLOCK_ENTRY_SZ * d_pkg_hdr.ublocks_nr;

    bdata_file.seekp(0, ios::beg);
    bdata_file.write((const char *)(&d_pkg_hdr), D_PKG_HDR_SZ);


    ldata_file.open(d_ldata_name, ios::binary | ios::in);
    if (!ldata_file.is_open()){
        fprintf(stderr, "Error: re-open ldata file \"%s\" in Dedupe::remove_files(...)\n", d_ldata_name);
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }

    ldata_file >> noskipws;
    ldata_file.seekg(0, ios::beg);
    bdata_file.seekp(0, ios::end);
    while(!ldata_file.eof()){
        memset(buf, 0, BLOCK_MAX_SIZE);
        ldata_file.read(buf, BLOCK_MAX_SIZE);
        rsize = ldata_file.gcount();
        bdata_file.write(buf, rsize);
    }
    ldata_file.close();

    mdata_file.open(d_mdata_name, ios::binary | ios::in);
    if (!mdata_file.is_open()){
        fprintf(stderr, "Error: re-open mdata file \"%s\" in Dedupe::remove_files(...)\n", d_mdata_name);
        ret = -1;
        goto _REMOVE_FILES_EXIT;
    }
    mdata_file >> noskipws;
    mdata_file.seekg(0, ios::beg);
    bdata_file.seekp(0, ios::end);

    while(!mdata_file.eof()){
        memset(buf, 0, BLOCK_MAX_SIZE);
        mdata_file.read(buf, BLOCK_MAX_SIZE);
        rsize = mdata_file.gcount();
        bdata_file.write(buf, rsize);
    }
    bdata_file.close();
    mdata_file.close();
    unlink(pkg_name);
    rename(d_bdata_name, pkg_name);

_REMOVE_FILES_EXIT:
    if (pkg_file.is_open())
        pkg_file.close();
    if (ldata_file.is_open())
        ldata_file.close();
    if (bdata_file.is_open())
        bdata_file.close();
    if (mdata_file.is_open())
        mdata_file.close();
    if (metadata){
        free(metadata);
        metadata = 0;
    }
    if (block_buf){
        free(block_buf);
        block_buf = 0;
    }
    if (lookup_table){
        lookup_table->closedb();
        lookup_table->unlinkdb();
        delete lookup_table;
        lookup_table = 0;
    }
    return ret;
}

int Dedupe::insert_files(const char *pkg_name, int files_nr, char **src_files)
{
    time_t start_time, end_time;
    char buf[BUFSIZ] = {0};
    int ret = 0, rsize = 0, prepos = 0;
    struct stat stat_buf;
    memset(d_pkg_name, 0, PATH_MAX_LEN);
    sprintf(d_pkg_name, "%s", pkg_name);
    ifstream pkg_file;
    pkg_file.open(d_pkg_name, ios::binary | ios::in);
    if (!pkg_file){
        fprintf(stderr, "Error: deduped package file %s not exist in Dedupe::insert_files(...)\n", pkg_name);
        return -1;
    }
    pkg_file.seekg(0, ios::beg);

    pkg_file >> noskipws;
    fstream ldata_file, bdata_file, mdata_file;
    ldata_file.open(d_ldata_name, ios::binary | ios::out);
    bdata_file.open(d_bdata_name, ios::binary | ios::out);
    mdata_file.open(d_mdata_name, ios::binary | ios::out);

    if (!ldata_file.is_open() || !bdata_file.is_open() || !mdata_file.is_open()){
        fprintf(stderr, "Error: open ldata, bdata, mdata in Dedupe::insert_files(...)\n");
        ret = -1;
        goto _INSERT_FILES_EXIT;
    }

    d_htab_bindex = new BigHashTable;
    ret = prepare_insert(pkg_file, ldata_file, bdata_file, mdata_file);

    ldata_file.close();
    ldata_file.open(d_ldata_name, ios::binary | ios::out | ios::in );
    bdata_file.close();
    bdata_file.open(d_bdata_name, ios::binary | ios::out | ios::in );
    mdata_file.close();
    mdata_file.open(d_mdata_name, ios::binary | ios::out | ios::in );
    ldata_file.seekp(0, ios::end);
    bdata_file.seekp(0, ios::end);
    mdata_file.seekp(0, ios::end);

    if (0 != ret)
            goto _INSERT_FILES_EXIT;
    start_time = time(0);
    for(int i = 0; i < files_nr; i++){
        ret = stat(src_files[i], &stat_buf);
        if (0 != ret){
            switch(errno){
            case ENOENT:
                fprintf(stderr, "Warning: file %s not exist in Dedupe::insert_files(...)\n", src_files[i]);
                break;
            default:
                fprintf(stderr, "Warning: stat %s in Dedupe::register_insert_files(...)\n", src_files[i]);
            }
            continue;
        }
        if (S_ISREG(stat_buf.st_mode) || S_ISDIR(stat_buf.st_mode)){
            if (verbose)
                fprintf(stderr, "Info: expect to dedupe file %s in Dedupe::insert_files(...)\n", src_files[i]);
            //get filename position in pathname
            prepos = strlen(src_files[i]) - 1;
            if (strcmp(src_files[i], "/") != 0 &&
                ( *(src_files[i] + prepos) == '/' || *(src_files[i] + prepos) == '\\')){
                *(src_files[i] + prepos--) = '\0';
            }
            while(*(src_files[i] + prepos) != '/' && *(src_files[i]+prepos) != '\\' &&  prepos >= 0) prepos--;
            prepos++;

            if (S_ISREG(stat_buf.st_mode)){
                ret = register_file(src_files[i], prepos, ldata_file, bdata_file, mdata_file);
                if (ret != 0){
                    fprintf(stderr, "Warning: register file %s failed, keep going in Dedupe::insert_files::register_file(...)\n", src_files[i]);
                }
            }else{
                ret = register_dir(src_files[i], prepos, ldata_file, bdata_file, mdata_file);
                if (0 != ret){
                    fprintf(stderr, "Warning: register directory %s failed, keep going in Dedupe::insert_files::register_file(...)\n", src_files[i]);
                }
            }

        }else{
            if (verbose){
                fprintf(stderr, "%s is not regular file or dir in Dedupe::insert_files\n", src_files[i]);
            }
        }
    }
 //   ret = register_file("J:/33466.docx", 3, ldata_file, bdata_file, mdata_file);
  //  ret = register_dir("J:/test", 3, ldata_file, bdata_file, mdata_file);
    d_pkg_hdr.fsp_block_sz = d_fsp_block_sz;
    d_pkg_hdr.sb_block_sz = d_sb_block_sz;
    d_pkg_hdr.ldata_offset = D_PKG_HDR_SZ + d_pkg_hdr.ublocks_len;
    d_pkg_hdr.mdata_offset = d_pkg_hdr.ldata_offset + D_LOGIC_BLOCK_ENTRY_SZ * d_pkg_hdr.ublocks_nr;
    bdata_file.seekp(0, ios::beg);
    bdata_file.write((const char *)(&d_pkg_hdr), D_PKG_HDR_SZ);

    ldata_file.close();
    ldata_file.open(d_ldata_name, ios::binary | ios::in);
    ldata_file >> noskipws;
    bdata_file.seekp(0, ios::end);
    cout << "the unique blocks length is " << d_pkg_hdr.ublocks_len << endl;
    cout << "after inserting these files, bdata file size = " << bdata_file.tellp() << endl;
    while(!ldata_file.eof()){
        ldata_file.read(buf, BUFSIZ);
        rsize = ldata_file.gcount();
      //  cout << "single read ldata size: " << rsize << endl;
        bdata_file.write(buf, rsize);
    }
    ldata_file.close();

    mdata_file.close();

    mdata_file.open(d_mdata_name, ios::binary | ios::in);
    mdata_file >> noskipws;
    while(!mdata_file.eof()){
        mdata_file.read(buf, BUFSIZ);
        rsize = mdata_file.gcount();
      //  cout << "sing read mdata size: " << rsize << endl;
        bdata_file.write(buf, rsize);
    }
    mdata_file.close();
    bdata_file.close();
    pkg_file.close();
    unlink(pkg_name);
    rename(d_bdata_name, pkg_name);
    ret = 0;
    end_time = time(0);
    cout << "Info: insert files with time " << (long)(end_time - start_time) << "s in Dedupe::insert_files(...)" << endl;

_INSERT_FILES_EXIT:
    if (pkg_file.is_open()) pkg_file.close();
    if (ldata_file.is_open()) ldata_file.close();
    if (bdata_file.is_open()) bdata_file.close();
    if (mdata_file.is_open()) mdata_file.close();
    if (d_htab_bindex){
        delete d_htab_bindex;
        d_htab_bindex = 0;
    }
    return ret;
}


int Dedupe::prepare_insert(ifstream &pkg_file, fstream &ldata_file, fstream &bdata_file, fstream &mdata_file)
{
    if (!d_htab_bindex)
        return -1;

    unsigned int rsize = 0;
    int ret = 0;
    unsigned long long meta_offset = 0;
    D_File_Entry fentry;
    char pathname[PATH_MAX_LEN] = {0};

    D_Package_Header pkg_hdr;
    pkg_file.read((char *)(&pkg_hdr), D_PKG_HDR_SZ);
    rsize = pkg_file.gcount();
    if (rsize != D_PKG_HDR_SZ){
        fprintf(stderr, "Error: read package header in Dedupe::insert_files::prepare_insert(...)\n");
        return -1;
    }
    if (pkg_hdr.magic_nr != DEDUP_MAGIC_NUM){
        fprintf(stderr, "Error: wrong package magic number in Dedupe::insert_files::prepare_insert(...)\n");
        return -1;
    }
    memcpy(&d_pkg_hdr, &pkg_hdr, D_PKG_HDR_SZ);
    bdata_file.write((const char *)(&pkg_hdr), D_PKG_HDR_SZ);


    char *buf = 0;
    buf = (char *)malloc(BUF_MAX_SIZE);
    if (0 == buf){
        fprintf(stderr, "Error: malloc buf in Dedupe::prepare_insert(...)\n");
        return -1;
    }
    memset(buf, 0, BUF_MAX_SIZE);
    D_Logic_Block_Entry lblock_entry;
   // unsigned char md5val[33] = {0};
    unsigned char csumstr[16] = {0};
    unsigned int* bid_list = 0;
    int value_sz = 0;
    for (unsigned int i = 0; i < d_pkg_hdr.ublocks_nr; i++){

        /*read logic block i, write it into ldata_file*/
        pkg_file.seekg(d_pkg_hdr.ldata_offset + D_LOGIC_BLOCK_ENTRY_SZ * i, ios::beg);
        pkg_file.read((char *)(&lblock_entry), D_LOGIC_BLOCK_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_LOGIC_BLOCK_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read logic block entry in Dedupe::prepare_insert(...)\n");
            ret = -1;
            goto _PREPARE_INSERT_EXIT;
        }

        ldata_file.write((const char *)(&lblock_entry), D_LOGIC_BLOCK_ENTRY_SZ);

        /*read physical unique block i, write it into bdata_file*/
        pkg_file.seekg(lblock_entry.ublock_off, ios::beg); //
        pkg_file.read(buf, lblock_entry.ublock_len);
        rsize = pkg_file.gcount();
        if (rsize != lblock_entry.ublock_len){
            fprintf(stderr, "Error: read physical unique block in Dedupe::prepare_insert(...)\n");
            ret = -1;
            goto _PREPARE_INSERT_EXIT;
        }

        bdata_file.write( (const char *)buf, rsize);

        /*rebuild BigHashTable for block index by md5: hash entry format <md5, idnum|id1|...|idn| */
        value_sz = 0;
        bid_list = 0;
        bid_list = (block_id_t *)d_htab_bindex->getvalue(lblock_entry.block_md5, value_sz);
        bid_list = (value_sz == 0) ? (block_id_t *)malloc(BLOCK_ID_SIZE * 2) :
            (block_id_t *)realloc(bid_list, BLOCK_ID_SIZE + (unsigned int)value_sz );
        if (0 == bid_list){
            fprintf(stderr, "Error: malloc/realloc in Dedupe::prepare_insert(...)\n");
            ret = -1;
            goto _PREPARE_INSERT_EXIT;
        }
        *bid_list = (value_sz == 0) ? 1 : (*bid_list) + 1;
        *(bid_list + *bid_list) = i;
        value_sz = (*bid_list + 1) * BLOCK_ID_SIZE;
        d_htab_bindex->insert(lblock_entry.block_md5, bid_list, value_sz);

        if (bid_list){
            free(bid_list);
            bid_list = 0;
        }

        /*rebuild BigHashTable for sliding block index by adler checksum*/
        if (d_chunk_alg == D_CHUNK_SB){
            unsigned int hkey = 0;
            hkey = adler32(buf, rsize);
            uint2str(hkey, csumstr);
            d_htab_sb_csum->insert(csumstr, (void *)"1", 1);
        }

    }

    /*read file metadata: (file entry, pathname, block id list, last block),*/
      meta_offset = d_pkg_hdr.mdata_offset;
      for (unsigned int i = 0; i < d_pkg_hdr.files_nr; i++){
        pkg_file.seekg(meta_offset, ios::beg);
        pkg_file.read((char *)(&fentry), D_FILE_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_FILE_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read %dth file entry in Dedupe::prepare_insert(...)\n", i);
            ret = -1;
            goto _PREPARE_INSERT_EXIT;
        }

        //rebuild BigHashTable for file name : d_htab_pathname
        memset(pathname, 0, PATH_MAX_LEN);
        pkg_file.read(pathname, fentry.fname_len);
        rsize = pkg_file.gcount();
        if (rsize != fentry.fname_len){
            fprintf(stderr, "Error: read %dth file entry's path name in Dedupe::prepare_insert(...)\n", i);
            ret = -1;
            goto _PREPARE_INSERT_EXIT;
        }
        d_htab_pathname->insert(pathname, (void *)"1", 1);
        meta_offset += fentry.fentry_sz;
      }
     // rewrite the file metadata from pkg_file into mdata_file
      pkg_file.seekg(d_pkg_hdr.mdata_offset, ios::beg);
      while(!pkg_file.eof()){
        pkg_file.read(buf, 4096);
        rsize = pkg_file.gcount();
        mdata_file.write(buf, rsize);
      }
      ret = 0;

_PREPARE_INSERT_EXIT:
    if (buf){
        free(buf);
        buf = 0;
    }
    return ret;
}


int Dedupe::register_dir(char *fullpath, int prepos, fstream &ldata_file, fstream &bdata_file, fstream &mdata_file)
{
    if (!ldata_file.is_open() || !bdata_file.is_open() || !mdata_file.is_open()){
        fprintf(stderr, "Error: ldata_file, bdata_file or mdata_file not open in Dedupe::register_dir(...)\n");
        return -1;
    }

    DIR *dp = 0;
    struct dirent *dirp = 0;
    struct stat stat_buf;
    char subpath[PATH_MAX_LEN] = {0};
    int ret = 0;

    dp = opendir(fullpath);
    if (0 == dp){
        fprintf(stderr, "Error: open directory %s in Dedupe::register_dir(...)\n", fullpath);
        return -1;
    }
    while(0 != (dirp = readdir(dp))){
        if (0 == strcmp(dirp->d_name, ".") || 0 == strcmp(dirp->d_name, ".."))
            continue;
        memset(subpath, 0, PATH_MAX_LEN);
        sprintf(subpath, "%s/%s", fullpath, dirp->d_name);
        ret = stat(subpath, &stat_buf);
        if (0 != ret){
            switch(errno){
            case ENOENT:
                fprintf(stderr, "Error: file %s not exist in Dedupe::register_dir(...)\n", subpath);
                break;
            default:
                fprintf(stderr, "Error: stat %s in Dedupe::register_dir(...)\n", subpath);
            }
        }else{
            if (S_ISREG(stat_buf.st_mode)){
                ret = register_file(subpath,prepos, ldata_file, bdata_file, mdata_file);
                if (0 != ret){
                    fprintf(stderr, "Error: dedupe file %s in Dedupe::register_dir::register_file(...)\n", subpath);
                    return ret;
                }
            }else if (S_ISDIR(stat_buf.st_mode)){
                ret = register_dir(subpath, prepos, ldata_file, bdata_file, mdata_file);
                if (0 != ret){
                    fprintf(stderr, "Error: dedupe directory %s in Dedupe::register_dir(...)\n", subpath);
                    return ret;
                }
            }
        }
    }

    closedir(dp);
    return 0;
}

int Dedupe::register_file(char *fullpath, int prepos, fstream &ldata_file, fstream &bdata_file, fstream &mdata_file)
{
    if (!ldata_file.is_open() || !bdata_file.is_open() || !mdata_file.is_open()){
        fprintf(stderr, "Error: ldata_file, bdata_file or mdata_file not open in Dedupe::register_file(...)\n");
        return -1;
    }
    int ret = 0;
    struct stat stat_buf;
    ret = stat(fullpath, &stat_buf);
    if (0 != ret){
        switch(errno){
        case ENOENT:
            fprintf(stderr, "Error: stat file %s not exist in Dedupe::register_file(...)\n", fullpath);
            break;
        default:
            fprintf(stderr, "Error:stat file %s unexpected err in Dedupe::register_file(...)\n", fullpath);
        }
        return -1;
    }

    D_File_Entry fentry;
    fentry.org_file_sz = stat_buf.st_size;
    fentry.atime = stat_buf.st_atime;
    fentry.mtime = stat_buf.st_mtime;
    fentry.mode = stat_buf.st_mode;

    unsigned int blocks_count = 0;
    unsigned int meta_cap = 0;
    block_id_t *metadata = 0;
    unsigned int last_block_len = 0;
    char *last_block = 0;
    ifstream src_file;

    switch (d_chunk_alg){
        case D_CHUNK_FSP:
            meta_cap = fentry.org_file_sz / d_fsp_block_sz + 1;
            break;
        case D_CHUNK_CDC:
            meta_cap = fentry.org_file_sz / BLOCK_MIN_SIZE + 1;
            break;
        case D_CHUNK_SB:
            meta_cap = fentry.org_file_sz / (d_sb_block_sz /2 ) + 1;
            break;
        case D_CHUNK_AAC:
            if (d_fsp_block_sz <= BLOCK_MIN_SIZE)
                meta_cap = fentry.org_file_sz / d_fsp_block_sz + 1;
            else
                meta_cap = fentry.org_file_sz / BLOCK_MIN_SIZE + 1;
            break;
        default:
            fprintf(stderr, "Error: set metadata's capacity in Dedupe::register_file(...)\n");
            return -1;
    }
    metadata = (block_id_t *)malloc(BLOCK_ID_SIZE * meta_cap);
    if (0 == metadata){
        fprintf(stderr, "Error: malloc metadata in Dedupe::register_file(...)\n");
        return -1;
    }
    memset(metadata, 0, BLOCK_ID_SIZE * meta_cap);

    last_block = (char *)malloc(BUF_MAX_SIZE);
    if (0 == last_block){
        fprintf(stderr, "Error: malloc last_block in Dedupe::register_file(...)\n");
        ret = -1;
        goto _REGISTER_FILE_EXIT;
    }
    memset(last_block, 0, BUF_MAX_SIZE);

    src_file.open(fullpath, ios::binary | ios::in);
    if (!src_file.is_open()){
        fprintf(stderr, "Error: open source file \"%s\" in Dedupe::register_file(...)\n", fullpath);
        return -1;
    }

    src_file.seekg(0, ios::beg);
    src_file >> noskipws;
    switch (d_chunk_alg){
    case D_CHUNK_FSP:
        ret = chunk_fsp(src_file, ldata_file, bdata_file, blocks_count, meta_cap, metadata,
                        last_block_len, last_block);
        break;

    case D_CHUNK_CDC:
        ret = chunk_cdc(src_file, ldata_file, bdata_file, blocks_count, meta_cap, metadata,
                        last_block_len, last_block);
        break;

    case D_CHUNK_SB:
        ret = chunk_sb(src_file, ldata_file, bdata_file, blocks_count, meta_cap, metadata,
                       last_block_len, last_block);
        break;

    case D_CHUNK_AAC:
        ret = chunk_aac(src_file, ldata_file, bdata_file, blocks_count, meta_cap, metadata,
                        last_block_len, last_block);
        break;
    default:
        fprintf(stderr, "Error: unknown chunk algorithm in Dedupe::register_file(...)\n");
        ret = -1;
    }
    if (0 != ret){
        fprintf(stderr, "Error: chunk file %s failed in Dedupe::register_file(...)\n", fullpath);
        goto _REGISTER_FILE_EXIT;
    }
    if (verbose){
        fprintf(stderr, "Info: %d. %s\n", d_pkg_hdr.files_nr, fullpath);
    }
    src_file.close();

    fentry.fblocks_nr = blocks_count;
    fentry.last_block_sz = last_block_len;
    fentry.fname_len = strlen(fullpath) - prepos;
    fentry.fentry_sz = D_FILE_ENTRY_SZ + fentry.fname_len + fentry.fblocks_nr * BLOCK_ID_SIZE + fentry.last_block_sz;

    mdata_file.write((const char*)(&fentry), D_FILE_ENTRY_SZ);
    mdata_file.write((const char*)(fullpath + prepos), fentry.fname_len);
    mdata_file.write((const char*)(metadata), fentry.fblocks_nr * BLOCK_ID_SIZE);
    mdata_file.write((const char*)last_block, fentry.last_block_sz);

    d_pkg_hdr.files_nr++;
    d_htab_pathname->insert(fullpath, (void *)"1", 1);

_REGISTER_FILE_EXIT:
    if (src_file.is_open()){
        src_file.close();
    }
    if (metadata){
        free(metadata);
        metadata = 0;
    }
    if (last_block){
        free(last_block);
        last_block = 0;
    }
    return ret;
}

int Dedupe::chunk_fsp(ifstream& src_file, fstream &ldata_file, fstream &bdata_file,
        unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata,
        unsigned int &last_block_len, char *last_block)
{
    if (!src_file.is_open() || !ldata_file.is_open() || !bdata_file.is_open()){
        fprintf(stderr, "Error: source file, ldata, bdata or mdata not open in Dedupe::chun_fsp(...)\n");
        return -1;
    }

    //metadata : block id list <bid1, bid2, bidn>
    blocks_count = 0;
    last_block_len = 0;
    unsigned char md5val[33] = {0};
    int ret = 0;

    const unsigned int BUF_SZ = d_fsp_block_sz * 2;
    char *buf = 0;
    unsigned int rsize = 0;

    buf = (char *)malloc(BUF_SZ);
    if (0 == buf){
        fprintf(stderr, "Error: malloc buf in Dedupe::chunk_fsp(...)\n");
        return -1;
    }
    memset(buf, 0, BUF_SZ);

    src_file.seekg(0, ios::beg);
    src_file >> noskipws;
    while(!src_file.eof()){
        src_file.read(buf, d_fsp_block_sz);
        rsize = src_file.gcount();
        if (rsize < d_fsp_block_sz ){
            last_block_len = rsize;
            if (last_block_len > 0)
                memcpy(last_block, buf, last_block_len);
            break;
        }else if(rsize != d_fsp_block_sz){
            fprintf(stderr, "Error: read source file in Dedupe::chunk_fsp(...)\n");
            ret = -1;
            goto _CHUNK_FSP_EXIT;
        }
        memset(md5val, 0, 33);
        MD5::message_digest_func(buf, rsize, md5val);
        ret = register_block(buf, rsize, md5val, ldata_file, bdata_file, blocks_count, meta_cap, metadata);
        if (0 != ret){
            fprintf(stderr, "Error: register_block(...) in Dedupe::chunk_fsp(...)\n");
            goto _CHUNK_FSP_EXIT;
        }
    }

_CHUNK_FSP_EXIT:
    if (buf){
        free(buf);
        buf = 0;
    }
    return ret;
}


int Dedupe::chunk_cdc(ifstream& src_file, fstream &ldata_file, fstream &bdata_file,
        unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata,
        unsigned int &last_block_len, char *last_block)
{
    if (!src_file.is_open() || !ldata_file.is_open() || !bdata_file.is_open()){
        fprintf(stderr, "Error: source file, ldata, bdata or mdata not open in Dedupe::chun_cdc(...)\n");
        return -1;
    }

    //metadata : block id list <bid1, bid2, bidn>
    blocks_count = 0;
    last_block_len = 0;

    int ret = 0;

    char buf[BUF_MAX_SIZE] = {0}; //128KB
    char block_buf[BLOCK_MAX_SIZE] = {0};
    char win_buf[BLOCK_WIN_SIZE+1] = {0};
    unsigned char md5val[33] = {0};
    char adler_pre_char;
    unsigned int bpos = 0, bexp_rsize = BUF_MAX_SIZE;
    //bpos : mark the start position of reading data to fill up buf
    //bexp_rsize: the number of empty size in the buf
    unsigned int head = 0, tail = 0; // indicate the window's sliding range
    unsigned int rsize = 0;
    unsigned int block_len = 0, old_block_len = 0;
    unsigned int win_hkey = 0; // hash value for window block

    src_file.seekg(0, ios::beg);
    src_file >> noskipws;
    unsigned int bcount = 0, lencount = 0;
    while(!src_file.eof()){
        src_file.read(buf+bpos, bexp_rsize);
        rsize = src_file.gcount();
        lencount += rsize;
        if ((block_len + bpos + rsize) < BLOCK_MIN_SIZE){//last block
            break;
        }//last block

        head = 0;
        tail = bpos + rsize;

        /*avoid unnecessary computation */
        if (block_len < BLOCK_MIN_SIZE - BLOCK_WIN_SIZE){
            old_block_len = block_len;
            //block_len = BLOCK_MIN_SIZE - BLOCK_WIN_SIZE;
            block_len = (block_len + tail - head) > (BLOCK_MIN_SIZE - BLOCK_WIN_SIZE) ?
                (BLOCK_MIN_SIZE - BLOCK_WIN_SIZE ): (block_len + tail - head);
            memcpy(block_buf, buf, block_len - old_block_len);
            head += block_len - old_block_len;
        }
        while( (head + BLOCK_WIN_SIZE) <= tail ){
            memset(win_buf, 0, BLOCK_WIN_SIZE+1);
            memcpy(win_buf, buf+head, BLOCK_WIN_SIZE);

            if (d_rolling_hash){
                win_hkey = (block_len == BLOCK_MIN_SIZE - BLOCK_WIN_SIZE) ?
                        adler32(win_buf, BLOCK_WIN_SIZE) :
                        adler32_rolling(win_hkey, BLOCK_WIN_SIZE, adler_pre_char, win_buf[BLOCK_WIN_SIZE-1]);
            }else{
                win_hkey = d_cdc_hashfun(win_buf);
            }

            bool isboundary = false;
            isboundary = win_hkey % d_cdc_hash_mod == d_cdc_chunk_mark;
            if (isboundary){
                memcpy(block_buf+block_len, buf+head, BLOCK_WIN_SIZE);
                head += BLOCK_WIN_SIZE;
                block_len += BLOCK_WIN_SIZE;
                if (block_len >= BLOCK_MIN_SIZE){
                    memset(md5val, 0, 33);
                    MD5::message_digest_func(block_buf, block_len, md5val);
                    ret = register_block(block_buf, block_len, md5val,
                        ldata_file, bdata_file, blocks_count, meta_cap, metadata);
                   if (0 != ret){
                        fprintf(stderr, "Error: register normal block with size=%d in Dedupe::chunk_cdc(...)\n", block_len);
                        goto _CHUNK_CDC_EXIT;
                   }
                   bcount++;
              //     if (verbose)
             //       fprintf(stderr, "Info: register %dth block with size %d in Dedupe::chunk_cdc(...)\n", bcount, block_len);

                   block_len = 0;
                }
            }else {
                block_buf[block_len++] = buf[head++];
                //control the block size within range (BLOCK_MIN_SIZE, BLOCK_MAX_SIZE)
                if (block_len >= BLOCK_MAX_SIZE){
                    memset(md5val, 0, 33);
                    MD5::message_digest_func(block_buf, block_len, md5val);
                    ret = register_block(block_buf, block_len, md5val,
                            ldata_file, bdata_file, blocks_count, meta_cap, metadata);
                    if (0 != ret){
                        fprintf(stderr, "Error: register abnormal block with size=%d in Dedupe::chunk_cdc(...)\n", block_len);
                        goto _CHUNK_CDC_EXIT;
                    }
                    bcount++;
               //     if (verbose)
                 //       fprintf(stderr, "Info: register %dth block with size %d in Dedupe::chunk_cdc(...)\n", bcount, block_len);
                    block_len = 0;
                }
            }

            //avoid unnecessary computation
            if (block_len == 0){
                block_len = (tail - head) > (BLOCK_MIN_SIZE - BLOCK_WIN_SIZE) ?
                    (BLOCK_MIN_SIZE - BLOCK_WIN_SIZE) : (tail - head);
                memcpy(block_buf, buf+head, block_len);
                head += block_len;
            }
            adler_pre_char = buf[head - 1];
        }

        //read expected size data from source file to fill up buf
        bpos = tail - head;
        bexp_rsize = BUF_MAX_SIZE - bpos;
        adler_pre_char = buf[head - 1];
        memmove(buf, buf+head, bpos);
        rsize = 0;
    }

    last_block_len = block_len + bpos + rsize;
    if (last_block_len > 0 ){
        memcpy(last_block, block_buf, block_len);
        memcpy(last_block+block_len, buf, bpos+rsize);
    }
 /*   if (verbose){
        fprintf(stderr, "Info: total read size = %d in Dedupe::chunk_cdc()\n", lencount);
        cout << "Info: unique blocks size = " << (unsigned long long)d_pkg_hdr.ublocks_len <<" in Dedupe::chunk_cdc()" <<endl;
        fprintf(stderr, "Info: last block length = %d in Dedupe::chunk_cdc()\n", last_block_len);
    }
*/
_CHUNK_CDC_EXIT:
    return ret;
}


int Dedupe::chunk_sb(ifstream& src_file, fstream &ldata_file, fstream &bdata_file,
        unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata,
        unsigned int &last_block_len, char *last_block)
{
    if (!src_file.is_open() || !ldata_file.is_open() || !bdata_file.is_open()){
        fprintf(stderr, "Error: source file, ldata, bdata or mdata not open in Dedupe::chun_sb(...)\n");
        return -1;
    }

    //metadata : block id list <bid1, bid2, bidn>
    blocks_count = 0;
    last_block_len = 0;

    int ret = 0;
    char *buf = 0; //[BUF_MAX_SIZE] = {0};
    unsigned int bpos = 0, bexp_rsize = d_sb_block_sz * 2 + 1; // BUF_MAX_SIZE;
    unsigned int head = 0, tail = 0; // indicate the sliding window's traversing range

    char *win_buf = 0; //[BLOCK_MAX_SIZE * 2] = {0};
    char adler_pre_char = 0;
    unsigned int win_hkey = 0; //hash value for sliding window, ak, sliding block
    unsigned char csumstr[16] = {0};
    unsigned char win_md5val[33] = {0};

    char *block_buf = 0; //[BLOCK_MAX_SIZE * 2] = {0};
    unsigned int block_len = 0;
    unsigned char block_md5val[33] = {0};

    unsigned int rsize = 0;
    unsigned int buf_size = d_sb_block_sz * 2 + 1;
    buf = (char *)malloc(buf_size);
    if (0 == buf){
        fprintf(stderr, "Error: malloc buf in Dedupe::chunk_sb(...)\n");
        ret = -1;
        goto _CHUNK_SB_EXIT;
    }
    memset(buf, 0, buf_size);

    win_buf = (char *)malloc(d_sb_block_sz + 1);
    if (0 == win_buf){
        fprintf(stderr, "Error: malloc buf for sliding window in Dedupe::chunk_sb(...)\n");
        ret = -1;
        goto _CHUNK_SB_EXIT;
    }
    memset(win_buf, 0, d_sb_block_sz + 1);

    block_buf = (char *)malloc(d_sb_block_sz + 1);
    if (0 == block_buf){
        fprintf(stderr, "Error: malloc block buf in Dedupe::chunk_sb(...)\n");
        ret = -1;
        goto _CHUNK_SB_EXIT;
    }
    memset(block_buf, 0, d_sb_block_sz + 1);


    src_file.seekg(0, ios::beg);
    src_file >> noskipws;
    while(!src_file.eof()){
        src_file.read(buf+bpos, bexp_rsize);
        rsize = src_file.gcount();

        if ( (block_len + bpos + rsize) < d_sb_block_sz ){
            break;
        }//last block

        head = 0;
        tail = bpos + rsize;

        while ( (head + d_sb_block_sz) <= tail){
            memset(win_buf, 0, d_sb_block_sz+1);
            memcpy(win_buf, buf+head, d_sb_block_sz);
            win_hkey = (block_len == 0) ? adler32(win_buf, d_sb_block_sz) :
                adler32_rolling(win_hkey, d_sb_block_sz, adler_pre_char, win_buf[d_sb_block_sz-1]);

            uint2str(win_hkey, csumstr);

            int cmpflag = 0;
            /*cmpflag == 0 : sliding block win_buf's checksum, md5  are not identical to the existed chunk items
              cmpflag == 1 : sliding block win_buf's checksum Í¬, md5²»Í¬
              cmpflag == 2 : sliding block win_buf's checksum, md5 are identical to ...
            */
            if (d_htab_sb_csum->contain(csumstr)){
                cmpflag = 1;

                MD5::message_digest_func(win_buf, d_sb_block_sz, win_md5val);
                if (d_htab_bindex->contain(win_md5val)){
                    cmpflag = 2;

                    if (block_len != 0){ // insert data fragment in block_buf before inserting the slding block
                        MD5::message_digest_func(block_buf, block_len, block_md5val);
                        ret = register_block(block_buf, block_len, block_md5val,
                                ldata_file, bdata_file, blocks_count, meta_cap, metadata);
                        if (0 != ret){
                            fprintf(stderr, "Error: register data fragment block_buf with size=%d in Dedupe::chunk_sb(...)\n", block_len);
                            goto _CHUNK_SB_EXIT;
                        }
                    }

                    //insert sliding block win_buf with fixed size d_sb_block_sz;
                    ret = register_block(win_buf, d_sb_block_sz, win_md5val,
                            ldata_file, bdata_file, blocks_count, meta_cap, metadata);
                    if (0 != ret){
                        fprintf(stderr, "Error: register sliding block with size=%d in Dedupe::chunk_sb(...)\n", d_sb_block_sz);
                        goto _CHUNK_SB_EXIT;
                    }

                    head += d_sb_block_sz;
                    block_len = 0;
                    memset(block_buf, 0, d_sb_block_sz + 1);//!0515
                }

            }

            if (2 != cmpflag){
                block_buf[block_len++] = buf[head++];

                //insert the fixed size block
                if (block_len == d_sb_block_sz){
                    win_hkey = adler32(block_buf, block_len);
                    uint2str(win_hkey, csumstr);
                    d_htab_sb_csum->insert(csumstr, (void *)"1", 1);

                    MD5::message_digest_func(block_buf, block_len, block_md5val);
                    ret = register_block(block_buf, block_len, block_md5val,
                            ldata_file, bdata_file, blocks_count, meta_cap, metadata);
                    if (0 != ret){
                        fprintf(stderr, "Error: register new sliding block with fixed size=%d in Dedupe::chunk_sb(...)\n", block_len);
                        goto _CHUNK_SB_EXIT;
                    }
                    block_len = 0;
                    memset(block_buf, 0, d_sb_block_sz + 1);
                }
            }

            adler_pre_char = buf[head-1];
        }

        //read expected data from source file to fill up buf
        bpos = tail - head;
        bexp_rsize = buf_size - bpos;
        adler_pre_char = buf[head - 1];
        memmove(buf, buf+head, bpos);
        rsize = 0;

    }

    //last chunk
    last_block_len = block_len + bpos + rsize;
    if (last_block_len > 0){
        memcpy(last_block, block_buf, block_len);
        memcpy(last_block+block_len, buf, bpos+rsize);
    }

_CHUNK_SB_EXIT:
    if (buf){
        free(buf);
        buf = 0;
    }
    if (win_buf){
        free(win_buf);
        win_buf = 0;
    }
    if (block_buf){
        free(block_buf);
        block_buf = 0;
    }
    return ret;
}

int Dedupe::chunk_aac(ifstream& src_file, fstream &ldata_file, fstream &bdata_file,
        unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata,
        unsigned int &last_block_len, char *last_block)
{
    if (!src_file.is_open() || !ldata_file.is_open() || !bdata_file.is_open()){
        fprintf(stderr, "Error: source file, ldata, bdata or mdata not open in Dedupe::chun_sb(...)\n");
        return -1;
    }

    //metadata : block id list <bid1, bid2, bidn>
    blocks_count = 0;
    last_block_len = 0;

    int ret = 0, fret = 0;
    string ftype;
    unsigned int src_size = 0;
    src_file.seekg(0, ios::end);
    src_size = src_file.tellg();

    if (src_size > 10){
    fret = FileType::get_file_type(src_file, ftype);
    src_file.seekg(0, ios::beg);
    src_file >> noskipws;
    }

    if (fret == 0){//static files
        cout << "Info: static files, type is " << ftype <<  ", use FSP chunking" << endl;
        ret = chunk_fsp(src_file, ldata_file, bdata_file, blocks_count, meta_cap, metadata, last_block_len, last_block);
    }else if (fret == -2){ // not contain, dynamic files
        cout << "Info: dynamic files, use CDC chunking" << endl;
        ret = chunk_cdc(src_file, ldata_file, bdata_file, blocks_count, meta_cap, metadata, last_block_len, last_block);
    }else{
        fprintf(stderr, "Error: get file type in Dedupe::chunk_aac(...)\n");
        ret = -1; //
    }

    if (src_file.is_open()){
        src_file.close();
    }
    return ret;
}


int Dedupe::register_block(char *block_buf, unsigned int block_len, unsigned char *md5val,
            fstream &ldata_file, fstream &bdata_file,
            unsigned int &blocks_count, unsigned int &meta_cap, block_id_t * &metadata)
{
    D_Logic_Block_Entry lbentry;
    block_id_t *bid_list = 0;
    int value_sz = 0;
    bid_list = (block_id_t *)d_htab_bindex->getvalue(md5val, value_sz);
    unsigned int reg_block_id = 0;
    //old block
    bool is_new_block = true;
    int ret = 0;
    if (0 != bid_list){
        for(unsigned int i = 0; i < *bid_list; i++){
            ret = blocks_cmp(block_buf, block_len, ldata_file, bdata_file, bid_list[i+1]);
            if (0 == ret){
                reg_block_id = bid_list[i+1];
                is_new_block = false;
                break;
            }else if (-1 == ret){
                fprintf(stderr, "Error: compare blocks in Dedupe::register_block::blocks_cmp(...)\n");
                return -1;
            }
        }

    }
    if (is_new_block){
        bid_list = (0 == value_sz) ? (block_id_t *)malloc(BLOCK_ID_SIZE * 2) :
            (block_id_t *)realloc(bid_list, value_sz + BLOCK_ID_SIZE);
        if (0 == bid_list){
            fprintf(stderr, "Error: malloc/realloc block id list in Dedupe::register_block(...)");
            return -1;
        }
        *bid_list = (0 == value_sz) ? 1 : (*bid_list + 1);
        bid_list[*bid_list] = d_pkg_hdr.ublocks_nr;
        reg_block_id = d_pkg_hdr.ublocks_nr;

        memcpy(lbentry.block_md5, md5val, 33);
        lbentry.ublock_len = block_len;
        lbentry.ublock_off = d_pkg_hdr.ldata_offset;


        value_sz = BLOCK_ID_SIZE * (*bid_list + 1);
        d_htab_bindex->insert(md5val, bid_list, value_sz);
        if (bid_list){
            free(bid_list);
            bid_list = 0;
        }
        ldata_file.seekp(0, ios::end);
        ldata_file.write((const char *)(&lbentry), D_LOGIC_BLOCK_ENTRY_SZ);

        bdata_file.seekp(0, ios::end);
        bdata_file.write((const char *)block_buf, block_len);
        d_pkg_hdr.ublocks_nr++;
        d_pkg_hdr.ublocks_len += block_len;
        d_pkg_hdr.ldata_offset += block_len;
    }

    if ( (blocks_count + 1) >= meta_cap){
        metadata = (block_id_t *)realloc(metadata, BLOCK_ID_SIZE * (meta_cap + BLOCK_ID_ALLOC_INC));
        if (0 == metadata){
            fprintf(stderr, "Error: realloc metadata in Dedupe::register_block(...)\n");
            return -1;
        }
    }
    metadata[blocks_count] = reg_block_id;
    blocks_count++;
    return 0;
}

//same block : return 0;
//different blocks : return 1;
//error: return -1;
int Dedupe::blocks_cmp(char *buf, unsigned int len,
    fstream &ldata_file, fstream &bdata_file, unsigned int block_id)
{
    if (!ldata_file.is_open() || !bdata_file.is_open()){
        fprintf(stderr, "Error: lata file or bdata file not open in Dedupe::register_block::blocks_cmp(...)\n");
        return -1;
    }

    int ret = 0;
    D_Logic_Block_Entry lbentry;
    ldata_file >> noskipws;
    bdata_file >> noskipws;
    char *block_buf = 0;
    unsigned int rsize = 0;
    unsigned long long offset = block_id * D_LOGIC_BLOCK_ENTRY_SZ;

    ldata_file.seekg(offset, ios::beg);
    ldata_file.read((char*)(&lbentry), D_LOGIC_BLOCK_ENTRY_SZ);
    rsize = ldata_file.gcount();
    if (D_LOGIC_BLOCK_ENTRY_SZ != rsize){
        fprintf(stderr, "Error: read logic block with id=%d in Dedupe::register_block::blocks_cmp(..)\n", block_id);
        ret = -1;
        goto _BLOCKS_CMP_EXIT;
    }

    if(lbentry.ublock_len != len){
        ret = 1;
        goto _BLOCKS_CMP_EXIT;
    }

    block_buf = (char *)malloc(lbentry.ublock_len);
    if (0 == block_buf){
        fprintf(stderr, "Error: malloc block buf with size %d in Dedupe::register_block::block_cmp(...)\n", lbentry.ublock_len);
        ret = -1;
        goto _BLOCKS_CMP_EXIT;
    }
    bdata_file.seekg(lbentry.ublock_off, ios::beg);
    bdata_file.read(block_buf, lbentry.ublock_len);
    rsize = bdata_file.gcount();
    if (rsize != lbentry.ublock_len){
        fprintf(stderr, "Error: read block data in Dedupe::register_block::block_cmp(...)\n");
        ret = -1;
        goto _BLOCKS_CMP_EXIT;
    }
    if (0 == memcmp(buf, block_buf, lbentry.ublock_len)){
        ret = 0;
    }else
        ret = 1;

_BLOCKS_CMP_EXIT:
    if (block_buf){
        free(block_buf);
        block_buf = 0;
    }
    if (ldata_file.is_open()){
        ldata_file.seekg(0, ios::beg);
    }
    if (bdata_file.is_open()){
        bdata_file.seekg(0, ios::beg);
    }
    return ret;
}

int Dedupe::extract_all_files(const char *pkg_name, int files_nr, char **files_extract, char *dest_dir)
{
    ifstream pkg_file;
    unsigned int rsize = 0;

    pkg_file.open(pkg_name, ios::binary | ios::in);
    if (!pkg_file.is_open()){
        fprintf(stderr, "Error: open package \"%s\" in Dedupe::extract_all_files(...)\n", pkg_name);
        return -1;
    }
    pkg_file >> noskipws;

    D_Package_Header pkg_hdr;
    pkg_file.read((char *)(&pkg_hdr), D_PKG_HDR_SZ);
    rsize = pkg_file.gcount();
    if (D_PKG_HDR_SZ != rsize){
        fprintf(stderr, "Error: read package header in Dedupe::extract_all_files(...)\n");
        pkg_file.close();
        return -1;
    }
    if (DEDUP_MAGIC_NUM != pkg_hdr.magic_nr){
        fprintf(stderr, "Error: wrong package magic number %d of \"%s\" in Dedupe::extract_all_files(...)\n", pkg_hdr.magic_nr, pkg_name);
        pkg_file.close();
        return -1;
    }
    memcpy(&d_pkg_hdr, &pkg_hdr, D_PKG_HDR_SZ);

    D_File_Entry fentry;
    int ret = 0;
    unsigned long long offset = d_pkg_hdr.mdata_offset;
    char filename[PATH_MAX_LEN] = {0};
    for(unsigned int i = 0; i < d_pkg_hdr.files_nr; i++){
        pkg_file.seekg(offset, ios::beg);
        pkg_file.read((char*)(&fentry), D_FILE_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_FILE_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read the %dth file entry in Dedupe::extract_all_files(...)\n", i);
            ret = -1;
            break;
        }

        if (0 == files_nr){ //extract all files
            ret = extract_file(pkg_file, fentry, dest_dir);
            if (0 != ret)
                break;
        }else { //extract files in the files list -- files_extract
            memset(filename, 0, PATH_MAX_LEN);
            pkg_file.read(filename, fentry.fname_len);
            rsize = pkg_file.gcount();
            if (rsize != fentry.fname_len){
                fprintf(stderr, "Error: read file name of file entry %d in Dedupe::extract_all_files(...)\n", i);
                ret = -1;
                break;
            }
            pkg_file.seekg(offset + D_FILE_ENTRY_SZ, ios::beg);
            if (is_file_in_list(filename, files_nr, files_extract)){
                ret = extract_file(pkg_file, fentry, dest_dir);
                if (0 != ret){
                    break;
                }
            }
        }

        offset += fentry.fentry_sz;
    }
    if (pkg_file.is_open()){
        pkg_file.close();
    }

    return ret;
}


int Dedupe::extract_file(ifstream &pkg_file, D_File_Entry fentry, char *dest_dir)
{
    if (!pkg_file.is_open()){
        fprintf(stderr, "Error: package file not open in Dedupe::extract_file(...)\n");
        return -1;
    }

    char filename[PATH_MAX_LEN] = {0};
    char fullpath[PATH_MAX_LEN] = {0};
    block_id_t *metadata = 0;
    char *last_block = 0;
    D_Logic_Block_Entry lbentry;
    struct utimbuf ftime;

    char *buf = 0;
    unsigned int rsize = 0;
    fstream des_file;

    metadata = (block_id_t *)malloc(BLOCK_ID_SIZE * fentry.fblocks_nr);
    if (0 == metadata){
        fprintf(stderr, "Error: malloc metadata in Dedupe::extract_file(...)\n");
        return -1;
    }
    memset(metadata, 0, BLOCK_ID_SIZE * fentry.fblocks_nr);

    int ret = 0;
    buf = (char *)malloc(BUF_MAX_SIZE);
    if (0 == buf){
        fprintf(stderr, "Error: malloc buf in Dedupe::extract_file(...)\n");
        ret = -1;
        goto _EXTRACT_FILE_EXIT;
    }
    memset(buf, 0, BUF_MAX_SIZE);

    last_block = (char *)malloc(BUF_MAX_SIZE);
    if (0 == last_block){
        fprintf(stderr, "Error: malloc last_block in Dedupe::extract_file(...)\n");
        ret = -1;
        goto _EXTRACT_FILE_EXIT;
    }
    memset(last_block, 0, BUF_MAX_SIZE);

    pkg_file.read(filename, fentry.fname_len);
    rsize = pkg_file.gcount();
    if (rsize != fentry.fname_len){
        fprintf(stderr, "Error: read file name from package in Dedupe::extract_file(...)\n");
        ret = -1;
        goto _EXTRACT_FILE_EXIT;
    }

    pkg_file.read((char *)metadata, fentry.fblocks_nr * BLOCK_ID_SIZE);
    rsize = pkg_file.gcount();
    if (rsize != fentry.fblocks_nr * BLOCK_ID_SIZE){
        fprintf(stderr, "Error: read file metadata (blocks id list) in Dedupe::extract\n");
        ret = -1;
        goto _EXTRACT_FILE_EXIT;
    }

    pkg_file.read(last_block, fentry.last_block_sz);
    rsize = pkg_file.gcount();
    if (rsize != fentry.last_block_sz){
        fprintf(stderr, "Error: read last block in Dedupe::extract_file(...)\n");
        ret = -1;
        goto _EXTRACT_FILE_EXIT;
    }

    prepare_target_file(filename, dest_dir, fullpath);
    des_file.open(fullpath, ios::out | ios::binary);
    if (!des_file.is_open()){
        fprintf(stderr, "Error: create destination file %s in Dedupe::extract_file(...)\n", fullpath);
        ret = -1;
        goto _EXTRACT_FILE_EXIT;
    }

    if(verbose)
        cout << "Info: extract file's path is " << fullpath << endl;

    for(unsigned int i = 0; i < fentry.fblocks_nr; i++){
        unsigned long long offset = 0;
        offset = d_pkg_hdr.ldata_offset + metadata[i] * D_LOGIC_BLOCK_ENTRY_SZ;
        pkg_file.seekg(offset, ios::beg);
        pkg_file.read((char*)(&lbentry), D_LOGIC_BLOCK_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_LOGIC_BLOCK_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read %dth logic block with id=%d in Dedupe::extract_file(...)\n", i, metadata[i]);
            ret = -1;
            goto _EXTRACT_FILE_EXIT;
        }

        offset = lbentry.ublock_off;
        pkg_file.seekg(offset, ios::beg);
        pkg_file.read(buf, lbentry.ublock_len);
        rsize = pkg_file.gcount();
        if (rsize != lbentry.ublock_len){
            fprintf(stderr, "Error: read %dth ublock data with id=%d in Dedupe::extract_file(...)\n", i, metadata[i]);
            ret = -1;
            goto _EXTRACT_FILE_EXIT;
        }

        des_file.write(buf, rsize);
    }
    des_file.write(last_block, fentry.last_block_sz);
    des_file.close();
    ftime.actime = fentry.atime;
    ftime.modtime = fentry.mtime;
    utime(fullpath, &ftime);

_EXTRACT_FILE_EXIT:
    if (buf){
        free(buf);
        buf = 0;
    }
    if (metadata){
        free(metadata);
        metadata = 0;
    }
    if (last_block){
        free(last_block);
        last_block = 0;
    }
    if (des_file.is_open()){
        des_file.close();
    }
    return ret;
}


int Dedupe::package_stat(const char *pkg_name)
{
    fstream pkg_file;
    D_Package_Header pkg_hdr;
    D_File_Entry fentry;
    D_Logic_Block_Entry lbentry;
    int ret = 0;
    unsigned int rsize = 0;
    struct stat stat_buf;
    unsigned long long offset = 0;
    unsigned long long total_files_sz = 0;
    unsigned long long last_blocks_sz = 0;
    unsigned long long dup_blocks_sz = 0;
    unsigned int dup_blocks_nr = 0;
    unsigned long long saved_bytes = 0;
    unsigned long long pkg_size = 0;
    unsigned long long system_overhead = 0;
    block_id_t  *metadata = 0;
    block_id_t *lblock_array = 0;

    ret = stat(pkg_name, &stat_buf);
    if (0 != ret){
        switch (errno){
        case ENOENT:
            fprintf(stderr, "Error: deduped package %s not exist in Dedupe::package_stat(...)\n", pkg_name);
            return -1;
        default:
            fprintf(stderr, "Error: stat deduped package %s in Dedupe::package_stat(...)\n", pkg_name);
            return -1;
        }
    }

    pkg_file.open(pkg_name, ios::binary | ios::in);
    if (!pkg_file.is_open()){
        fprintf(stderr, "Error: open deduped package %s in Dedupe::package_stat(...)\n", pkg_name);
        return -1;
    }
    pkg_file.seekg(0, ios::beg);
    pkg_file >> noskipws;
    pkg_file.read((char *)(&pkg_hdr), D_PKG_HDR_SZ);
    rsize = pkg_file.gcount();
    if (D_PKG_HDR_SZ != rsize){
        fprintf(stderr, "Error: read deduped package %s in Dedupe::package_stat(...)\n", pkg_name);
        ret = -1;
        goto _PACKAGE_STAT_EXIT;
    }
    if (DEDUP_MAGIC_NUM != pkg_hdr.magic_nr){
        fprintf(stderr, "Error: wrong deduped package magic number in Dedupe::package_stat(...)\n");
        ret = -1;
        goto _PACKAGE_STAT_EXIT;
    }

    lblock_array = (block_id_t *)malloc(BLOCK_ID_SIZE * pkg_hdr.ublocks_nr);
    if (0 == lblock_array){
        fprintf(stderr, "Error: malloc logic block id array in Dedupe::package_stat(...)\n");
        ret = -1;
        goto _PACKAGE_STAT_EXIT;
    }
    memset(lblock_array, 0, BLOCK_ID_SIZE * pkg_hdr.ublocks_nr);

    offset = pkg_hdr.mdata_offset;
    for (unsigned int i = 0; i < pkg_hdr.files_nr; i++){
        memset(&fentry, 0, D_FILE_ENTRY_SZ);
        pkg_file.seekg(offset, ios::beg);
        pkg_file.read((char *)(&fentry), D_FILE_ENTRY_SZ);
        rsize = pkg_file.gcount();

        if (D_FILE_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read %dth file entry in Dedupe::package_stat(...)\n", i);
            ret = -1;
            goto _PACKAGE_STAT_EXIT;
        }
        last_blocks_sz += fentry.last_block_sz;
        total_files_sz += fentry.org_file_sz;

        metadata = (block_id_t *)malloc(BLOCK_ID_SIZE * fentry.fblocks_nr);
        if (0 == metadata){
            fprintf(stderr, "Error: malloc metadata for file entry %d in Dedupe::package_stat(...)\n", i);
            ret = -1;
            goto _PACKAGE_STAT_EXIT;
        }
        memset(metadata, 0, BLOCK_ID_SIZE * fentry.fblocks_nr);

        pkg_file.seekg(offset + D_FILE_ENTRY_SZ + fentry.fname_len, ios::beg);
        pkg_file.read((char *)metadata, BLOCK_ID_SIZE * fentry.fblocks_nr);
        rsize = pkg_file.gcount();
        if (rsize != BLOCK_ID_SIZE * fentry.fblocks_nr){
            fprintf(stderr, "Error: read metadata of file entry %d in Dedupe::package_stat(...)\n", i);
            ret = -1;
            goto _PACKAGE_STAT_EXIT;
        }
        for(unsigned int j = 0; j < fentry.fblocks_nr; j++){
            if (metadata[j] >= pkg_hdr.ublocks_nr){
                fprintf(stderr, "Error: metadata[%d]=%d > unique blocks number %d in Dedupe::package_stat(...)\n", j, metadata[j], pkg_hdr.ublocks_nr);
                ret = -1;
                goto _PACKAGE_STAT_EXIT;
            }
            lblock_array[ metadata[j] ]++;
        }
        if (metadata){
            free(metadata);
            metadata = 0;
        }
        offset += fentry.fentry_sz;
    }

    /*traverse logic blocks to get dup_block_sz*/
    pkg_file.seekg(pkg_hdr.ldata_offset, ios::beg);
    for(unsigned int i = 0; i < pkg_hdr.ublocks_nr; i++){
        if (lblock_array[i] > 1){
            dup_blocks_nr++;
        }
        memset(&lbentry, 0, D_LOGIC_BLOCK_ENTRY_SZ);
        pkg_file.read((char*)(&lbentry), D_LOGIC_BLOCK_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_LOGIC_BLOCK_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read %dth logic block entry in Dedupe::package_stat(...)\n", i);
            ret = -1;
            goto _PACKAGE_STAT_EXIT;
        }
        dup_blocks_sz += (lblock_array[i] * lbentry.ublock_len);
        if (lblock_array[i] > 1){
            saved_bytes += (lblock_array[i] - 1) * lbentry.ublock_len;
        }
    }
    pkg_file.seekg(0, ios::end);
    pkg_size = pkg_file.tellg();
    pkg_file.close();

    system_overhead = pkg_size - pkg_hdr.ublocks_len - last_blocks_sz;

    show_pkg_header(pkg_name);
    /*show package statistical info */
    cout << endl;
    cout << "------------------In Dedupe::package_stat----------------------" << endl;
    cout << "1. number of duplicated blocks:           " << dup_blocks_nr << endl;
    cout << "2. total size of original file system:    " << (unsigned long long)total_files_sz << endl;
    cout << "   total size of all original files:      " << (unsigned long long)(dup_blocks_sz + last_blocks_sz) << endl;
    cout << "3. saved bytes calculated by pkg_hdr:     " << total_files_sz - pkg_hdr.ublocks_len - last_blocks_sz << endl;
    cout << "   saved bytes via traversing:            " << saved_bytes << endl;
    cout << "4. size of the deduped system(stat):      " << (unsigned long)stat_buf.st_size << endl;
    cout << "   size of the deduped system(seek):      " << (unsigned long long)pkg_size << endl;
    cout << "5_0. costs of storing md5:                " << pkg_hdr.ublocks_nr * 36 << endl;
    cout << "5. costs of logic block entry:            " << pkg_hdr.ublocks_nr * D_LOGIC_BLOCK_ENTRY_SZ << endl;
    cout << "6. costs of file metadata:               " << (unsigned long long)(pkg_size - pkg_hdr.mdata_offset - last_blocks_sz)<< endl;
    cout << "7. saved bytes / org_file_size:          " << (double)(1.0*saved_bytes/total_files_sz *100.0) << "%"<<endl;
    cout << endl;
    cout << "-----------Info of Origninal File System and DDE (bytes)----------" << endl;
    cout << "0. Files number:                                  " << (unsigned int)pkg_hdr.files_nr << endl;
    cout << "1. overall size of Orginal File System:           " << (unsigned long long )total_files_sz << endl;
    cout << "2. overall size of DDE system:                    " << (unsigned long long )pkg_size << endl;
    cout << "   where in DDE system, " << endl;
    cout << "    (1) length of blocks stored in DDE:           " << (unsigned long long )(pkg_hdr.ublocks_len + last_blocks_sz) << endl;
    cout << "        where, length of last blocks:             " << (unsigned long long )last_blocks_sz << endl;
    cout << "               length of unique blocks:           " << (unsigned long long )pkg_hdr.ublocks_len << endl;
    cout << "                 1)number of unique blocks:       " << (unsigned int)pkg_hdr.ublocks_nr << endl;
    cout << "                 2)thus, ublocks average size:    " << (double)(1.0 * pkg_hdr.ublocks_len / pkg_hdr.ublocks_nr) << endl;
    cout << "    (2) overhead(Header/logic block/metadata):    " << (unsigned long long )system_overhead << endl;
    cout << "        overhead/orginal files' size(%):          " << (double)(1.0*system_overhead / total_files_sz*100.0) << "%" << endl;

    cout << "Therfore, "  << endl;
    cout << "   (1) duplicates detected(bytes):                " << (unsigned long long )saved_bytes << endl;
    cout << "       duplicates/original files'size(%):         " << (double)(1.0*saved_bytes/total_files_sz *100.0) << "%"<<endl;
    cout << "   (2) net storage saved = duplicates - overhead: " << (unsigned long long )(saved_bytes - system_overhead) << endl;
    cout << "       net_saved/org_files' size(%):              " << (double)(1.0*(saved_bytes-system_overhead)/total_files_sz * 100.0) << "%" <<endl;
    cout << endl;

_PACKAGE_STAT_EXIT:
    if (pkg_file.is_open())
        pkg_file.close();
    if (metadata){
        free(metadata);
        metadata = 0;
    }
    if (lblock_array){
        free(lblock_array);
        lblock_array = 0;
    }
    return ret;
}

int Dedupe::show_package_files(const char *pkg_name)
{
    int ret = 0;
    D_Package_Header pkg_hdr;
    D_File_Entry fentry;
    unsigned long long offset;
    char pathname[PATH_MAX_LEN] = {0};
    unsigned rsize = 0;

    cout << "--------------------In Dedupe::show_package_files-----------------" << endl;
    fstream pkg_file;
    pkg_file.open(pkg_name, ios::binary | ios::in);
    if (!pkg_file.is_open()){
        fprintf(stderr, "Error: open deduped package %s in Dedupe::show_package_files(...)\n", pkg_name);
        return -1;
    }

    pkg_file >> noskipws;
    pkg_file.seekg(0, ios::beg);
    pkg_file.read((char*)(&pkg_hdr), D_PKG_HDR_SZ);
    rsize = pkg_file.gcount();
    if (D_PKG_HDR_SZ != rsize){
        fprintf(stderr, "Error: read package header in Dedupe::show_package_files(...)\n");
        ret = -1;
        goto _SHOW_PACKAGE_FILES_EXIT;
    }
    if (DEDUP_MAGIC_NUM != pkg_hdr.magic_nr ){
        fprintf(stderr, "Error: wrong package magic number in Dedupe::show_package_files(...)\n");
        ret = -1;
        goto _SHOW_PACKAGE_FILES_EXIT;
    }

    offset = pkg_hdr.mdata_offset;
    for(unsigned int i = 0; i < pkg_hdr.files_nr; i++){
        pkg_file.seekg(offset, ios::beg);
        pkg_file.read((char*)(&fentry),D_FILE_ENTRY_SZ);
        rsize = pkg_file.gcount();
        if (D_FILE_ENTRY_SZ != rsize){
            fprintf(stderr, "Error: read %dth file entry in Dedupe::show_package_files(...)\n", i);
            ret = -1;
            goto _SHOW_PACKAGE_FILES_EXIT;
        }

        memset(pathname, 0, PATH_MAX_LEN);
        pkg_file.read(pathname, fentry.fname_len);
        rsize = pkg_file.gcount();
        if (rsize != fentry.fname_len){
            fprintf(stderr, "Error: read pathname of %dth file entry in Dedupe::show_package_files(...)\n", i);
            ret = -1;
            goto _SHOW_PACKAGE_FILES_EXIT;
        }
        fprintf(stderr, "%d.%s\n", i+1, pathname);
        offset += fentry.fentry_sz;
    }


_SHOW_PACKAGE_FILES_EXIT:
    if (pkg_file.is_open()){
        pkg_file.close();
    }
    return ret;
}

int Dedupe::set_chunk_alg(const char *cname)
{
    int ret = 0;
    if(0 == strcmp(cname, CHUNK_FSP_NAME))
        d_chunk_alg = D_CHUNK_FSP;
    else if (0 == strcmp(cname, CHUNCK_CDC_NAME)){
        d_chunk_alg = D_CHUNK_CDC;
    }
    else if (0 == strcmp(cname, CHUNCK_SB_NAME)){
        d_chunk_alg = D_CHUNK_SB;
        if (0 == d_htab_sb_csum){
            char tabname[PATH_MAX_LEN] = {0};
            char bloomname[PATH_MAX_LEN]  = {0};
            sprintf(tabname, "data/BigHashTable/.hashdb_sbcsum_%d.db", getpid());
            sprintf(bloomname, "data/BigHashTable/.hashdb_sbcsum_%d.bf", getpid());
            d_htab_sb_csum = new BigHashTable(tabname, bloomname);
        }
    }else if (0 == strcmp(cname, CHUNCK_AAC_NAME)){
        d_chunk_alg = D_CHUNK_AAC;
    }else{
        fprintf(stderr, "Error: wrong chunking name %s in Dedupe::set_chunk_alg(...)\n", cname);
        fprintf(stderr, "Usage: int set_chunk_alg(const char *name)\n");
        fprintf(stderr, ".....      <name> : \"FSP\", \"CDC\", \"SB\", \"AAC\" \n");
        ret = -1;
    }

    return ret;
}

#define DEDUPLICATION_TEST
#ifdef  DEDUPLICATION_TEST
#include <iostream>
using namespace std;
int main()
{
    char *files_extract[] = {"Download/Cracking_the_Coding_Interview.pdf", "lbfs_des_2.pdf"};
    char *src_files[] = {"nachos-3.4"};
    char *rm_files[] = {"Download/author.pdf", "Download/Resume.pdf", "lbfs.pdf", "Download/author.pdf"};
    const char *pkg_name = "data/dedup_fsp_nachos.ded";
    Dedupe dp(true);

    dp.create_package(pkg_name);
    dp.set_chunk_alg("CDC");
    dp.set_cdc_hashfun("APHash"); //Adler, APHash,SDBMHash, DJBHash, DJB2Hash, DEKHash, CRCHash
    dp.show_pkg_header(pkg_name);
    dp.insert_files(pkg_name, 1, src_files);

 //   dp.show_pkg_header(pkg_name);
 //   dp.remove_files(pkg_name, 2, rm_files);
 //   dp.extract_all_files(pkg_name, 0, 0, "unded");
    dp.show_pkg_header(pkg_name);
    dp.package_stat(pkg_name);
//    dp.show_package_files(pkg_name);

   // dp.set_chunk_alg("CDC");
   // dp.set_cdc_hashfun("AdlerHash");
 //  dp.create_package("data/dedup_cdc_adler.ded");
    //dp.show_pkg_header("data/dedup_cdc_adler.ded");
  //  dp.insert_files("data/dedup_cdc_adler.ded", 0, src_files);
 //   dp.show_pkg_header("data/dedup_cdc_adler.ded");
  //  dp.extract_all_files("data/dedup_cdc_adler.ded", 0, 0, "J:/undedupe_cdc_adler");
   // dp.package_stat("data/dedup_cdc_adler.ded");

  //  dp.show_package_files("data/dedup_cdc_adler.ded");
 //   dp.insert_files("data/dedup.ded", 2, src_files);
  //  dp.show_pkg_header("data/dedup.ded");
  //  dp.extract_all_files("data/dedup.ded", 2, files_extract, "J:/Undedup_Spe");
  //  dp.package_stat("data/dedup.ded");
  //  dp.show_package_files("data/dedup.ded");
    return 0;
}
#endif // DEDUPLICATION_TEST
