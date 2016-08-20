#ifndef UTILS_H_INCLUDED
#define UTILS_H_INCLUDED

#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifndef PATH_MAX_LEN
#define PATH_MAX_LEN 255
#endif


//transfer unsigned integer into string str, return str's length
int uint2str(unsigned int x, unsigned char *str);

//check whether the file is in file list
bool is_file_in_list(char *filepath, int files_nr, char **files_list);

//create necessary directories for the target file, open the target file
//return  the file's full path name
int prepare_target_file(const char *filename, const char *basepath, char *fullpath);

/*get file extention from filename*/
void get_file_ext(const char *filename, char * &fileext);

#endif // UTILS_H_INCLUDED
