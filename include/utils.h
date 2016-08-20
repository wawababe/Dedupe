/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
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
