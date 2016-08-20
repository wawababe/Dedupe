/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "utils.h"
#ifndef DEBUG_MODE
#define DEBUG_MODE
#endif // DEBUG_MODE


int uint2str(unsigned int x,unsigned char *str)
{
	unsigned int i = 0;
	unsigned int xx = x, t;

	while (xx)
	{
		str[i++] = (xx % 10) + '0';
		xx = xx / 10;
	}
	str[i] = '\0';

	xx = i;
	for (i = 0; i < xx/2; i++)
	{
		t = str[i];
		str[i] = str[xx - i -1];
		str[xx - i -1] = t;
	}

	return xx;
}


bool is_file_in_list(char *filepath, int files_nr, char **files_list)
{
    for (int i = 0; i < files_nr; i++){
        if (0 == strcmp(filepath, files_list[i]))
            return true;
    }
    return false;
}


int prepare_target_file(const char *filename, const char *basepath, char *fullpath)
{
    char path[PATH_MAX_LEN] = {0};
    char *p = 0;
    int pos = 0;
    if (filename[1] == ':')
        sprintf(fullpath, "%s/%s", basepath, filename+2);
    else
        sprintf(fullpath, "%s/%s", basepath, filename);
    p = fullpath;
    while(*p){
        path[pos++] = *p;
        if (*p == '/')
            mkdir(path, 766);
        p++;
    }
    return 0;
}

// get file extention into fileext;
void get_file_ext(const char *filename, char * &fileext)
{
    int i = strlen(filename) - 1;
    while(i >= 0 && filename[i] != '.') --i;
    ++i;
    if (i != 0)
        fileext = strdup(filename+i);
}
