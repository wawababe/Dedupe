#ifndef FILETYPE_H
#define FILETYPE_H

#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <string.h>
#include <stdlib.h>

using namespace std;

class FileType{
public:
    static int get_file_type(ifstream &file, std::string &type);
    void get_file_ext(const char *filename, char * &fileext);
private:
    static std::map<std::string, std::string> create_file_map();

private:
    static const map<string, string> file_map;

};
string bytesToHexString(const char* input, unsigned int len);

#endif // FILETYPE_H

