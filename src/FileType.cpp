/*
Copyright (c) <2016> <Cuiting Shi>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "FileType.h"

const map<string, string> FileType::file_map = FileType::create_file_map();


void FileType::get_file_ext(const char *filename, char * &fileext)
{
    int i = strlen(filename) - 1;
    while(i >= 0 && filename[i] != '.') --i;
    ++i;
    if (i != 0)
        fileext = strdup(filename+i);
}

int FileType::get_file_type(std::ifstream &file,std::string &type)
{
    if (!file.is_open()){
        fprintf(stderr, "Error: file not open in FileType::get_file_type\n");
        return -1;
    }
    char header[8] = {0};
    unsigned int rsize = 0;

    file.seekg(0, ios::beg);
    file >> noskipws;
    file.read(header, 8);
    rsize = file.gcount();
    if (rsize != 8){
        fprintf(stderr, "Error: read file header in FileType::get_file_type\n");
        file.seekg(0, ios::beg);
        return -1;
    }
    file.seekg(0, ios::beg);

    std::map<string, string>::const_iterator cit;
    for (unsigned int i = 8; i >= 2; i--){
        string hdr_str = bytesToHexString(header, i);
    //    fprintf(stderr, "Info: %d bytes, hex value: %s", i, hdr_str.c_str());
        cit  = file_map.find(hdr_str);
        if (cit != file_map.end()){
            type = cit->second;
            return 0;
        }
    }

    return -2; //not found
}

std::map<std::string, std::string> FileType::create_file_map()
{
    std::map<std::string, std::string> m;
    m["FFD8FF"] = "jpg";
    m["89504E470D0A1A0A"] = "png";
    m["425A68"] = "gif";
    m["474946383761"] = "gif";
    m["474946383961"] = "gif";
    m["49492A00"] = "tif";
    m["424D"] = "bmp";
    m["00000100"] = "ico";

    m["52494646"] = "video";
    m["57415645"] = "wav"; //end
    m["41564920"] = "avi"; //end
    m["FFF1"] = "aac"; //MPEG-4 Advanced Audio Encoding

    m["FFFB"] = "mp3";
    m["494443"] = "mp3";
    m["4344303031"] = "iso";
    m["4B444D"] = "vmdk";
    m["4D5A"] = "exe";
    m["7F454C46"] = "elf";
    //	m["D0CF11E0A1B11AE1"] = "microsoft office documement";
    //	m["25215053"] = "ps";
    m["25504446"] = "pdf";
    m["CAFEBABE"] = "java class";
    //	m["504B0304"] = "zip"; // and other file formats based on it, jar, odf, docx,pptx, vsdx, apk
    //	m["526172211A0700"] = "rar";
    //	m["526172211A070100"] = "rar";
    //	m["1F8B08"] = "gz";
    //	m["1F9D"] = "z"; //z or tar.z based on LZW algorithm
    //	m["1FA0"] = "z"; //z or tar.z based on LZH algorithm
    //	m["377ABCAF271C"] = "7z";
    //	m["38425053"] = "psd"; //photoshop document

    //	m["FFFFFFFF"] = "sys";
    return m;
}

string bytesToHexString(const char* input, unsigned int len)
{
    const char *HEX_SET = "0123456789ABCDEF";

    string str;
    str.reserve(len << 1);
    for(unsigned int i = 0; i < len; i++){
        unsigned int a = (unsigned int)(input[i] >> 4) & 0xF; // div 16
        unsigned int b = (unsigned int)input[i] & 0xF; // mod 16
        str.append(1, HEX_SET[a]);
        str.append(1, HEX_SET[b]);
    }
    return str;
}


//#define FILE_TYPE_TEST
#ifdef FILE_TYPE_TEST
int main()
{
    char *fileext = 0;
    const char *filename = "E:/BaiduYunDownload/ubuntu-16.04-desktop-amd64.iso";
    //"J:/codes/shellsort.exe";
    //"E:/BaiduYunDownload/riverside.avi";
    //"J:/6大创开展情况汇总表.xls";
	// "J:/ftisland_heaven"; //"J:/2015校庆奖学金.rar"; //"J:/33466.docx"; //"J:/lena.jpg"; //"J:/lbfs.pdf";

	std::fstream file;
    file.open(filename, ios::binary | ios::in);
    string type;
   // bool isdynamic = false;

    int ret = FileType::get_file_type(file, type);
    if (ret == 0)
        cout << filename << " type via file signature: " <<  type << endl;

     FileType::get_file_ext(filename, fileext);
    cout << filename << " file extention: " << fileext << endl;
    if (fileext){
        free(fileext);
        fileext = 0;
    }
}


#endif // FILE_TYPE_TEST
