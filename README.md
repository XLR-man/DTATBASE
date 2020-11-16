//
// Created by XIE on 2020/11/14.
//

#ifndef LOGGER_KVDBHANDLER_H
#define LOGGER_KVDBHANDLER_H
#include <iostream>
#include<fstream>
#include <map>
#include "Logger.h"
using namespace std;
const int KVDB_OK=0;
const int KVDB_INVALID_AOF_PATH=1;
const int KVDB_INVALID_KEY=2;
const int KVDB_NO_SPACE_LEFT_ON_DEVICES=3;
struct dlen
{
    int klen;
    int vlen;
    dlen(int kl, int vl)
    {
        klen = kl;
        vlen = vl;
    }
};
struct data
{
    string key;
    string value;
    data(string k, string v)
    {
        key = k;
        value = v;
    }
};
void setdata(dlen &d1, data &d2, ofstream &out)
{
    cout << "Now writing the data to the file" << endl;
    out.write((char *)&d1.klen, 4);
    out.write((char *)&d1.vlen, 4);
    out.write((char *)&d2.key[0], d1.klen);
    out.write((char *)&d2.value[0], d1.vlen);
}
class KVDBHandler {
private:
    ofstream out;
    ifstream in;
    string filename;
    Logger *logger1;
public:
    KVDBHandler(const string db_file);
    int set(const string &key, const string &value);
    int get(const string &key, string &value);
    void del(const string &key);
    void purge();
    ~KVDBHandler();
};
KVDBHandler::KVDBHandler(const string db_file)
{
    filename=db_file;
    logger1=new Logger(Logger::file_and_terminal,Logger::debug,"result.log");
}
void KVDBHandler::purge()
{
    in.open(filename,ios::in);
    if(!in)
    {
        cout<<"Error opening the file"<<endl;
        logger1->Warning("void KVDBHandler::purge()---Error opening file");
    }
    map<string,string>mapKV;
    if(in.is_open())
    {
        while(in.peek()!=EOF)
        {
            int kl, vl;
            in.read((char *)&kl, 4);
            in.read((char *)&vl, 4);
            char *tt = new char[kl + 1];
            in.read(tt, kl);
            tt[kl] = '\0';
            string t1=string(tt);
            map<string, string>::iterator it;
            if(vl!=-1)
            {
                it=mapKV.find(t1);
                char * tv = new char[vl + 1];
                in.read(tv, vl);
                tv[vl] = '\0';
                string t2=string(tv);
                if(it!=mapKV.end())     //如果map里没有对应的key值就插入，有就替换
                    mapKV[t1]=t2;
                else
                {
                    mapKV.insert(map<string,string>::value_type(t1,t2));
                }
            }
            else
            {
                it=mapKV.find(t1);
                if(it!=mapKV.end())
                    mapKV.erase(it);
            }
        }
    }
    in.close();
    fstream temp;
    temp.open("temp.txt",ios::out|ios::binary|ios::app);
    if(!temp)
        cout<<"Error opening temp_file"<<endl;
    map<string,string>::iterator it;
    it=mapKV.begin();
    while(it!=mapKV.end())
    {
        int kl,vl;
        kl=it->first.length();
        vl=it->second.length();
        temp.write((char *)&kl,4);
        temp.write((char *)&vl,4);
        temp.write((char *)&it->first[0],kl);
        temp.write((char *)&it->second[0],vl);
        it++;
    }
    temp.close();
    char *file_name=new char[filename.length()+1];
    for(int i=0;i<filename.length()+1;i++)
        file_name[i]=filename[i];
    file_name[filename.length()]='\0';
    if(remove(file_name)==0)
        cout<<"Delete  file success"<<endl;
    else
        cout<<"Delete file fail"<<endl;
    if(rename("temp.txt",file_name)==0)
        cout<<"Rename file success"<<endl;
    else
        cout<<"Rename file fail"<<endl;
    cout<<"Purge operation succeeded"<<endl;
}
int KVDBHandler::set(const string &key, const string &value)
{
    if(key.length()==0)
    {
        logger1->Warning("int KVDBHandler::set()---the key is invalid");
        return KVDB_INVALID_KEY;
    }
    else
    {
        out.open(filename, ios::out | ios::binary |ios::app);
        if (!out)
        {
            cout << "Error opening file" << endl;
        }
        dlen d1(key.length(), value.length());
        data d2(key, value);
        if (out.is_open())
        {
            setdata(d1, d2, out);
        }
        out.flush();//@增加这个语句，将缓冲区的数据立刻写到文件中
        out.close();
        cout<<"Set operation succeeded"<<endl;
        logger1->Info("int KVDBHandler::set()---Set operation succeeded");
        return KVDB_OK;
    }
}
int KVDBHandler::get(const string &key, string &value)
{
    if(key.length()==0)
    {
        logger1->Warning("int KVDBHandler::get()---the key is invalid");
        return KVDB_INVALID_KEY;
    }
    else
    {
        in.open(filename, ios::in);
        if (!in)
            cout << "Error opening file" << endl;
        data d2(key, value);
        if (in.is_open())
        {
            while (in.peek() != EOF)
            {
                int kl, vl;
                in.read((char *)&kl, 4);
                in.read((char *)&vl, 4);
                char *tt = new char[kl + 1];
                in.read(tt, kl);
                tt[kl] = '\0';
                string tt2;
                tt2=string(tt);
                if(vl==-1&&tt2==key)
                {
                    cout<<"The key is deleted"<<endl;
                }
                else if (tt2 == key&&vl!=-1)
                {
                    char * tv = new char[vl + 1];
                    in.read(tv, vl);
                    tv[vl] = '\0';
                    value = string(tv);
                }
                else
                {
                    in.seekg(vl, ios::cur);
                }
            }
        }
        in.close();
        cout<<"Get operation succeeded"<<endl;
        //获取数据，获取文件中最后一个关键词为key的value
        logger1->Info("int KVDBHandler::get()---Get operation succeeded");
        return KVDB_OK;
    }
}
void KVDBHandler::del(const string &key)
{
    dlen d1(key.length(),-1);
    data d2(key,"");
    cout<<"Now ready to del"<<endl;
    out.open(filename, ios::out | ios::binary |ios::app);
    if (!out)
    {
        cout << "Error opening file" << endl;
    }
    out.write((char *)&d1.klen, 4);
    out.write((char *)&d1.vlen, 4);
    out.write((char *)&d2.key[0], d1.klen);
    out.flush();
    out.close();
    cout<<"Delete operation succeeded"<<endl;
}
KVDBHandler::~KVDBHandler()
{
    if(out)
        out.close();
    if(in)
        in.close();
}
#endif //LOGGER_KVDBHANDLER_H
