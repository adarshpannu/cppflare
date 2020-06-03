//  main.cpp

#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <vector>
#include <sstream>
#include <algorithm>
#include <iterator>

#include "utils.h"

struct ThreadContext {
public:
    int         thread_id;
    int         parallel_degree;
};

template <typename T, typename U> class MapOp;
template <typename T, typename U> class FlatMapOp;

template <typename T>
class DataOp {
public:
    DataOp(ThreadContext& ctx): _ctx(ctx) {}
    
    virtual ~DataOp() { }
    virtual void Open() { }
    virtual bool HasNext() { return false; }
    
    virtual void Next(T& t) { }
    
    template <typename U>
    auto map(std::function<U(T)>  map_fn) {
        auto   *d = new MapOp<T, U>(_ctx, this, map_fn);
        return d;
    }
    
    template <typename U>
    auto flatMap(std::function<std::vector<U>(T)>flatmap_fn) {
        auto   *d = new FlatMapOp<T, U>(_ctx, this, flatmap_fn);
        return d;
    }
    
protected:
    ThreadContext     _ctx;
};

template <typename T, typename U>
class MapOp : public DataOp<U> {
public:
    using FN_T2U = std::function<U(T)>;
    
    MapOp(ThreadContext& ctx, DataOp<T>* parent, FN_T2U map_fn)
    : DataOp<U>(ctx), _parent(parent), _map_fn(map_fn) {}
    
    virtual bool HasNext() {
        return _parent->HasNext();
    }
    
    virtual void Next(U& u) {
        T    t;
        _parent->Next(t);
        u = _map_fn(t);
    }
    
    virtual void Open() {
        _parent->Open();
    }

    virtual ~MapOp() {
        delete _parent;
    }

private:
    DataOp<T>   *_parent;
    FN_T2U          _map_fn;
};

template <typename T, typename U>
class FlatMapOp : public DataOp<U> {
public:
    using FLATMAP_FN_SIG = std::function<std::vector<U>(T)>;

    FlatMapOp(ThreadContext& ctx, DataOp<T>* parent, FLATMAP_FN_SIG flatmap_fn)
    : DataOp<U>(ctx), _parent(parent), _flatmap_fn(flatmap_fn) {}
    
    virtual bool HasNext() {
        if (_it != _vector.end()) {
            return true;
        } else if (_parent->HasNext()) {
            T    t;
            _parent->Next(t);
            _vector = _flatmap_fn(t);
            _it = _vector.begin();
            return true;
        } else {
            return false;
        }
    }
    
    virtual void Next(U& u) {
        u = *_it++;
    }
    
    virtual void Open() {
        _parent->Open();
        _it = _vector.begin();
    }

    virtual ~FlatMapOp() {
        delete _parent;
    }

private:
    DataOp<T>                           *_parent;
    FLATMAP_FN_SIG                      _flatmap_fn;
    std::vector<T>                      _vector;
    typename std::vector<T>::iterator   _it;
    int                                 _index;
};

class TextFileOp : public DataOp<string> {
public:
    
    TextFileOp(ThreadContext& ctx, string filename) : DataOp(ctx), _filename(filename) {
        struct stat     results;
        string          line;
        
        /* Compute partitions */
        if (::stat(filename.c_str(), &results) == 0) {
            _blk_size = (int) (results.st_size / ctx.parallel_degree);
            _blk_size = (_blk_size < 10) ? 10 : _blk_size;
            ComputeBlockRanges(filename, results.st_size);
        }
    }
    
    ~TextFileOp() {
        cout << "~TextFileOp" << endl;
    }
    
    void Open() {
        _range = _block_offsets[_ctx.thread_id];
        _fp.open(_filename);
        if (! _fp.is_open())
            die("Cannot open file: " + _filename);

        _fp.seekg(_range.first);
    }
    
    bool HasNext() {
        if (_fp.is_open()) {
            if (_fp.tellg() < _range.second)
                return true;
            else {
                _fp.close();
                return false;
            }
        } else {
            return false;
        }
    }

    void Next(string& line) {
        getline(_fp, line);
        //cout << "> " << line << endl;
    }
    
private:
    void    ComputeBlockRanges(string filename, size_t sz) {
        std::ifstream       fp(filename);
        string              line;
        size_t              begin = 0, end = 0;
        
        for (begin = 0; end != sz; begin = end) {
            end = begin + _blk_size;
            if (end > sz)
                end = sz;
            else {
                fp.seekg(end);
                getline(fp, line);
                end += line.size() + 1;
            }
            _block_offsets.push_back(std::make_pair(begin, end));
        }
        fp.close();
        PrintFirstLines();
    }
    
    void PrintFirstLines() {
        for (auto p: _block_offsets) {
            cout << ": (" << p.first << "," << p.second << "] " << endl;
        }
    }
    
private:
    blksize_t           _blk_size;
    vector<Range>       _block_offsets;
    string              _filename;
    std::ifstream       _fp;
    Range               _range;
};

std::vector<string> SplitString(string str) {
    std::stringstream       ss(str);
    std::string             token;
    std::vector<string>     v;
    
    while (std::getline(ss, token, ' ')) {
        cout << "  : " << token << endl;
        v.push_back(token);
    }
    return v;
}


template<typename Functor>
void f(Functor functor)
{
   cout << functor(10) << endl;
}

int g(int x)
{
    return x * x;
}

void FOO()
{
    auto lambda = [] (int x) { return x * 100; };
    f(lambda); //pass lambda
}

int main(int argc, const char * argv[]) {
    std::pair <std::string,double> product1;                     // default constructor
    std::vector<int>  v;
    
    string filename = "/Users/adarshrp/Projects/Flora/small.txt";
        
    ThreadContext   ctx;
    ctx.thread_id = 0;
    ctx.parallel_degree = 6;
    
    using STR2INT = std::function<int(string)>;
    
    STR2INT strlen = [](string s) {
        //return std::make_pair(s, s.length());
        return s.length();
    };
    
    auto strlen2 = [](string s) {
        //return std::make_pair(s, s.length());
        return s.length();
    };
    using STR2VECSTR = std::function<std::vector<string>(string)>;

    STR2VECSTR splitString = [](string str) {
        std::stringstream       ss(str);
        std::string             token;
        std::vector<string>     v;
        
        while (std::getline(ss, token, ' ')) {
            v.push_back(token);
        }
        return v;
    };
    
    auto toUpper = [](string s) { return s.length(); };
    
    using INT2INT = std::function<int(int)>;

    INT2INT negate = [](int i) { return -i; };

    //auto            tf = (new TextFileOp(ctx, filename)) -> map(strlen) -> map(negate);
    auto            tf = (new TextFileOp(ctx, filename)) -> flatMap(splitString);

    int             i;
    string          s;


    cout << ">>> HELLO!!!" << endl;

    tf->Open();
    while (tf->HasNext()) {
        tf->Next(s);
        //cout << "----: " << s << endl;
    }

    delete tf;
}
