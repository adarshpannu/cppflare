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
template <typename T> class FilterOp;

template <typename T>
class DataOp {
public:
    DataOp(ThreadContext& ctx): _ctx(ctx) {}
    
    T type();

    virtual ~DataOp() { }
    virtual void Open() { }    
    virtual bool Next(T& t) = 0;
    
    template <typename U>
    auto map(std::function<U(T)>  map_fn) {
        auto   *d = new MapOp<T, U>(_ctx, this, map_fn);
        return d;
    }

    auto filter(std::function<bool(T)>  filter_fn) {
        auto   *d = new FilterOp<T>(_ctx, this, filter_fn);
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
    
    virtual void Open() {
        _parent->Open();
    }

    virtual bool Next(U& u) {
        T    t;
        if (_parent->Next(t)) {
            u = _map_fn(t);
            return true;
        } else {
            return false;
        }
    }
    
    virtual ~MapOp() {
        delete _parent;
    }

private:
    DataOp<T>   *_parent;
    FN_T2U          _map_fn;
};

template <typename T>
class FilterOp : public DataOp<T> {
public:
    using FILTER_FN = std::function<bool(T)>;
    
    FilterOp(ThreadContext& ctx, DataOp<T>* parent, FILTER_FN filter_fn)
        : DataOp<T>(ctx), _parent(parent), _filter_fn(filter_fn) {}
    
        virtual void Open() {
        _parent->Open();
    }

    virtual bool Next(T& t) {
        while (_parent->Next(t)) {
            if (_filter_fn(t))
                return true;
        }
        return false;
    }

    virtual ~FilterOp() {
        delete _parent;
    }

private:
    DataOp<T>           *_parent;
    FILTER_FN           _filter_fn;
    T                   _cached_value;
};

template <typename T, typename U>
class FlatMapOp : public DataOp<U> {
public:
    using FLATMAP_FN_SIG = std::function<std::vector<U>(T)>;

    FlatMapOp(ThreadContext& ctx, DataOp<T>* parent, FLATMAP_FN_SIG flatmap_fn)
        : DataOp<U>(ctx), _parent(parent), _flatmap_fn(flatmap_fn) {}
    
    virtual void Open() {
        _parent->Open();
        _it = _vector.begin();
    }

    virtual bool Next(U& u) {
        T    t;
        if (_it != _vector.end()) {
            u = *_it++;
            return true;
        } else if (_parent->Next(t)) {
            _vector = _flatmap_fn(t);
            _it = _vector.begin();
            u = *_it++;
            return true;
        } else {
            return false;
        }
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
    
    bool Next(string& line) {
        if (_fp.is_open()) {
            if (_fp.tellg() < _range.second) {
                getline(_fp, line);
                return true;
            } else {
                _fp.close();
            }
        }
        return false;
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
        return (int) s.length();
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
    
    auto to_Upper = [](string data) { 
        /*
        std::for_each(data.begin(), data.end(), [](char & c) { 
            c = ::toupper(c);
        });    
        */
        return data;
    };
    
    using INT2INT = std::function<int(int)>;

    INT2INT negate = [](int i) { return -i; };
     
    std::function<int(string)> len5 = [] (string str) { return str.length() == 5; };

    //auto            tf = (new TextFileOp(ctx, filename)) -> map(strlen) -> map(negate);
    auto            tf = (new TextFileOp(ctx, filename)) -> flatMap(splitString) -> filter(len5);
        
    int            i;
    string         s;
    decltype(tf->type())    dt;

    cout << ">>> HELLO!!!" << endl;

    tf->Open();
    while (tf->Next(s)) {
        cout << ": " << s << endl;
    }

    delete tf;
}
