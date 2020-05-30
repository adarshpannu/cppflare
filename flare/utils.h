//
//  utils.h
//  Flora
//
//  Created by Adarsh Pannu on 5/14/20.
//

#ifndef utils_h
#define utils_h

using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::pair;

using Range = std::pair<size_t, size_t>;

inline void die(string s) {
    cout << "Die: " << s << endl;
    exit(0);
}

#endif /* utils_h */
