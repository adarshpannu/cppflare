//  entity.h

#ifndef Entity_h
#define Entity_h
using STR2INT = std::function<int(string)>;

class Entity {
public:
    Entity() {  }
    ~Entity() { }
    Entity(Entity& that) { cout << "copy ctor" << endl; }
    Entity& operator=(const Entity& that) { cout << "assignment operator" << endl; return *this; }
    Entity(const Entity&& that) { cout << "move ctor" << endl; }
    Entity& operator=(const Entity&& that) { cout << "move assignment operator" << endl; return *this; }
};

Entity createEntity(Entity e4) {
    Entity e3;
    return e3;
}

void test_ctors() {
    STR2INT strlen = [](string s) {
        cout << "> " << s << endl;
        return s.length();
    };

    Entity  e1;
    
    cout << "Entity  e2 = createEntity(e1);" << endl;
    Entity  e2 = createEntity(e1);
    cout << endl;
    
    cout << "e1 = e2" << endl;
    e1 = e2;
    cout << endl;

    cout << "e1 = createEntity(e1)" << endl;
    e1 = createEntity(e1);
    cout << endl;

}

#endif /* Entity_h */
