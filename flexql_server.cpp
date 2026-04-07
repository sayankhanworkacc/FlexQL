/*=================================================================
  FlexQL Server — Complete Single-File Implementation
  
  Wire protocol (matches flexql.cpp client exactly):
    Client → Server : <sql text including semicolon>
    Server reads until ';' found in buffer.
    
    Server → Client responses (all end with END\n):
      OK\nEND\n                              non-SELECT success
      ERROR: <message>\nEND\n               any error
      ROW <n> <L>:<col><L>:<val>...\n       one line per result row
      ...rows...
      END\n
  
  Compile: g++ -O3 -std=c++17 -march=native -pthread flexql_server.cpp -o server
  Run:     ./server           (listens on 127.0.0.1:9000)
=================================================================*/

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

static std::atomic<bool> g_stop{false};

/* ================================================================
   SECTION 1: STORAGE TYPES
   ================================================================ */

enum class ColType { DECIMAL, VARCHAR, INT, DATETIME, TEXT };

struct ColDef {
    std::string name;
    ColType     type;
};

struct Schema {
    std::string          table_name;
    std::vector<ColDef>  cols;

    int index_of(const std::string &n) const {
        for (int i = 0; i < (int)cols.size(); ++i)
            if (cols[i].name == n) return i;
        return -1;
    }
};

struct Row {
    std::vector<std::string> fields;
    int64_t                  expires_at_ns = 0; // 0 = immortal

    bool is_expired() const {
        if (expires_at_ns == 0) return false;
        using namespace std::chrono;
        return system_clock::now().time_since_epoch() / nanoseconds(1) > expires_at_ns;
    }
};

/* ================================================================
   SECTION 2: B+ TREE (primary key index)
   ================================================================ */

template<typename K, typename V, int Order = 128>
class BTree {
    static_assert(Order >= 4, "");
    static constexpr int MAX_K = Order - 1;
    static constexpr int LEAF_MAX = Order - 1;

    struct Node { bool leaf; Node(bool l):leaf(l){} virtual ~Node()=default; };

    struct INode : Node {
        std::array<K, MAX_K+1> keys;
        std::array<std::shared_ptr<Node>, Order+1> ch;
        int n = 0;
        INode():Node(false){}
    };
    struct LNode : Node {
        std::array<K, LEAF_MAX+1>   keys;
        std::array<V, LEAF_MAX+1>   vals;
        int n = 0;
        std::shared_ptr<LNode> next;
        LNode():Node(true){}
    };

    static int lb(const K *keys, int n, const K &k) {
        int lo=0,hi=n;
        while(lo<hi){int m=(lo+hi)>>1; if(keys[m]<k)lo=m+1; else hi=m;}
        return lo;
    }

    struct Split { K key; std::shared_ptr<Node> right; };

    std::optional<Split> ins(std::shared_ptr<Node> &nd, const K &k, const V &v) {
        if (nd->leaf) {
            auto *l = static_cast<LNode*>(nd.get());
            int p = lb(l->keys.data(), l->n, k);
            if (p < l->n && l->keys[p]==k) { l->vals[p]=v; return {}; }
            for(int i=l->n;i>p;--i){l->keys[i]=std::move(l->keys[i-1]);l->vals[i]=std::move(l->vals[i-1]);}
            l->keys[p]=k; l->vals[p]=v; ++l->n; ++sz_;
            if(l->n<=LEAF_MAX) return {};
            return split_leaf(nd);
        }
        auto *in = static_cast<INode*>(nd.get());
        int p = lb(in->keys.data(), in->n, k);
        if(p<in->n && in->keys[p]==k) ++p;
        auto r = ins(in->ch[p],k,v);
        if(!r) return {};
        for(int i=in->n;i>p;--i){in->keys[i]=std::move(in->keys[i-1]);in->ch[i+1]=std::move(in->ch[i]);}
        in->keys[p]=std::move(r->key); in->ch[p+1]=std::move(r->right); ++in->n;
        if(in->n<=MAX_K) return {};
        return split_inode(nd);
    }

    Split split_leaf(std::shared_ptr<Node> &nd) {
        auto *l=static_cast<LNode*>(nd.get());
        auto r=std::make_shared<LNode>();
        int m=l->n/2;
        r->n=l->n-m;
        for(int i=0;i<r->n;++i){r->keys[i]=std::move(l->keys[m+i]);r->vals[i]=std::move(l->vals[m+i]);}
        l->n=m; r->next=l->next; l->next=r;
        return {r->keys[0], r};
    }
    Split split_inode(std::shared_ptr<Node> &nd) {
        auto *in=static_cast<INode*>(nd.get());
        auto r=std::make_shared<INode>();
        int m=in->n/2;
        K promoted=std::move(in->keys[m]);
        r->n=in->n-m-1;
        for(int i=0;i<r->n;++i) r->keys[i]=std::move(in->keys[m+1+i]);
        for(int i=0;i<=r->n;++i) r->ch[i]=std::move(in->ch[m+1+i]);
        in->n=m;
        return {std::move(promoted), r};
    }

    LNode* leftmost() const {
        Node *c=root_.get(); while(!c->leaf) c=static_cast<INode*>(c)->ch[0].get();
        return static_cast<LNode*>(c);
    }
    LNode* find_leaf(const K &k) const {
        if(!root_) return nullptr;
        Node *c=root_.get();
        while(!c->leaf){auto *in=static_cast<INode*>(c); int p=lb(in->keys.data(),in->n,k); if(p<in->n&&in->keys[p]==k)++p; c=in->ch[p].get();}
        return static_cast<LNode*>(c);
    }

public:
    BTree()=default;
    size_t size() const { return sz_; }

    void insert(const K &k, const V &v) {
        if(!root_){ root_=std::make_shared<LNode>(); }
        auto r=ins(root_,k,v);
        if(r){ auto nr=std::make_shared<INode>(); nr->keys[0]=std::move(r->key); nr->ch[0]=root_; nr->ch[1]=std::move(r->right); nr->n=1; root_=nr; }
    }

    std::optional<V> find(const K &k) const {
        LNode *l=find_leaf(k); if(!l) return {};
        int p=lb(l->keys.data(),l->n,k);
        if(p<l->n&&l->keys[p]==k) return l->vals[p];
        return {};
    }

    void scan_all(std::function<bool(const K&,const V&)> fn) const {
        if(!root_) return;
        LNode *l=leftmost();
        while(l){ for(int i=0;i<l->n;++i) if(!fn(l->keys[i],l->vals[i])) return; l=l->next.get(); }
    }

    void scan_from(const K &lo, bool inclusive, std::function<bool(const K&,const V&)> fn) const {
        LNode *l=find_leaf(lo); if(!l) return;
        int p=lb(l->keys.data(),l->n,lo);
        while(l){
            for(int i=p;i<l->n;++i){
                const K &k=l->keys[i];
                if(!inclusive && k==lo) continue;
                if(!fn(k,l->vals[i])) return;
            }
            l=l->next.get(); p=0;
        }
    }

private:
    std::shared_ptr<Node> root_;
    size_t sz_=0;
};

/* ================================================================
   SECTION 3: LRU CACHE
   ================================================================ */

template<typename K, typename V>
class LRUCache {
    using Pair = std::pair<K,V>;
    using List = std::list<Pair>;
    std::size_t cap_;
    List lru_;
    std::unordered_map<K,typename List::iterator> map_;
    mutable std::mutex mu_;
public:
    explicit LRUCache(std::size_t cap):cap_(cap){}
    std::optional<V> get(const K &k){
        std::lock_guard<std::mutex> g(mu_);
        auto it=map_.find(k); if(it==map_.end()) return {};
        lru_.splice(lru_.begin(),lru_,it->second);
        return it->second->second;
    }
    void put(const K &k, V v){
        std::lock_guard<std::mutex> g(mu_);
        auto it=map_.find(k);
        if(it!=map_.end()){ it->second->second=std::move(v); lru_.splice(lru_.begin(),lru_,it->second); return; }
        if(lru_.size()>=cap_){ auto last=std::prev(lru_.end()); map_.erase(last->first); lru_.erase(last); }
        lru_.emplace_front(k,std::move(v));
        map_[k]=lru_.begin();
    }
    void invalidate_containing(const std::string &sub){
        std::lock_guard<std::mutex> g(mu_);
        for(auto it=lru_.begin();it!=lru_.end();){
            if(it->first.find(sub)!=std::string::npos){ map_.erase(it->first); it=lru_.erase(it); } else ++it;
        }
    }
    void clear(){ std::lock_guard<std::mutex> g(mu_); lru_.clear(); map_.clear(); }
};

/* ================================================================
   SECTION 4: TABLE
   ================================================================ */

class Table {
public:
    explicit Table(Schema s): schema_(std::move(s)) {}

    const Schema &schema() const { return schema_; }
    std::shared_mutex &mu()      { return mu_; }
    size_t row_count()     const { return rows_.size(); }
    bool   has_ttl()       const { return has_ttl_; }

    /* Caller holds WRITE lock */
    void insert_locked(Row row) {
        if (row.fields.size() != schema_.cols.size())
            throw std::runtime_error("Column count mismatch: expected "
                + std::to_string(schema_.cols.size()) + ", got "
                + std::to_string(row.fields.size()));
        const std::string &pk = row.fields[0];
        if (pk_idx_.find(pk).has_value())
            throw std::runtime_error("Duplicate primary key: " + pk);
        if (row.expires_at_ns != 0) has_ttl_ = true;
        uint64_t idx = rows_.size();
        rows_.push_back(std::move(row));
        pk_idx_.insert(pk, idx);
    }

    /* Caller holds READ lock */
    void scan_locked(const std::function<bool(const Row&)> &fn) const {
        for (auto &r: rows_) { if(r.is_expired()) continue; if(!fn(r)) return; }
    }

    const Row *pk_lookup_locked(const std::string &pk) const {
        auto idx = pk_idx_.find(pk);
        if (!idx) return nullptr;
        const Row &r = rows_[*idx];
        return r.is_expired() ? nullptr : &r;
    }

    /* Range scan: op is "=",">","<",">=","<=" */
    void pk_scan_locked(const std::string &op, const std::string &val,
                        const std::function<bool(const Row&)> &fn) const
    {
        if (op == "=") {
            if (auto idx = pk_idx_.find(val)) {
                const Row &r = rows_[*idx];
                if (!r.is_expired()) fn(r);
            }
            return;
        }
        bool gt  = (op==">"  || op==">=");
        bool inc = (op==">=" || op=="<=");

        if (gt) {
            pk_idx_.scan_from(val, inc, [&](const std::string &k, uint64_t idx)->bool{
                const Row &r = rows_[idx];
                if (r.is_expired()) return true;
                return fn(r);
            });
        } else {
            /* "<" or "<=" : walk from beginning */
            pk_idx_.scan_all([&](const std::string &k, uint64_t idx)->bool{
                int c = cmp_str(k, val);
                if (op=="<"  && c >= 0) return false;
                if (op=="<=" && c >  0) return false;
                const Row &r = rows_[idx];
                if (r.is_expired()) return true;
                return fn(r);
            });
        }
    }

    void vacuum() {
        std::unique_lock<std::shared_mutex> lk(mu_);
        std::vector<Row> live;
        for (auto &r: rows_) if(!r.is_expired()) live.push_back(std::move(r));
        rows_ = std::move(live);
        pk_idx_ = BTree<std::string,uint64_t,128>();
        for (uint64_t i=0;i<rows_.size();++i) pk_idx_.insert(rows_[i].fields[0],i);
        has_ttl_ = false;
    }

private:
    static int cmp_str(const std::string &a, const std::string &b) {
        /* Numeric comparison if both parse as doubles */
        char *ea,*eb;
        double da=strtod(a.c_str(),&ea), db=strtod(b.c_str(),&eb);
        if(*ea=='\0'&&*eb=='\0') return (da<db)?-1:(da>db)?1:0;
        return a.compare(b);
    }

    Schema                         schema_;
    std::vector<Row>               rows_;
    BTree<std::string,uint64_t,128> pk_idx_;
    bool                           has_ttl_ = false;
    mutable std::shared_mutex      mu_;
};

/* ================================================================
   SECTION 5: CATALOG
   ================================================================ */

class Catalog {
public:
    bool create_table(Schema s) {
        std::unique_lock<std::shared_mutex> lk(mu_);
        if (tbls_.count(s.table_name)) return false;
        auto t = std::make_shared<Table>(std::move(s));
        tbls_[t->schema().table_name] = t;
        return true;
    }
    std::shared_ptr<Table> get(const std::string &name) const {
        std::shared_lock<std::shared_mutex> lk(mu_);
        auto it = tbls_.find(name);
        return it==tbls_.end() ? nullptr : it->second;
    }
private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string,std::shared_ptr<Table>> tbls_;
};

/* ================================================================
   SECTION 6: SQL TOKENIZER + PARSER
   ================================================================ */

struct Tok {
    enum K { WORD,NUM,STR,LP,RP,COMMA,DOT,STAR,EQ,GT,LT,GEQ,LEQ,SEMI,END } k;
    std::string v;
};

static std::vector<Tok> tokenize(const std::string &sql) {
    std::vector<Tok> ts;
    size_t i=0,n=sql.size();
    while(i<n){
        char c=sql[i];
        if(std::isspace(c)){++i;continue;}
        if(c=='\''||c=='"'){
            char q=c; ++i;
            std::string s;
            while(i<n&&sql[i]!=q) s+=sql[i++];
            if(i<n)++i;
            ts.push_back({Tok::STR,s}); continue;
        }
        if(std::isalpha(c)||c=='_'){
            std::string w;
            while(i<n&&(std::isalnum(sql[i])||sql[i]=='_'||sql[i]=='@'||sql[i]=='.'))
                w+=sql[i++];
            /* uppercase */
            std::string up=w;
            for(auto &x:up) x=toupper(x);
            /* If word contains a dot, split into table.col - handled at parse level */
            ts.push_back({Tok::WORD,up}); continue;
        }
        if(std::isdigit(c)||(c=='-'&&i+1<n&&std::isdigit(sql[i+1]))){
            std::string num;
            if(c=='-'){num+=c;++i;}
            while(i<n&&(std::isdigit(sql[i])||sql[i]=='.')) num+=sql[i++];
            ts.push_back({Tok::NUM,num}); continue;
        }
        ++i;
        switch(c){
            case '(': ts.push_back({Tok::LP,"("});  break;
            case ')': ts.push_back({Tok::RP,")"});  break;
            case ',': ts.push_back({Tok::COMMA,","});break;
            case '.': ts.push_back({Tok::DOT,"."});  break;
            case '*': ts.push_back({Tok::STAR,"*"}); break;
            case '=': ts.push_back({Tok::EQ,"="});   break;
            case ';': ts.push_back({Tok::SEMI,";"});  break;
            case '>': if(i<n&&sql[i]=='='){++i;ts.push_back({Tok::GEQ,">="});}
                      else ts.push_back({Tok::GT,">"});break;
            case '<': if(i<n&&sql[i]=='='){++i;ts.push_back({Tok::LEQ,"<="});}
                      else ts.push_back({Tok::LT,"<"});break;
            default: break;
        }
    }
    ts.push_back({Tok::END,""});
    return ts;
}

/* --- AST --- */
struct WhereClause {
    std::string tbl_prefix; // "USERS" in "USERS.COL op val"
    std::string col;
    std::string op;
    std::string val;
};
struct ColRef { std::string tbl; std::string col; }; // tbl may be empty
struct JoinClause {
    std::string right_tbl;
    std::string left_col, right_col;
    std::string left_tbl_ref, right_tbl_ref; // original qualifiers in ON clause
};
struct CreateStmt {
    std::string name;
    struct CD { std::string col; std::string type; };
    std::vector<CD> cols;
};
struct InsertStmt {
    std::string name;
    std::vector<std::vector<std::string>> rows;  // multi-row VALUES support
    int64_t ttl_sec = 0;
};
struct SelectStmt {
    bool wildcard=false;
    std::vector<ColRef> cols;
    std::string from_tbl;
    std::string from_alias;
    std::unique_ptr<JoinClause> join;
    std::unique_ptr<WhereClause> where;
};
enum class StmtT { CREATE, INSERT, SELECT };
struct Stmt {
    StmtT type;
    std::unique_ptr<CreateStmt>  cr;
    std::unique_ptr<InsertStmt>  ins;
    std::unique_ptr<SelectStmt>  sel;
};

/* --- Parser --- */
class Parser {
    std::vector<Tok> ts_;
    size_t p_=0;
    Tok &cur() { return ts_[p_]; }
    bool at_end(){return cur().k==Tok::END||cur().k==Tok::SEMI;}

    std::string expect_word(){
        if(cur().k!=Tok::WORD) throw std::runtime_error("Expected identifier, got '"+cur().v+"'");
        return ts_[p_++].v;
    }
    void expect_kw(const std::string &kw){
        std::string w=expect_word();
        if(w!=kw) throw std::runtime_error("Expected '"+kw+"', got '"+w+"'");
    }
    void expect_tok(Tok::K k){
        if(cur().k!=k) throw std::runtime_error("Unexpected token '"+cur().v+"'");
        ++p_;
    }
    bool try_kw(const std::string &kw){
        if(cur().k==Tok::WORD&&cur().v==kw){++p_;return true;}
        return false;
    }
    std::string parse_value(){
        auto k=cur().k;
        if(k==Tok::WORD||k==Tok::STR||k==Tok::NUM) return ts_[p_++].v;
        if(k==Tok::STAR){++p_;return "*";}
        throw std::runtime_error("Expected value");
    }
    std::string parse_op(){
        switch(cur().k){
            case Tok::EQ:  ++p_; return "=";
            case Tok::GT:  ++p_; return ">";
            case Tok::LT:  ++p_; return "<";
            case Tok::GEQ: ++p_; return ">=";
            case Tok::LEQ: ++p_; return "<=";
            default: throw std::runtime_error("Expected operator");
        }
    }

    /* Parse "WORD" or "WORD.WORD" as a ColRef */
    ColRef parse_col_ref(){
        if(cur().k==Tok::STAR){++p_;return {"","*"};}
        std::string first=expect_word();
        /* Check for dotted notation (tokenizer may have combined or split it) */
        if(cur().k==Tok::DOT){++p_; std::string col=expect_word(); return {first,col};}
        /* Check if first contains a dot (tokenizer merged it) */
        auto dot=first.find('.');
        if(dot!=std::string::npos) return {first.substr(0,dot),first.substr(dot+1)};
        return {"",first};
    }

    /* Parse a type name, skipping optional (n) suffix */
    std::string parse_type_name(){
        std::string t=expect_word();
        if(cur().k==Tok::LP){
            ++p_;
            while(cur().k!=Tok::RP&&!at_end()) ++p_;
            if(cur().k==Tok::RP) ++p_;
        }
        return t;
    }

    std::unique_ptr<WhereClause> parse_where(){
        if(!try_kw("WHERE")) return nullptr;
        auto wc=std::make_unique<WhereClause>();
        /* Could be "TABLE.COL op val" or "COL op val" */
        std::string first=expect_word();
        /* Check if it's dotted or if next is a dot */
        std::string col_name;
        auto dot=first.find('.');
        if(dot!=std::string::npos){
            wc->tbl_prefix=first.substr(0,dot);
            col_name=first.substr(dot+1);
        } else if(cur().k==Tok::DOT){
            ++p_;
            wc->tbl_prefix=first;
            col_name=expect_word();
        } else {
            col_name=first;
        }
        wc->col=col_name;
        wc->op=parse_op();
        wc->val=parse_value();
        return wc;
    }

    Stmt parse_create(){
        expect_kw("TABLE");
        auto c=std::make_unique<CreateStmt>();
        c->name=expect_word();
        expect_tok(Tok::LP);
        while(cur().k!=Tok::RP&&!at_end()){
            CreateStmt::CD cd;
            cd.col=expect_word();
            cd.type=parse_type_name();
            /* Skip any extra modifiers: NOT NULL, PRIMARY KEY, etc. */
            while(cur().k==Tok::WORD){
                std::string mod=cur().v;
                if(mod=="NOT"||mod=="NULL"||mod=="PRIMARY"||mod=="KEY"||
                   mod=="AUTO_INCREMENT"||mod=="DEFAULT"||mod=="UNIQUE"||
                   mod=="UNSIGNED"||mod=="CONSTRAINT"||mod=="CHECK")
                    ++p_;
                else break;
            }
            c->cols.push_back(cd);
            if(cur().k==Tok::COMMA) ++p_;
        }
        expect_tok(Tok::RP);
        Stmt s; s.type=StmtT::CREATE; s.cr=std::move(c); return s;
    }

    Stmt parse_insert(){
        expect_kw("INTO");
        auto ins=std::make_unique<InsertStmt>();
        ins->name=expect_word();
        expect_kw("VALUES");
        /* Accept one or more (v1,v2,...) groups separated by commas. */
        do {
            expect_tok(Tok::LP);
            std::vector<std::string> row_vals;
            while(cur().k!=Tok::RP&&!at_end()){
                row_vals.push_back(parse_value());
                if(cur().k==Tok::COMMA) ++p_;
            }
            expect_tok(Tok::RP);
            ins->rows.push_back(std::move(row_vals));
        } while(cur().k==Tok::COMMA&&(++p_,true));
        /* Optional TTL */
        if(try_kw("TTL")){
            if(cur().k!=Tok::NUM) throw std::runtime_error("Expected number after TTL");
            ins->ttl_sec=std::stoll(ts_[p_++].v);
        }
        Stmt s; s.type=StmtT::INSERT; s.ins=std::move(ins); return s;
    }

    Stmt parse_select(){
        auto sel=std::make_unique<SelectStmt>();
        if(cur().k==Tok::STAR){++p_;sel->wildcard=true;}
        else{
            while(!at_end()){
                sel->cols.push_back(parse_col_ref());
                if(sel->cols.back().col=="*") sel->wildcard=true;
                if(cur().k==Tok::COMMA){++p_;continue;} break;
            }
        }
        expect_kw("FROM");
        sel->from_tbl=expect_word();
        /* Optional alias: next word that is not a keyword */
        if(cur().k==Tok::WORD){
            const std::string &nxt=cur().v;
            if(nxt!="INNER"&&nxt!="WHERE"&&nxt!="JOIN"&&nxt!="ON")
                sel->from_alias=ts_[p_++].v;
        }
        if(sel->from_alias.empty()) sel->from_alias=sel->from_tbl;

        /* Optional INNER JOIN */
        if(try_kw("INNER")){
            expect_kw("JOIN");
            auto jc=std::make_unique<JoinClause>();
            jc->right_tbl=expect_word();
            /* Optional alias */
            if(cur().k==Tok::WORD){
                const std::string &nx=cur().v;
                if(nx!="ON"&&nx!="WHERE") { /* skip alias */ ++p_; }
            }
            expect_kw("ON");
            /* left_ref.col = right_ref.col */
            std::string lref=expect_word();
            std::string lcol;
            if(cur().k==Tok::DOT){++p_;lcol=expect_word();}
            else{auto d=lref.find('.');if(d!=std::string::npos){lcol=lref.substr(d+1);lref=lref.substr(0,d);}else lcol=lref;}
            jc->left_tbl_ref=lref; jc->left_col=lcol;

            expect_tok(Tok::EQ);

            std::string rref=expect_word();
            std::string rcol;
            if(cur().k==Tok::DOT){++p_;rcol=expect_word();}
            else{auto d=rref.find('.');if(d!=std::string::npos){rcol=rref.substr(d+1);rref=rref.substr(0,d);}else rcol=rref;}
            jc->right_tbl_ref=rref; jc->right_col=rcol;

            sel->join=std::move(jc);
        }

        sel->where=parse_where();
        Stmt s; s.type=StmtT::SELECT; s.sel=std::move(sel); return s;
    }

public:
    explicit Parser(const std::string &sql):ts_(tokenize(sql)){}
    Stmt parse(){
        std::string kw=expect_word();
        if(kw=="CREATE") return parse_create();
        if(kw=="INSERT") return parse_insert();
        if(kw=="SELECT") return parse_select();
        throw std::runtime_error("Unknown statement: "+kw);
    }
};

/* ================================================================
   SECTION 7: EXECUTOR
   ================================================================ */

using CacheVal = std::pair<std::vector<std::string>,std::vector<std::vector<std::string>>>;
using QCache   = LRUCache<std::string,CacheVal>;

struct QResult {
    bool ok=true;
    std::string err;
    std::vector<std::string> cols;
    std::vector<std::vector<std::string>> rows;
};

static ColType parse_coltype(const std::string &t){
    if(t=="DECIMAL"||t=="INT"||t=="INTEGER"||t=="NUMERIC"||t=="FLOAT"||t=="DOUBLE"||t=="REAL") return ColType::DECIMAL;
    if(t=="TEXT"||t=="VARCHAR"||t=="CHAR"||t=="STRING") return ColType::VARCHAR;
    if(t=="DATETIME"||t=="DATE"||t=="TIMESTAMP") return ColType::DATETIME;
    return ColType::VARCHAR; /* default */
}

static bool eval_where(const WhereClause &wc,
                        const std::vector<std::string> &col_names,
                        const Row &row)
{
    /* Find column: match by plain name, ignoring table prefix */
    int idx=-1;
    for(int i=0;i<(int)col_names.size();++i){
        std::string cn=col_names[i];
        auto d=cn.rfind('.');
        if(d!=std::string::npos) cn=cn.substr(d+1);
        if(cn==wc.col){idx=i;break;}
    }
    if(idx<0||idx>=(int)row.fields.size()) return false;

    const std::string &val=row.fields[idx];
    int c;
    char *ea,*eb;
    double da=strtod(val.c_str(),&ea),db=strtod(wc.val.c_str(),&eb);
    bool num=(*ea=='\0'&&*eb=='\0');
    if(num) c=(da<db)?-1:(da>db)?1:0;
    else    c=val.compare(wc.val);

    if(wc.op=="=")  return c==0;
    if(wc.op==">")  return c>0;
    if(wc.op=="<")  return c<0;
    if(wc.op==">=") return c>=0;
    if(wc.op=="<=") return c<=0;
    return false;
}

class Executor {
public:
    Executor(Catalog &cat, size_t cache_cap=8192):cat_(cat),cache_(cache_cap){}

    QResult run(const std::string &sql){
        try{
            Parser p(sql); Stmt st=p.parse();
            switch(st.type){
                case StmtT::CREATE: return do_create(*st.cr);
                case StmtT::INSERT: return do_insert(*st.ins);
                case StmtT::SELECT: return do_select(*st.sel,sql);
            }
        }catch(const std::exception &e){ return {false,e.what()}; }
        return {false,"Unknown"};
    }

private:
    Catalog &cat_;
    QCache   cache_;

    QResult do_create(const CreateStmt &s){
        Schema sc; sc.table_name=s.name;
        for(auto &cd:s.cols) sc.cols.push_back({cd.col,parse_coltype(cd.type)});
        if(!cat_.create_table(std::move(sc)))
            return {false,"Table already exists: "+s.name};
        return {};
    }

    QResult do_insert(const InsertStmt &s){
        auto tbl=cat_.get(s.name);
        if(!tbl) return {false,"Table not found: "+s.name};

        /* Compute TTL timestamp once for all rows in the batch. */
        int64_t exp_ns=0;
        if(s.ttl_sec>0){
            using namespace std::chrono;
            exp_ns=duration_cast<nanoseconds>(
                (system_clock::now()+seconds(s.ttl_sec)).time_since_epoch()).count();
        }

        /* Acquire write lock once for the whole batch — avoids N lock/unlock cycles. */
        {
            std::unique_lock<std::shared_mutex> lk(tbl->mu());
            for(auto &vals:s.rows){
                Row row; row.fields=vals; row.expires_at_ns=exp_ns;
                tbl->insert_locked(std::move(row));
            }
        }
        cache_.invalidate_containing(s.name);
        return {};
    }

    QResult do_select(const SelectStmt &s, const std::string &sql_key){
        auto ltbl=cat_.get(s.from_tbl);
        if(!ltbl) return {false,"Table not found: "+s.from_tbl};

        std::shared_ptr<Table> rtbl;
        if(s.join){
            rtbl=cat_.get(s.join->right_tbl);
            if(!rtbl) return {false,"Table not found: "+s.join->right_tbl};
        }

        bool has_ttl=ltbl->has_ttl()||(rtbl&&rtbl->has_ttl());
        if(!has_ttl){
            if(auto cv=cache_.get(sql_key)){
                QResult qr; qr.cols=cv->first; qr.rows=cv->second; return qr;
            }
        }

        QResult res;

        if(!s.join){
            /* Simple SELECT */
            std::shared_lock<std::shared_mutex> lk(ltbl->mu());
            const Schema &sc=ltbl->schema();

            /* Build output column names & validate */
            if(s.wildcard){
                for(auto &cd:sc.cols) res.cols.push_back(cd.name);
            } else {
                for(auto &cr:s.cols){
                    if(cr.col=="*"){ for(auto &cd:sc.cols) res.cols.push_back(cd.name); continue; }
                    int idx=sc.index_of(cr.col);
                    if(idx<0) return {false,"Column not found: "+cr.col};
                    res.cols.push_back(cr.col);
                }
            }

            /* Column names for WHERE */
            std::vector<std::string> eval_names;
            for(auto &cd:sc.cols) eval_names.push_back(cd.name);

            /* Use index if WHERE is on first column */
            bool use_idx = s.where && !sc.cols.empty() && sc.cols[0].name==s.where->col;

            auto project=[&](const Row &row)->bool{
                if(s.where&&!eval_where(*s.where,eval_names,row)) return true;
                if(s.wildcard){
                    res.rows.push_back(row.fields);
                } else {
                    std::vector<std::string> out;
                    for(auto &cr:s.cols){
                        if(cr.col=="*"){for(auto &f:row.fields)out.push_back(f);continue;}
                        int i=sc.index_of(cr.col);
                        out.push_back(i>=0?row.fields[i]:"NULL");
                    }
                    res.rows.push_back(std::move(out));
                }
                return true;
            };

            if(use_idx) ltbl->pk_scan_locked(s.where->op,s.where->val,project);
            else        ltbl->scan_locked(project);

        } else {
            /* INNER JOIN */
            auto &jc=*s.join;
            /* Lock in consistent order to prevent deadlock */
            bool lf=s.from_tbl<=jc.right_tbl;
            auto &mu1=lf?ltbl->mu():rtbl->mu();
            auto &mu2=lf?rtbl->mu():ltbl->mu();
            std::shared_lock<std::shared_mutex> lk1(mu1),lk2(mu2);

            const Schema &ls=ltbl->schema(), &rs=rtbl->schema();

            int l_idx=ls.index_of(jc.left_col);
            int r_idx=rs.index_of(jc.right_col);
            if(l_idx<0) return {false,"Column not found: "+jc.left_col};
            if(r_idx<0) return {false,"Column not found: "+jc.right_col};

            /* Output col names: use from_alias.col, right_alias.col */
            std::string la=s.from_alias, ra=jc.right_tbl;
            if(s.wildcard){
                for(auto &cd:ls.cols) res.cols.push_back(la+"."+cd.name);
                for(auto &cd:rs.cols) res.cols.push_back(ra+"."+cd.name);
            } else {
                for(auto &cr:s.cols){
                    if(cr.tbl.empty()) res.cols.push_back(cr.col);
                    else res.cols.push_back(cr.tbl+"."+cr.col);
                }
                /* Validate projected columns */
                for(auto &cr:s.cols){
                    if(cr.col=="*") continue;
                    bool is_left=(cr.tbl.empty()||cr.tbl==la||cr.tbl==s.from_tbl);
                    bool is_right=(cr.tbl==ra||cr.tbl==jc.right_tbl);
                    if(is_left&&ls.index_of(cr.col)<0&&!is_right)
                        return {false,"Column not found: "+cr.col};
                    if(is_right&&rs.index_of(cr.col)<0)
                        return {false,"Column not found: "+cr.col};
                }
            }

            /* Merged col names for WHERE */
            std::vector<std::string> merged_names;
            for(auto &cd:ls.cols) merged_names.push_back(la+"."+cd.name);
            for(auto &cd:rs.cols) merged_names.push_back(ra+"."+cd.name);

            /* Hash probe from right table */
            std::unordered_map<std::string,std::vector<const Row*>> probe;
            probe.reserve(rtbl->row_count());
            rtbl->scan_locked([&](const Row &rrow)->bool{
                probe[rrow.fields[r_idx]].push_back(&rrow); return true;
            });

            int lw=(int)ls.cols.size();

            ltbl->scan_locked([&](const Row &lrow)->bool{
                auto it=probe.find(lrow.fields[l_idx]);
                if(it==probe.end()) return true;
                for(const Row *rrow:it->second){
                    Row merged;
                    merged.fields.insert(merged.fields.end(),lrow.fields.begin(),lrow.fields.end());
                    merged.fields.insert(merged.fields.end(),rrow->fields.begin(),rrow->fields.end());
                    if(s.where&&!eval_where(*s.where,merged_names,merged)) continue;
                    if(s.wildcard){
                        res.rows.push_back(merged.fields);
                    } else {
                        std::vector<std::string> out;
                        for(auto &cr:s.cols){
                            if(cr.col=="*"){for(auto &f:merged.fields)out.push_back(f);continue;}
                            int idx=-1;
                            bool is_left=(cr.tbl.empty()||cr.tbl==la||cr.tbl==s.from_tbl);
                            bool is_right=(cr.tbl==ra||cr.tbl==jc.right_tbl);
                            if(is_left){int li=ls.index_of(cr.col);if(li>=0)idx=li;}
                            if(idx<0&&is_right){int ri=rs.index_of(cr.col);if(ri>=0)idx=lw+ri;}
                            if(idx<0){int li=ls.index_of(cr.col);if(li>=0)idx=li;}
                            if(idx<0){int ri=rs.index_of(cr.col);if(ri>=0)idx=lw+ri;}
                            out.push_back((idx>=0&&idx<(int)merged.fields.size())?merged.fields[idx]:"NULL");
                        }
                        res.rows.push_back(std::move(out));
                    }
                }
                return true;
            });
        }

        if(!has_ttl) cache_.put(sql_key,{res.cols,res.rows});
        return res;
    }
};

/* ================================================================
   SECTION 8: WIRE PROTOCOL HELPERS
   ================================================================ */

/*  Format one result row as: ROW <n> <len>:<name><len>:<val>...\n  */
static std::string encode_row(const std::vector<std::string> &col_names,
                               const std::vector<std::string> &vals)
{
    std::string line = "ROW " + std::to_string(vals.size()) + " ";
    for (size_t i = 0; i < vals.size(); ++i) {
        const std::string &name = (i < col_names.size()) ? col_names[i] : "";
        const std::string &val  = vals[i];
        line += std::to_string(name.size()) + ":" + name;
        line += std::to_string(val.size())  + ":" + val;
    }
    line += "\n";
    return line;
}

static void send_all(int fd, const std::string &data) {
    size_t sent=0;
    while(sent<data.size()){
        ssize_t n=::send(fd,data.data()+sent,data.size()-sent,MSG_NOSIGNAL);
        if(n<=0) return;
        sent+=n;
    }
}

static void respond_ok(int fd){
    send_all(fd,"OK\nEND\n");
}
static void respond_error(int fd, const std::string &msg){
    send_all(fd,"ERROR: "+msg+"\nEND\n");
}
static void respond_rows(int fd, const QResult &qr){
    std::string buf;
    buf.reserve(qr.rows.size()*64);
    for(auto &row:qr.rows)
        buf+=encode_row(qr.cols,row);
    buf+="END\n";
    send_all(fd,buf);
}

/* ================================================================
   SECTION 9: THREAD POOL
   ================================================================ */

class ThreadPool {
public:
    explicit ThreadPool(size_t n):stop_(false){
        for(size_t i=0;i<n;++i)
            workers_.emplace_back([this]{
                for(;;){
                    std::function<void()> t;
                    {std::unique_lock<std::mutex> lk(mu_);
                     cv_.wait(lk,[this]{return stop_||!q_.empty();});
                     if(stop_&&q_.empty()) return;
                     t=std::move(q_.front()); q_.pop();}
                    t();
                }
            });
    }
    ~ThreadPool(){
        {std::unique_lock<std::mutex> lk(mu_);stop_=true;}
        cv_.notify_all();
        for(auto &w:workers_) w.join();
    }
    void submit(std::function<void()> f){
        {std::unique_lock<std::mutex> lk(mu_);q_.push(std::move(f));}
        cv_.notify_one();
    }
private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> q_;
    std::mutex mu_; std::condition_variable cv_; bool stop_;
};

/* ================================================================
   SECTION 10: SERVER
   ================================================================ */

static void handle_client(int fd, Executor &exec) {
    std::string pending;
    char buf[65536];

    while (true) {
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n <= 0) break;
        pending.append(buf, n);

        /* Process all complete SQL statements (delimited by ';') */
        size_t pos;
        while ((pos = pending.find(';')) != std::string::npos) {
            std::string sql = pending.substr(0, pos); /* strip ';' */
            pending.erase(0, pos + 1);

            /* Trim whitespace */
            while (!sql.empty() && std::isspace(sql.front())) sql.erase(sql.begin());
            while (!sql.empty() && std::isspace(sql.back()))  sql.pop_back();

            if (sql.empty()) { send_all(fd,"OK\nEND\n"); continue; }

            QResult qr = exec.run(sql);

            if (!qr.ok) {
                respond_error(fd, qr.err);
            } else if (qr.cols.empty()) {
                respond_ok(fd);
            } else {
                respond_rows(fd, qr);
            }
        }
    }
    ::close(fd);
}

int main(int argc, char **argv) {
    int port = 9000;
    if (argc >= 2) port = std::atoi(argv[1]);

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  [](int){ g_stop = true; });
    signal(SIGTERM, [](int){ g_stop = true; });

    Catalog catalog;
    Executor exec(catalog, 8192);

    size_t nthreads = std::max(2u, std::thread::hardware_concurrency());
    ThreadPool pool(nthreads);

    int srv = ::socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); return 1; }

    int opt = 1;
    ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (::bind(srv, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (::listen(srv, SOMAXCONN) < 0) { perror("listen"); return 1; }

    /* Non-blocking accept so we can check g_stop */
    ::fcntl(srv, F_SETFL, O_NONBLOCK);

    std::cout << "FlexQL Server running on port " << port
              << " (" << nthreads << " threads)\n";

    while (!g_stop) {
        pollfd pfd{srv, POLLIN, 0};
        if (::poll(&pfd, 1, 200) <= 0) continue;

        sockaddr_in ca{};
        socklen_t cl = sizeof(ca);
        int fd = ::accept(srv, (sockaddr*)&ca, &cl);
        if (fd < 0) continue;

        int flag = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        pool.submit([fd, &exec]{ handle_client(fd, exec); });
    }

    ::close(srv);
    std::cout << "[FlexQL] Shutdown.\n";
    return 0;
}
