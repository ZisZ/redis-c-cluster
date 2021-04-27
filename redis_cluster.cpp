#include "redis_cluster.h"
#include "deps/crc16.c"
#include <time.h>
#include <string.h>
#include <iostream>
#include <sstream>
#include <string>
#include <set>
#include <vector>
#include <map>
#include <iterator>
#include <hiredis/hiredis.h>

#ifdef DEBUG
#define DEBUGINFO(msg) std::cout << "[DEBUG] "<< msg << std::endl;
#else
#define DEBUGINFO(msg)
#endif

#define rcassert(b) \
    if(!(b)) {\
        abort();\
    }

namespace {
void SplitString(const std::string& srcStr, std::vector<std::string>& vec,const std::string& separator) {
   std::string::size_type posSubstringStart;
   std::string::size_type posSeparator;
   posSeparator= srcStr.find(separator);
   posSubstringStart= 0;
   while (std::string::npos != posSeparator){
     vec.push_back(srcStr.substr(posSubstringStart, posSeparator- posSubstringStart));
     posSubstringStart= posSeparator+ separator.size();
     posSeparator= srcStr.find(separator, posSubstringStart);
   }
   if (posSubstringStart!= srcStr.length())
     vec.push_back(srcStr.substr(posSubstringStart));
}
}

namespace redis {
namespace cluster {

static const char *UNSUPPORT = "#INFO#SHUTDOWN#MULTI#SLAVEOF#CONFIG#";
static const char *SLAVE = "#GET#GETRANGE#MGET#ZRANK#ZRANGE#ZRANGEBYSCORE#ZREVRANK#ZREVRANGE#ZREVRANGEBYSCORE#";

static inline std::string to_upper(const std::string& in) {
    std::string out;
    out.reserve( in.size() );
    for(std::size_t i=0; i<in.length(); i++ ) {
        out += char( toupper(in[i]) );
    }
    return out;
}

void  free_specific_data(void * sdata) {
    delete ((redis::cluster::Cluster::ThreadDataType *)sdata);
}

/**
 * class Node
 */
Node::Node(const std::string& host, unsigned int port,
    unsigned int timeout, const std::string& password) {
    host_ = host;
    port_ = port;
    timeout_ = timeout;
    password_ = password;

    conn_get_count_ = 0;
    conn_reuse_count_ = 0;
    conn_put_count_ = 0;

    int ret = pthread_spin_init(&lock_,PTHREAD_PROCESS_PRIVATE);
    rcassert(ret == 0);
}

Node::~Node() {

    std::list<void *>::iterator iter = connections_.begin();
    for(; iter!=connections_.end(); iter++) {
        redisContext *conn = (redisContext *)*iter;
        redisFree( conn );
    }

}

void *Node::get_conn() {

    DEBUGINFO("get_conn");
    redisContext *conn = NULL;
    bool new_conn = false;

    {
        LockGuard lg(lock_);
        conn_get_count_++;
        for(; connections_.size()>0;) {
            conn = (redisContext *)connections_.back();
            connections_.pop_back();

            if( conn->err==REDIS_OK ) {
                conn_reuse_count_++;
                break;
            }

            redisFree( conn );
            conn = NULL;
        }

    }
    if( !conn ) {
        if (timeout_ > 0) {
            struct timeval tv;
            tv.tv_sec = timeout_ / 1000;
            tv.tv_usec = (timeout_ % 1000) * 1000;

            conn = redisConnectWithTimeout(host_.c_str(), port_, tv);
            if (conn && (conn->err != REDIS_OK)) {
                redisFree( conn );
                conn = NULL;
            }

            if (conn && (redisSetTimeout(conn, tv) != REDIS_OK)) {
                redisFree( conn );
                conn = NULL;
            }
        } else {
            conn = redisConnect(host_.c_str(), port_);
            if (conn && (conn->err != REDIS_OK)) {
                redisFree( conn );
                conn = NULL;
            }
        }
        if (conn) {
          new_conn = true;
        }
    }
    // send auth
    if (conn != NULL and new_conn and password_.length() > 0) {
        redisReply * reply = (redisReply *)redisCommand(conn, "auth %s", password_.c_str());
        if ( !reply ) {
          DEBUGINFO("connection error, auth " << password_ << " not send!");
          redisFree( conn );
          conn = NULL;
          return conn;
        }
        DEBUGINFO("send auth cmd success! " << reply->str);
        if (strcmp(reply->str, "OK") != 0 ) {
          DEBUGINFO("send auth cmd error! " << reply->str);
        }
        freeReplyObject(reply);
    }
    return conn;
}

void *Node::get_readonly_conn() {

    DEBUGINFO("get_readonly_conn");
    redisContext *conn = NULL;
    bool new_conn = false;

    {
        LockGuard lg(lock_);
        conn_get_count_++;
        for(; readonly_connections_.size()>0;) {
            conn = (redisContext *)readonly_connections_.back();
            readonly_connections_.pop_back();

            if( conn->err==REDIS_OK ) {
                conn_reuse_count_++;
                break;
            }

            redisFree( conn );
            conn = NULL;
        }

    }
    if( !conn ) {
        if (timeout_ > 0) {
            struct timeval tv;
            tv.tv_sec = timeout_ / 1000;
            tv.tv_usec = (timeout_ % 1000) * 1000;

            conn = redisConnectWithTimeout(host_.c_str(), port_, tv);
            if (conn && (conn->err != REDIS_OK)) {
                redisFree( conn );
                conn = NULL;
            }

            if (conn && (redisSetTimeout(conn, tv) != REDIS_OK)) {
                redisFree( conn );
                conn = NULL;
            }
        } else {
            conn = redisConnect(host_.c_str(), port_);
            if (conn && (conn->err != REDIS_OK)) {
                redisFree( conn );
                conn = NULL;
            }
        }
        if (conn) {
          new_conn = true;
        }
    }
    // send auth
    if (conn != NULL and new_conn and password_.length() > 0) {
        redisReply * reply = (redisReply *)redisCommand(conn, "auth %s", password_.c_str());
        if ( !reply ) {
          DEBUGINFO("connection error, auth " << password_ << " not send!");
          redisFree( conn );
          conn = NULL;
          return conn;
        }
        DEBUGINFO("send auth cmd success! " << reply->str);
        if (strcmp(reply->str, "OK") != 0 ) {
          DEBUGINFO("send auth cmd error! " << reply->str);
        }
        freeReplyObject(reply);
    }
    // send readonly
    if (conn != NULL) {
        redisReply * reply = (redisReply *)redisCommand(conn, "readonly");
        // 20190306 fix bug: reply is NULL. (Probably some node restarting!)
        if ( !reply ) {
          DEBUGINFO("connection error, readonly not send!");
          redisFree( conn );
          conn = NULL;
          return conn;
        }
        if (strcmp(reply->str, "OK") != 0 ) {
          DEBUGINFO("send readonly cmd error! " << reply->str);
        }
        freeReplyObject(reply);
    }
    return conn;
}

void Node::put_conn(void *conn) {
    LockGuard lg(lock_);
    conn_put_count_++;
    connections_.push_front( conn );
}

void Node::put_readonly_conn(void *conn) {
    LockGuard lg(lock_);
    conn_put_count_++;
    readonly_connections_.push_front( conn );
}

std::string Node::simple_dump() const {
    std::ostringstream ss;
    ss<<"Node{"<< host_ << ":" << port_<<"}";
    return ss.str();
}

std::string Node::stat_dump() {
    std::ostringstream ss;
    LockGuard lg(lock_);
    ss<<"Node{"<< host_ << ":" << port_ << " pool_size(free conn): "<<connections_.size()
      <<" conn_create: "<< conn_get_count_ - conn_reuse_count_
      <<" conn_get: "<< conn_get_count_
      <<" conn_reuse: "<< conn_reuse_count_
      <<" conn_put: "<< conn_put_count_<<"}";
    return ss.str();
}

/**
 * class Cluster
 */
Cluster::Cluster(unsigned int timeout)
    :load_slots_asap_(false),
     timeout_(timeout) {
}

Cluster::~Cluster() {

    // release node pool
    NodePoolType::iterator iter = node_pool_.begin();
    for(; iter!=node_pool_.end(); iter++) {
        Node *node = *iter;
        delete node;
    }
    node_pool_.clear();

    iter = slave_node_pool_.begin();
    for(; iter!=slave_node_pool_.end(); iter++) {
        Node *node = *iter;
        delete node;
    }
    slave_node_pool_.clear();
}

int Cluster::setup(const char *startup, bool lazy, const std::string& password) {

    int ret = pthread_key_create(&key_, free_specific_data);
    rcassert(ret == 0);

    ret = pthread_spin_init(&np_lock_, PTHREAD_PROCESS_PRIVATE);
    if(ret != 0) {
        return -1;
    }

    ret = pthread_spin_init(&load_slots_lock_, PTHREAD_PROCESS_PRIVATE);
    if(ret != 0) {
        return -1;
    }

    password_ = password;
    if( parse_startup(startup)<0 ) {
        return -1;
    }

    slots_.resize( HASH_SLOTS );
    slave_lists_.resize(HASH_SLOTS);
    for(size_t i = 0; i<slots_.size(); i++) {
        slots_[i] = NULL;
    }

    if( !lazy && load_slots_cache()<0 ) {
        return -1;
    }

    if( lazy ) {
        load_slots_asap_ = true;
    }

    return 0;

}

redisReply* Cluster::run(const std::vector<std::string> &commands) {
    std::vector<const char *> argv;
    std::vector<size_t> argvlen;
    bool is_slave = false;

    if( commands.size()<2 ) {
        set_error(E_COMMANDS) << "none-key commands are not supported";
        return NULL;
    }

    std::string cmd = to_upper(commands[0]);

    do {
        std::ostringstream ss;
        ss << "#" << cmd << "#";
        if( strstr(UNSUPPORT, ss.str().c_str()) ) {
            set_error(E_COMMANDS) << "command [" << cmd << "] not supported";
            return NULL;
        }
        if (strstr(SLAVE, ss.str().c_str())) {
            is_slave = true;
        }
    } while(0);

    for( size_t i=0; i<commands.size(); i++ ) {
        argv.push_back(commands[i].c_str());
        argvlen.push_back(commands[i].length());
    }

    return redis_command_argv(commands[1], argv.size(), argv.data(), argvlen.data(), is_slave);
}

bool Cluster::add_node_list(const std::string &host, int port, std::vector<Node *> &nlist) {
    Node *node = new Node(host, port, timeout_, password_);
    rcassert( node );
    LockGuard lg(np_lock_);

    std::pair<NodePoolType::iterator, bool> reti = slave_node_pool_.insert(node);
    nlist.push_back( *(reti.first) );

    if(reti.second) {
        return true;
    } else {
        delete node;
        return false;
    }
}

bool Cluster::add_node(const std::string &host, int port, Node *&rpnode) {
    Node *node = new Node(host, port, timeout_, password_);
    rcassert( node );

    LockGuard lg(np_lock_);

    std::pair<NodePoolType::iterator, bool> reti = node_pool_.insert(node);
    rpnode = *(reti.first);

    if(reti.second) {
        return true;
    } else {
        delete node;
        return false;
    }
}

int Cluster::parse_startup(const char *startup) {
    char *p1, *p2, *p3;
    char *tmp = (char *)malloc( strlen(startup)+1 );
    memcpy(tmp, startup, strlen(startup)+1);
    int  port;
    std::string host;

    p1 = p2 = tmp;
    do {
        p2 = strchr(p1, ',');
        if( p2 ) {
            *p2 = '\0';
        }
        p3 = strchr(p1, ':');
        if( p3 ) {

            *p3 = '\0';
            port = atoi(p3+1); //get port

            while(p1<p3 && *p1==' ')//trim left
                p1++;

            p3--;
            while(p3>p1 && *p3==' ')//trim right
                *(p3--) = '\0';

            host = p1;   //get host

            Node *node_in_pool;
            bool ret = add_node(host, port, node_in_pool);
            if(ret) {
                DEBUGINFO("parse startup add " << node_in_pool->simple_dump());
            } else {
                DEBUGINFO("parse startup duplicate " << node_in_pool->simple_dump() << " ignored");
            }
        }
        if( p2 )
            p1 = p2+1;
        else
            break;
    } while(1);

    free( tmp );
    return node_pool_.size();
}

int Cluster::load_slots_cache() {

    int start, end;
    int count = 0;
    redisReply *reply, *subr, *innr, *subsub;
    Node *node;
    std::vector<Node *> node_seeds;

    if(pthread_spin_trylock(&load_slots_lock_) != 0) {
        return 0;   // only one thread is allowed to process loading
    }

    DEBUGINFO("load_slots_cache loading start...");

    {
        LockGuard lg(np_lock_);
        NodePoolType::iterator iter = node_pool_.begin();
        for(; iter != node_pool_.end(); iter++) {
            node_seeds.push_back(*iter);
        }
    }

    for(size_t node_idx = 0; node_idx < node_seeds.size(); node_idx++) {
        node = node_seeds[node_idx];

        redisContext *c = (redisContext *)node->get_conn();
        if( !c ) {
            continue;
        }

        reply = (redisReply *)redisCommand(c, "cluster slots");
        if( !reply ) {
            node->put_conn(c);
            continue;
        } else if( reply->type==REDIS_REPLY_ERROR ) {
            freeReplyObject(reply);
            node->put_conn(c);
            continue;
        }

//78) 1) (integer) 4301            subr->element[0]
//    2) (integer) 4505            subr->element[1]
//    3) 1) "10.10.18.187"         subr->element[2]
//       2) (integer) 7001
//       3) "1d065b2adf206ffe567fc9be161e8dc1907457d7"
//    4) 1) "10.10.18.186"         reply->element[i]->elements, this case is 4.
//       2) (integer) 6906
//       3) "ac9656dd68845711522af787c01c336e1a93a2cb"
//79) 1) (integer) 8397
//    2) (integer) 8601
//    3) 1) "10.10.18.191"
//       2) (integer) 7402
//       3) "aadaee9707008210b875b088d1b7bc27c46b076e"
//    4) 1) "10.10.18.190"
//       2) (integer) 7307
//       3) "d25c92858a5b51769954e2b72a15fb5c88f5f229"
        for(size_t i=0; i<reply->elements; i++) {
            DEBUGINFO("reply->elements num: " << reply->elements << std::endl);
            subr = reply->element[i];
            DEBUGINFO("subr num: " << reply->element[i]->elements << std::endl);
            if( subr->elements<3
                || subr->element[0]->type!=REDIS_REPLY_INTEGER
                || subr->element[1]->type!=REDIS_REPLY_INTEGER
                || subr->element[2]->type!=REDIS_REPLY_ARRAY )
                continue;

            start = subr->element[0]->integer;
            end = subr->element[1]->integer;
            innr = subr->element[2];

            if( innr->elements<2
                || innr->element[0]->type!=REDIS_REPLY_STRING
                || innr->element[1]->type!=REDIS_REPLY_INTEGER )
                continue;

            Node *node_in_pool;
            bool ret = add_node(innr->element[0]->str, innr->element[1]->integer, node_in_pool);
            if(ret) {
               DEBUGINFO("insert new node "<< node_in_pool->simple_dump()<< " from cluster slots map" );
            }

            for(int jj=start; jj<=end; jj++)
                slots_[jj] = node_in_pool;

            count += (end-start+1);
            // -----------add slave list-----------------
            // reply->element[i]->element[k]  k = 2, master; k > 2, slaves
            std::vector<Node* > slave_node_list;
            std::vector<Node* >::iterator it;
            for (size_t k = 3; k < subr->elements; k++) {
               subsub = subr->element[k];
               std::string ip = subsub->element[0]->str;
               int port = subsub->element[1]->integer;
               ret = add_node_list(ip, port, slave_node_list);
            }
            std::vector<Node* > swap_slave_node_list;
            for (int kk=start; kk<=end; kk++) {
               swap_slave_node_list.assign(slave_node_list.begin(), slave_node_list.end());
               if (slave_node_list.size() == 0) {
                  // if no slave, put master_node to slave_list_
                  swap_slave_node_list.push_back(node_in_pool);
               }
               slave_lists_[kk].swap(swap_slave_node_list);
            }
            // ---------end of adding slave list--------
        }//for i

        freeReplyObject(reply);
        node->put_conn(c);
        break;

    }//for citer

    if( count>0 )  {
        DEBUGINFO("load_slots_cache count " << count <<"("<< (count == HASH_SLOTS? "complete":"incomplete!")<<") from " << node->simple_dump());
    } else {
        DEBUGINFO("load_slots_cache fail from all startup node");
    }

    DEBUGINFO("load_slots_cache loading finished");

    pthread_spin_unlock(&load_slots_lock_);
    return count;
}

Node *Cluster::get_random_node_from_list(const std::vector<Node *> &slave_node_list) {
    if (slave_node_list.empty())
      return NULL;
    srand(unsigned(time(NULL)));
    int idx = rand() % slave_node_list.size();
    return slave_node_list[idx];
}

int Cluster::clear_slots_cache() {
    slots_.clear();
    slots_.resize(HASH_SLOTS);
    for(size_t i = 0; i<slots_.size(); i++) {
        slots_[i] = NULL;
    }
    return 0;
}

Node *Cluster::get_random_node(const Node *last) {

    struct timeval tp;
    gettimeofday(&tp, NULL);

    LockGuard lg(np_lock_);

    std::size_t len = node_pool_.size();
    if( len==0 )
        return NULL;

    NodePoolType::iterator iter = node_pool_.begin();
    std::advance( iter, tp.tv_usec % len );//random position
    for(size_t i = 0; i < len; i++,iter++) {
        if(iter == node_pool_.end())
            iter = node_pool_.begin();
        DEBUGINFO("get_random_node try "<<(*iter)->simple_dump());
        if(*iter != last) {
            return *iter;
        }
    }

    return NULL;
}

uint16_t Cluster::get_key_hash(const std::string &key) {
    std::string::size_type pos1, pos2;
    std::string hashing_key = key;

    pos1 = key.find("{");
    if( pos1!=std::string::npos ) {
        pos2 = key.find("}", pos1+1);
        if((pos2!=std::string::npos) && (pos2 != pos1+1)) {
            hashing_key = key.substr(pos1+1, (pos2-pos1)-1);
        }
    }
    return crc16(hashing_key.c_str(), hashing_key.length());
}

redisReply* Cluster::redis_command_argv(const std::string& key, int argc, const char **argv, const size_t *argvlen, bool is_slave) {

#define MAX_TTL 5

    int ttl = MAX_TTL;
    Node *node = NULL;
    redisContext *c = NULL;
    redisReply *reply = NULL;
    bool try_random_node = false;
    bool try_other_slave = false;

    set_error(E_OK);

    if( load_slots_asap_ ) {
        load_slots_asap_ = false;
        load_slots_cache();
    }

    uint16_t hashing = get_key_hash(key);
    const int slot = hashing % HASH_SLOTS;

    while( ttl>0 ) {
        ttl--;
        specific_data().ttls = (MAX_TTL - ttl);
        DEBUGINFO("ttl " << ttl);

        if( try_random_node && !try_other_slave) {

            try_random_node = false;
            DEBUGINFO("try random node");
            node = get_random_node(node);
            if( !node ) {
                set_error(E_IO) << "try random node: no avaliable node";
                return NULL;
            }
            DEBUGINFO("slot " << slot << " use random " << node->simple_dump());
        } else {//find slot
            try_other_slave = false;// if try all slave_list fail, then try master
            if (is_slave) {
                node = get_random_node_from_list(slave_lists_[slot]);
            }
            if (node == NULL) {
                node = slots_[slot];
            }
            if( !node ) { //not hit
                DEBUGINFO("slot "<<slot<<" don't have node, try connection from random node.");
                try_random_node = true;//try random next ttl
                continue;
            }
            // std::cout << "slot " << slot << " hit at " << node->simple_dump() << std::endl;
            DEBUGINFO("slot " << slot << " hit at " << node->simple_dump());
        }

        if(is_slave)
          c = (redisContext*)node->get_readonly_conn();
        else
          c = (redisContext*)node->get_conn();
        if( !c ) {
            DEBUGINFO("get connection fail from " << node->simple_dump());
            try_random_node = true;//try random next ttl
            if (is_slave && ttl > MAX_TTL/2) {
               try_other_slave = true;  // try all slave_list
            }
            continue;
        }

        reply = (redisReply *)redisCommandArgv(c, argc, argv, argvlen);
        if( !reply ) {//next ttl

            DEBUGINFO("redisCommandArgv error. " << c->errstr << "(" << c->err << ")");
            set_error(E_IO) << "redisCommandArgv error. " << c->errstr << "(" << c->err << ")";
            if (is_slave)
              node->put_readonly_conn(c);
            else
              node->put_conn(c);
            try_random_node = true;//try random next ttl
            continue;

        } else if( reply->type==REDIS_REPLY_ERROR
                   &&(!strncmp(reply->str,"MOVED",5) || !strcmp(reply->str,"ASK")) ) { //next ttl
            char *p = reply->str, *s;
            /*
                     * [S] for pointer 's'
                     * [P] for pointer 'p'
                     */
            s = strchr(p,' ');      /* MOVED[S]3999 127.0.0.1:6381 */
            p = strchr(s+1,' ');    /* MOVED[S]3999[P]127.0.0.1:6381 */
            *p = '\0';

            rcassert( slot == atoi(s+1) );

            s = strchr(p+1,':');    /* MOVED 3999[P]127.0.0.1[S]6381 */
            *s = '\0';

            Node *node_in_pool;
            bool ret = add_node(p+1, atoi(s+1), node_in_pool);
            if(ret) {
                DEBUGINFO("insert new node "<< node_in_pool->simple_dump()<< " from redirection" );
            } else {
                DEBUGINFO("redirect slot "<< slot <<" to " << node_in_pool->simple_dump());
            }

            slots_[slot] = node_in_pool;

            load_slots_asap_ = true;//cluster nodes must have being changed, load slots cache as soon as possible.
            freeReplyObject( reply );
            if (is_slave)
              node->put_readonly_conn(c);
            else
              node->put_conn(c);
            continue;

        }
        if (is_slave)
          node->put_readonly_conn(c);
        else
          node->put_conn(c);
        return reply;
    }

    set_error(E_TTL) << "max ttl fail";
    return NULL;

#undef MAX_TTL
}

std::vector<redisReply*> Cluster::run_pipeline(const std::vector<std::string> &pipeline_commands){
  int size = pipeline_commands.size();
  std::map<int,std::vector<std::pair<int,std::string> > > map_commands;
  std::map<int,std::vector<std::pair<int,std::string> > > map_commands_slave;
  std::vector<redisReply*> reply_res(size);
  for(int i=0;i<size;i++){
    std::vector<std::string> commands;
    SplitString(pipeline_commands[i], commands, " ");
    if( commands.size()<2 ) {
      set_error(E_COMMANDS) << "none-key commands are not supported";
      continue;
    }
    std::string cmd = to_upper(commands[0]);
    std::ostringstream ss;
    ss << "#" << cmd << "#";
    if( strstr(UNSUPPORT, ss.str().c_str()) ) {
      set_error(E_COMMANDS) << "command [" << cmd << "] not supported";
      continue;
    }
    uint16_t hashing = get_key_hash(commands[1]);
    int slot = hashing % HASH_SLOTS;
    if (strstr(SLAVE, ss.str().c_str())) {
      std::pair<int,std::string> temp(i,pipeline_commands[i]);
      map_commands_slave[slot].push_back(temp);
    }else{
      std::pair<int,std::string> temp(i,pipeline_commands[i]);
      map_commands[slot].push_back(temp);
    }
  }
  std::map<int,std::vector<std::pair<int,std::string> > >::iterator it;
  for(it=map_commands.begin();it!=map_commands.end();it++){
    std::vector<redisReply*> reply_vec = redis_command_pipeline(it->first,it->second,false);
    for(int i=0;i<reply_vec.size();i++){
      reply_res[it->second[i].first]=reply_vec[i];
    }
  }
  for(it=map_commands_slave.begin();it!=map_commands_slave.end();it++){
    std::vector<redisReply*> reply_vec = redis_command_pipeline(it->first,it->second,true);
    for(int i=0;i<reply_vec.size();i++){
        reply_res[it->second[i].first]=reply_vec[i];
    }
  }
  return reply_res;
}

std::vector<redisReply*> Cluster::redis_command_pipeline(int slot,std::vector<std::pair<int,std::string> >commands,bool is_slave){
  Node *node = NULL;
  redisContext *c = NULL;
  redisReply *reply = NULL;
  std::vector<redisReply*> reply_vec;
  std::vector<redisReply*> reply_null(commands.size());
  bool try_random_node = false;
  bool try_other_slave = false;
  set_error(E_OK);
  if( load_slots_asap_ ) {
    load_slots_asap_ = false;
    load_slots_cache();
  }

  if( try_random_node && !try_other_slave) {
    try_random_node = false;
    DEBUGINFO("try random node");
    node = get_random_node(node);
    if( !node ) {
      set_error(E_IO) << "try random node: no avaliable node";
      std::cerr<<"(error)"<<"try random node: no avaliable node"<<std::endl;
      return reply_null;
    }
    DEBUGINFO("slot " << slot << " use random " << node->simple_dump());
  } else {
    try_other_slave = false;
    if (is_slave) {
      node = get_random_node_from_list(slave_lists_[slot]);
    }
    if (node == NULL) {
      node = slots_[slot];
    }
    if( !node ) {
      DEBUGINFO("slot "<<slot<<" don't have node, try connection from random node.");
      std::cerr<<"(error)"<<"slot "<<slot<<" don't have node, try connection from random node.";
      return reply_null;
    }
    DEBUGINFO("slot " << slot << " hit at " << node->simple_dump());
  }
  if(is_slave)
    c = (redisContext*)node->get_readonly_conn();
  else
    c = (redisContext*)node->get_conn();
  if( !c ) {
    DEBUGINFO("get connection fail from " << node->simple_dump());
    std::cerr<<"(error)"<<"get connection fail from " << node->simple_dump();
    return reply_null;
  }

  for(int i=0;i<commands.size();i++){
    const char* temp = commands[i].second.data();
    redisAppendCommand(c,temp);
  }
  for(int i=0;i<commands.size();i++){
    redisGetReply(c,(void**)&reply);
    if( !reply ) {
      std::cerr << "(error) "<<std::endl;
    } else if( reply->type==REDIS_REPLY_ERROR ) {
      std::cerr << "(error) " << reply->str << std::endl;
    } else {
      reply_vec.push_back(reply);
    }
  }

  return reply_vec;
}

Cluster::ThreadDataType &Cluster::specific_data() {
    ThreadDataType *pd = (ThreadDataType *)pthread_getspecific(key_);
    if(!pd) {
        pd =  new ThreadDataType;
        pd->err = E_OK;
        pd->ttls  = 0;
        rcassert(pd);
        int ret = pthread_setspecific(key_, (void *)pd);
        rcassert(ret == 0);
    }
    return *pd;
};

std::ostringstream& Cluster::set_error(ErrorE e) {
    ThreadDataType &sd = specific_data();
    sd.err = e;
    sd.strerr.str("");
    return sd.strerr;
}

int Cluster::err() {
    return specific_data().err;
}
std::string Cluster::strerr() {
    return specific_data().strerr.str();
}
int Cluster::ttls() {
    return specific_data().ttls;
}
std::string Cluster::stat_dump() {
    std::ostringstream ss;

    LockGuard lg(np_lock_);

    ss<<"Cluster have "<<node_pool_.size() <<" nodes: ";

    for(NodePoolType::iterator iter = node_pool_.begin(); iter != node_pool_.end(); iter++) {
        ss<< "\r\n" <<(*iter)->stat_dump();
    }
    ss << "\r\n";

    return ss.str();
}

int Cluster::test_parse_startup(const char *startup) {
    return parse_startup( startup );
}

Cluster::NodePoolType & Cluster::get_startup_nodes() {
    return node_pool_;
}
int Cluster::test_key_hash(const std::string &key) {
    return get_key_hash(key);
}

}//namespace cluster
}//namespace redis
