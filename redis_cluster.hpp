/* Copyright (C) 
 * 2015 - supergui@live.cn
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * A c++ client library for redis cluser, simple wrapper of hiredis.
 * Inspired by antirez's (antirez@gmail.com) redis-rb-cluster.
 *
 */
#ifndef REDIS_CLUSTER_H_
#define REDIS_CLUSTER_H_

#include <string>
#include <vector>
#include <sstream>
#include <unordered_map>

struct redisReply;

namespace redis {
namespace cluster {

class Reply;
class Cluster {
public:
    const static int HASH_SLOTS = 16384;

    enum ErrorE {
        E_OK = 0, 
        E_COMMANDS = 1,
        E_SLOT_MISSED = 2,
        E_IO = 3,
        E_TTL = 4,
        E_OTHER = 5 
    };

    typedef struct NodeInfoS{
        std::string host;
        int port;
        
        /**
         * A comparison function for equality; 
         * This is required because the hash cannot rely on the fact 
         * that the hash function will always provide a unique hash value for every distinct key 
         * (i.e., it needs to be able to deal with collisions), 
         * so it needs a way to compare two given keys for an exact match. 
         * You can implement this either as a class that overrides operator(), 
         * or as a specialization of std::equal, 
         * or – easiest of all – by overloading operator==() for your key type (as you did already).
         */
        bool operator==(const struct NodeInfoS &other) const {
            return (host==other.host && port==other.port);
        }
    }NodeInfoType, *NodeInfoPtr, &NodeInfoRef;

    /**
     * A hash function; 
     * this must be a class that overrides operator() and calculates the hash value given an object of the key-type. 
     * One particularly straight-forward way of doing this is to specialize the std::hash template for your key-type.
     */
    struct KeyHasherS {
        std::size_t operator()(const NodeInfoType &node) const {
            return (std::hash<std::string>()(node.host) ^ std::hash<int>()(node.port));
        }
    };

    typedef std::unordered_map<NodeInfoType, void *, KeyHasherS> ConnectionsType;//NodeInfoType=>redisContext
    typedef ConnectionsType::iterator ConnectionsIter;
    typedef ConnectionsType::const_iterator ConnectionsCIter;

    Cluster();
    virtual ~Cluster();
    
    /**
     *  Setup with startup nodes.
     *  Immediately loading slots cache from startup nodes.
     *
     * @param 
     *  startup - '127.0.0.1:7000, 127.0.0.1:8000'
     *
     * @return 
     *  0 - success
     *  <0 - fail
     */
    int setup(const char *startup);

    /**
     * Caller should call freeReplyObject to free reply.
     *
     * @return 
     *  not NULL - succ
     *  NULL     - error
     *             get the last error message with function err() & errstr() 
     */
    redisReply* run(const std::vector<std::string> &commands);

    inline int err() const { return errno_; }
    inline std::string errstr() const { return error_.str(); } 

public:/* for unittest */
    int test_parse_startup(const char *startup);
    std::vector<NodeInfoType>& get_startup_nodes();
    int test_key_hash(const std::string &key);

private:
    std::ostringstream& set_error(ErrorE e);
    int parse_startup(const char *startup);
    int load_slots_cache();
    int clear_slots_cache();

    /**
     *  Support hash tag, which means if there is a substring between {} bracket in a key, only what is inside the string is hashed.
     *  For example {foo}key and other{foo} are in the same slot, which hashed with 'foo'.
     */
    uint16_t get_key_hash(const std::string &key);

    /**
     *  Agent for connecting and run redisCommandArgv.
     *  Max ttl(5 default) retries or redirects.
     *
     * @return 
     *  not NULL: success, return the redisReply object. The caller should call freeReplyObject to free reply object.
     *  NULL    : error
     */
    redisReply* redis_command_argv(const std::string& key, int argc, const char **argv, const size_t *argvlen);

    std::vector<NodeInfoType> startup_nodes_;
    ConnectionsType connections_;
    std::vector<NodeInfoType> slots_;

    ErrorE errno_;
    std::ostringstream error_;
};
#if 0
class Reply {

    /**
     * Shame to steal from redis3m::reply.
     * A copy of redisReply.
     */
public:

    /**
     * As same defination as hiredis/hiredis.h
     * It's a good idea translate from hiredis's defination to local, which to avoid at risk of future change.
     */
    enum TypeE {
        T_STRING = 1,
        T_ARRAY = 2,
        T_INTEGER = 3,
        T_NIL = 4,
        T_STATUS = 5,
        T_ERROR = 6
    };
    
    inline TypeE type() const { return type_; }
    inline const std::string& str() const { return str_; }
    inline long long integer() const { return integer_; }
    inline const std::vector<Reply>& elements() const { return elements_; }

private:
    Reply(const redisReply *rp);

    TypeE type_;
    std::string str_;
    long long integer_;
    std::vector<Reply> elements_;
};
#endif
}//namespace cluster
}//namespace redis

#endif
