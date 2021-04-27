#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <hiredis/hiredis.h>
#include "../redis_cluster.h"

int main(int argc, char *argv[]) {
    std::string startup = "10.10.160.113:6702";
    if( argc>1 ) {
        startup = argv[1];
    }
    std::cout << "cluster startup with " << startup << std::endl;
    redis::cluster::Cluster *cluster = new redis::cluster::Cluster();

    if( cluster->setup(startup.c_str(), true)!=0 ) {
        std::cerr << "cluster setup fail" << std::endl;
        return 1;
    }

    std::vector<std::string> commands;

    std::cerr << "run pipeline ..." << std::endl;
    commands.push_back("SET foo hello");
    commands.push_back("GET foo");
    commands.push_back("SET foo1 hello1");
    commands.push_back("GET foo1");
    commands.push_back("SET foo2 hello2");
    commands.push_back("GET foo2");
    commands.push_back("SET foo3 hello3");
    commands.push_back("GET foo3");
    commands.push_back("SET foo4 world4");
    commands.push_back("GET foo4");
    commands.push_back("GET foo");
    commands.push_back("GET foo1");
    commands.push_back("GET foo2");
    commands.push_back("GET foo3");
    commands.push_back("GET foo4");
    commands.push_back("GET foo");
    commands.push_back("GET foo");
    commands.push_back("GET foo");
    commands.push_back("GET foo3");
    commands.push_back("GET foo4");

    for(int i =0;i<commands.size();i++){
      std::cout <<commands[i]<<";";
    }
    std::cout <<std::endl;

    std::vector<redisReply *> res_reply;
    res_reply = cluster->run_pipeline(commands);
    for(int i =0;i<res_reply.size();i++){
      if( !res_reply[i] ) {
          std::cerr << "(error) " << cluster->strerr() << ", " << cluster->err() << std::endl;
      } else if( res_reply[i]->type==REDIS_REPLY_ERROR ) {
          std::cerr << "(error) " << res_reply[i]->str << std::endl;
      } else {
        std::cout <<res_reply[i]->str<<std::endl;
      }
      if( res_reply[i] )
        freeReplyObject( res_reply[i] );
    }
    delete cluster;
    return 0;
}
