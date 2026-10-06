#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/event.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

static uint64_t now_ns() {
  timespec t{}; clock_gettime(CLOCK_MONOTONIC,&t);
  return uint64_t(t.tv_sec)*1000000000ULL+t.tv_nsec;
}
struct Message { uint64_t seq; uint64_t sent_ns; };
struct Partial { std::array<char,sizeof(Message)> bytes{}; size_t used=0; };
struct Pair { int send_fd=-1, recv_fd=-1; };
static void fail(const char* what){perror(what);exit(2);}
static void summary(const char* name,std::vector<int64_t> v){
  std::sort(v.begin(),v.end());long double sum=0;for(auto x:v)sum+=x;
  auto q=[&](double p){return double(v[size_t(p*(v.size()-1))])/1000;};
  printf("%s mean=%.3Lf p50=%.3f p99=%.3f max=%.3f us\n",name,sum/v.size()/1000,q(.5),q(.99),double(v.back())/1000);
}
int main(int argc,char**argv){
  if(argc!=5){fprintf(stderr,"usage: probe tcp|unix SOCKETS MESSAGES INTERVAL_NS\n");return 2;}
  bool tcp=strcmp(argv[1],"tcp")==0;int sockets=atoi(argv[2]);int messages=atoi(argv[3]);uint64_t interval=strtoull(argv[4],nullptr,10);
  if(sockets<1||messages<sockets||interval<1000)return 2;
  rlimit limit{rlim_t(std::max(8192,sockets*2+128)),rlim_t(std::max(8192,sockets*2+128))};setrlimit(RLIMIT_NOFILE,&limit);
  std::vector<Pair> pairs(sockets);
  int listener=-1;sockaddr_in addr{};socklen_t addr_len=sizeof(addr);
  if(tcp){
    listener=socket(AF_INET,SOCK_STREAM,0);if(listener<0)fail("listener");
    addr.sin_family=AF_INET;addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    if(bind(listener,(sockaddr*)&addr,sizeof(addr))<0||listen(listener,1024)<0||getsockname(listener,(sockaddr*)&addr,&addr_len)<0)fail("listen");
  }
  for(int i=0;i<sockets;++i){
    if(tcp){
      pairs[i].send_fd=socket(AF_INET,SOCK_STREAM,0);if(pairs[i].send_fd<0)fail("client");
      int one=1;setsockopt(pairs[i].send_fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
      if(connect(pairs[i].send_fd,(sockaddr*)&addr,sizeof(addr))<0)fail("connect");
      pairs[i].recv_fd=accept(listener,nullptr,nullptr);if(pairs[i].recv_fd<0)fail("accept");
    } else {
      int fd[2];if(socketpair(AF_UNIX,SOCK_STREAM,0,fd)<0)fail("socketpair");
      pairs[i].send_fd=fd[0];pairs[i].recv_fd=fd[1];
    }
    int flags=fcntl(pairs[i].recv_fd,F_GETFL);if(fcntl(pairs[i].recv_fd,F_SETFL,flags|O_NONBLOCK)<0)fail("nonblock");
  }
  int kq=kqueue();if(kq<0)fail("kqueue");
  std::vector<struct kevent> changes(sockets);
  for(int i=0;i<sockets;++i)EV_SET(&changes[i],pairs[i].recv_fd,EVFILT_READ,EV_ADD,0,0,(void*)(uintptr_t)i);
  if(kevent(kq,changes.data(),sockets,nullptr,0,nullptr)<0)fail("register");
  std::vector<uint64_t> sent(messages),send_done(messages),ready(messages),received(messages);
  std::vector<Partial> partial(sockets);
  std::thread rx([&]{
    int done=0;std::array<struct kevent,64> ev{};
    while(done<messages){
      int n=kevent(kq,nullptr,0,ev.data(),ev.size(),nullptr);uint64_t ready_at=now_ns();
      if(n<0)fail("kevent");
      for(int j=0;j<n;++j){
        int idx=(int)(uintptr_t)ev[j].udata;auto& p=partial[idx];
        for(;;){
          ssize_t got=recv(pairs[idx].recv_fd,p.bytes.data()+p.used,sizeof(Message)-p.used,MSG_DONTWAIT);
          if(got>0){
            p.used+=size_t(got);
            if(p.used==sizeof(Message)){
              Message msg{};memcpy(&msg,p.bytes.data(),sizeof(msg));
              if(msg.seq>=uint64_t(messages))abort();
              ready[msg.seq]=ready_at;received[msg.seq]=now_ns();++done;p.used=0;
            }
            continue;
          }
          if(got<0&&(errno==EAGAIN||errno==EWOULDBLOCK))break;
          if(got==0)break;
          if(got<0&&errno==EINTR)continue;
          fail("recv");
        }
      }
    }
  });
  uint64_t start=now_ns()+100000000ULL;
  for(int i=0;i<messages;++i){
    uint64_t target=start+uint64_t(i)*interval;
    while(now_ns()<target){}
    uint64_t before=now_ns();Message msg{uint64_t(i),before};
    ssize_t n=send(pairs[i%sockets].send_fd,&msg,sizeof(msg),0);
    if(n!=sizeof(msg))fail("send");
    sent[i]=before;send_done[i]=now_ns();
  }
  rx.join();
  std::vector<int64_t> syscall,send_to_ready,return_to_ready,ready_to_recv,send_to_recv;
  syscall.reserve(messages);send_to_ready.reserve(messages);return_to_ready.reserve(messages);ready_to_recv.reserve(messages);send_to_recv.reserve(messages);
  for(int i=0;i<messages;++i){
    syscall.push_back(int64_t(send_done[i]-sent[i]));
    send_to_ready.push_back(int64_t(ready[i])-int64_t(sent[i]));
    return_to_ready.push_back(int64_t(ready[i])-int64_t(send_done[i]));
    ready_to_recv.push_back(int64_t(received[i])-int64_t(ready[i]));
    send_to_recv.push_back(int64_t(received[i])-int64_t(sent[i]));
  }
  printf("mode=%s sockets=%d messages=%d interval_ns=%llu nodelay=%d\n",tcp?"tcp":"unix",sockets,messages,(unsigned long long)interval,tcp);
  summary("send_call",std::move(syscall));summary("send_to_kqueue_return",std::move(send_to_ready));
  summary("send_return_to_kqueue_return",std::move(return_to_ready));
  summary("kqueue_return_to_recv_return",std::move(ready_to_recv));
  summary("send_to_recv_return",std::move(send_to_recv));
  for(auto&p:pairs){close(p.send_fd);close(p.recv_fd);}close(kq);if(listener>=0)close(listener);
}
