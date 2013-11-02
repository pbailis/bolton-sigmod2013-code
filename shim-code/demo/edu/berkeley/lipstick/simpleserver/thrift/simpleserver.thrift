
namespace java edu.berkeley.lipstick.simpleserver

service SimpleServerService {
    binary get(1:string key),
    void put(1:string key, 2:binary value, 3:i64 timestamp),
}                        

