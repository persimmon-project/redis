cmake_minimum_required(VERSION 3.10)

project(redis-lwip)

message(STATUS "Host is: ${CMAKE_HOST_SYSTEM}.  Build target is: ${CMAKE_SYSTEM}")
get_filename_component(REDIS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)
message(STATUS "Project root directory is: ${REDIS_ROOT}")

file(GLOB HEADERS "src/*.h")

add_executable(redis-server
    src/adlist.c
    src/quicklist.c
    src/ae.c
    src/anet.c
    src/dict.c
    src/server.c
    src/sds.c
    src/zmalloc.c
    src/lzf_c.c
    src/lzf_d.c
    src/pqsort.c
    src/zipmap.c
    src/sha1.c
    src/ziplist.c
    src/release.c
    src/networking.c
    src/util.c
    src/object.c
    src/db.c
    src/replication.c
    src/rdb.c
    src/t_string.c
    src/t_list.c
    src/t_set.c
    src/t_zset.c
    src/t_hash.c
    src/config.c
    src/aof.c
    src/pubsub.c
    src/multi.c
    src/debug.c
    src/sort.c
    src/intset.c
    src/syncio.c
    src/cluster.c
    src/crc16.c
    src/endianconv.c
    src/slowlog.c
    src/scripting.c
    src/bio.c
    src/rio.c
    src/rand.c
    src/memtest.c
    src/crc64.c
    src/bitops.c
    src/sentinel.c
    src/notify.c
    src/setproctitle.c
    src/blocked.c
    src/hyperloglog.c
    src/latency.c
    src/sparkline.c
    src/redis-check-rdb.c
    src/redis-check-aof.c
    src/geo.c
    src/lazyfree.c
    src/module.c
    src/evict.c
    src/expire.c
    src/geohash.c
    src/geohash_helper.c
    src/childinfo.c
    src/defrag.c
    src/siphash.c
    src/rax.c
    ${HEADERS}
)

target_include_directories(redis-server
    PRIVATE ${REDIS_ROOT}/deps/hiredis
    PRIVATE ${REDIS_ROOT}/deps/linenoise
    PRIVATE ${REDIS_ROOT}/deps/lua/src
)

target_link_libraries(redis-server
    PRIVATE ${REDIS_ROOT}/deps/hiredis/libhiredis.a
    PRIVATE ${REDIS_ROOT}/deps/lua/src/liblua.a
    m dl pthread
)

add_executable(redis-benchmark
    src/ae.c
    src/anet.c
    src/redis-benchmark.c
    src/adlist.c
    src/zmalloc.c
    ${HEADERS}
)

target_include_directories(redis-benchmark
    PRIVATE ${REDIS_ROOT}/deps/hiredis
)

target_link_libraries(redis-benchmark
    PRIVATE ${REDIS_ROOT}/deps/hiredis/libhiredis.a
    PRIVATE m
    PRIVATE dl
    PRIVATE pthread
)
