set (FDBUNLOCKER_SRCS
  fdbunlocker.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbunlocker SRCS ${FDBUNLOCKER_SRCS})
target_link_libraries(fdbunlocker PRIVATE fdbclient)

fdb_install(TARGETS fdbunlocker DESTINATION bin COMPONENT clients)
