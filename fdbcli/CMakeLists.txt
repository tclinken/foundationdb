set(FDBCLI_SRCS
  fdbcli.actor.cpp
  FlowLineNoise.actor.cpp
  FlowLineNoise.h
  linenoise/linenoise.h)

if(NOT WIN32)
  list(APPEND FDBCLI_SRCS linenoise/linenoise.c)
endif()

add_flow_target(EXECUTABLE NAME fdbcli SRCS ${FDBCLI_SRCS})
add_flow_target(EXECUTABLE NAME fdbcli-rw SRCS ${FDBCLI_SRCS})
target_compile_definitions(fdbcli-rw PRIVATE FDBCLI_RW)
target_link_libraries(fdbcli PRIVATE fdbclient)
target_link_libraries(fdbcli-rw PRIVATE fdbclient)

fdb_install(TARGETS fdbcli DESTINATION bin COMPONENT clients)
fdb_install(TARGETS fdbcli-rw DESTINATION bin COMPONENT clients)
