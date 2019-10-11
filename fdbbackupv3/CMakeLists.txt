set(FDBBACKUP_SRCS
  main.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbbackupv3 SRCS ${FDBBACKUP_SRCS})
target_link_libraries(fdbbackupv3 PRIVATE fdbclient)

if(NOT OPEN_FOR_IDE)
  fdb_install(TARGETS fdbbackupv3 DESTINATION bin COMPONENT clients)
  install_symlink(
    COMPONENT clients
    FILE_DIR bin
    LINK_DIR bin
    FILE_NAME fdbbackupv3
    LINK_NAME fdbrestorev3)
endif()

if (GPERFTOOLS_FOUND)
  add_compile_definitions(USE_GPERFTOOLS)
  target_link_libraries(fdbbackupv3 PRIVATE gperftools)
endif()
