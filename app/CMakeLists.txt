
#set(SOURCES
#    "src/main.cpp"
#)
#add_library(app_lib STATIC ${SOURCES})

#target_link_libraries(proto-objects PUBLIC protobuf::libprotobuf)

#set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")

#target_include_directories(app_lib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" ${Boost_INCLUDE_DIRS})

add_executable(my_app
    "src/main.cpp"
)
#target_link_libraries(my_app)
