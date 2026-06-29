add_library(simkv_compile_flags INTERFACE)

target_compile_options(simkv_compile_flags INTERFACE
    -Wall
    -Wextra
    -Wpedantic
    -Werror
    -Wno-unused-parameter
    -Wno-missing-field-initializers
)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(simkv_compile_flags INTERFACE
        -fsanitize=address,undefined
        -fno-omit-frame-pointer
    )
    target_link_options(simkv_compile_flags INTERFACE
        -fsanitize=address,undefined
    )
endif()
