target("mmapBuffer")
    set_kind("static")
    set_languages("cxx20")
    add_files("code/*.cpp")

target("test")
    set_kind("binary")
    add_files("test/*.cpp")
    add_deps("mmapBuffer")
    set_languages("cxx20")
    add_syslinks("pthread")