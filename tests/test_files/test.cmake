set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output)
include_external_msproject(OMEGA_WIN "${CMAKE_SOURCE_DIR}/external/OMEGA/projects/VS2022/OMEGA.vcxproj")

ExternalProject_Add(zeta
  # Test Comment 1
  URL https://github.com/fakeorg/zeta/releases/download/zeta-1.0.0/zeta-1.0.0-win32.zip
  URL_MD5 1234567890abcdef1234567890abcdef
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/external/zeta"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  UPDATE_COMMAND "" # Skip updates for every build
  INSTALL_COMMAND ""
)

ExternalProject_Add(epsilon
  URL https://gitlab.com/opensource-devs/epsilon/releases/download/2.5.1/epsilon-2.5.1.zip
  URL_MD5 abcdef1234567890abcdef1234567890
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/external/epsilon"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  UPDATE_COMMAND "" # Skip updates for every build
  INSTALL_COMMAND ""
)

ExternalProject_Add(theta
  URL https://github.com/fakeuser/theta/releases/download/4.3.2/theta-4.3.2.bin.WIN32.zip
  URL_MD5 fedcba0987654321fedcba0987654321
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/external/theta"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  UPDATE_COMMAND "" # Skip updates for every build
  INSTALL_COMMAND ""
)

ExternalProject_Add(sigma
  URL https://example.com/sigma-binaries/archive/v3.1.4.zip
  URL_MD5 0fedcba9876543210fedcba987654321
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/external/sigma"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  UPDATE_COMMAND "" # Skip updates for every build
  INSTALL_COMMAND ""
)

include_directories(
  ${CMAKE_BINARY_DIR}
  "${CMAKE_SOURCE_DIR}/app"
  #"${CMAKE_SOURCE_DIR}/external/omega/include"
  "${CMAKE_SOURCE_DIR}/external/theta/include"
  "${CMAKE_SOURCE_DIR}/external/zeta/include"
  "${CMAKE_SOURCE_DIR}/external/epsilon"
  "${CMAKE_SOURCE_DIR}/external/sigma/include/sigma2"
  "${CMAKE_SOURCE_DIR}/external/sigma/include"
  "${CMAKE_SOURCE_DIR}/external/OMEGA/src"
)

link_directories(
  ${CMAKE_CURRENT_BINARY_DIR}
  "${CMAKE_CURRENT_BINARY_DIR}/external/xi/Debug"
  "${CMAKE_SOURCE_DIR}/external/omega/libs"
  "${CMAKE_SOURCE_DIR}/external/zeta/lib/Release/Win32"
  "${CMAKE_SOURCE_DIR}/external/theta/lib-vc2022"
  "${CMAKE_SOURCE_DIR}/external/OMEGA/projects/VS2022/Debug"
  "${CMAKE_SOURCE_DIR}/external/sigma/win32"
  "${CMAKE_SOURCE_DIR}/external/psi/win32"
)

add_executable(myapp-gamma
  ${COMMON_SRCS}
  app/graphics/shaders/vertex_shader.cpp
  app/graphics/displaytheta.cpp
)

target_link_libraries(myapp-gamma
  OpenGL32
  zeta32s
  zeta32
  theta3
  sigma314
  #sigma6.dll
  xi
  OMEGA
)

add_dependencies(myapp-gamma zeta epsilon theta sigma xi OMEGA_WIN)

include(FetchContent)

if (MYPROJECT_SHARED)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif()

if (NOT TARGET alpha::alpha)
  FetchContent_Declare(alpha
    GIT_REPOSITORY "https://github.com/exampleorg/alpha-lib.git"
    GIT_TAG        "master"
    GIT_SHALLOW    ON)
  FetchContent_MakeAvailable(alpha)
endif()

if (NOT TARGET bravo::bravo)
  FetchContent_Declare(bravo
    GIT_REPOSITORY "https://gitlab.com/opensource/bravo.git"
    # ---- Test Comment 2
    GIT_TAG        "2.3.1"
    GIT_SHALLOW    ON)
  FetchContent_MakeAvailable(bravo)
endif()

if (NOT TARGET charlie::charlie)
  FetchContent_Declare(charlie
    GIT_REPOSITORY "https://github.com/fakeuser/charlie.git"
    GIT_SHALLOW    ON)
  FetchContent_MakeAvailable(charlie)
endif()

if (NOT TARGET delta::delta)
  set(DELTA_BUILD_TESTS OFF)
  set(DELTA_BUILD_DOCS OFF)
  FetchContent_Declare(delta
    GIT_REPOSITORY "https://bitbucket.org/deltateam/delta.git"
    GIT_TAG        "v0.9.8"
    GIT_SHALLOW    ON)
  FetchContent_MakeAvailable(delta)
endif()

if (MYPROJECT_WITH_FEATURE_X)
  if (NOT TARGET echo::echo)
    FetchContent_Declare(echo
      GIT_REPOSITORY "https://github.com/echodevs/echo.git"
      GIT_TAG        "v1.2.0"
      GIT_SHALLOW    ON)
    FetchContent_MakeAvailable(echo)
  endif()
endif()

if (MYPROJECT_ENABLE_TESTS)
  if (NOT TARGET Foxtrot::FoxtrotMain)
    FetchContent_Declare(foxtrot
      GIT_REPOSITORY "https://github.com/foxtrotorg/foxtrot.git"
      GIT_TAG        "v4.0.0"
      GIT_SHALLOW    ON)
    FetchContent_MakeAvailable(foxtrot)
  endif()
endif()