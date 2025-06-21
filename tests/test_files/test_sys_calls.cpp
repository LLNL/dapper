// File: test_ls.cpp
#include <iostream>
#include <cstdlib>   // for system()
#include <unistd.h>  // for execlp()
#include <cerrno>    // for errno
#include <cstring>   // for strerror()

int main(int argc, char* argv[]) {
    std::cout << "=== Listing via system() ===\n";
    int ret = system("ls -l /tmp");
    if (ret == -1) {
        std::cerr << "  [ERROR] system() failed: "
                  << std::strerror(errno) << "\n";
    }

    std::cout << "\n=== Listing via execlp() ===\n";
    // execlp replaces the current process image on success,
    // so this code will only reach perror on failure.
    if (execlp(
            "ls",   // program to run (searched in $PATH)
            "ls",   // argv[0]
            "-a",   // argv[1]
            "/etc", // argv[2]
            nullptr // argv must be null-terminated
        ) == -1)
    {
        std::perror("  [ERROR] execlp() failed");
    }

    return 0;
}