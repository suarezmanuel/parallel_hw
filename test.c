#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
int main(int argc, char** argv)
{
    char c1 = 'a', c2 = 'a';
    int f1 = open(argv[1], O_RDONLY);
    int f2 = open(argv[2], O_RDONLY);
    if (!f1 && !f2) exit(2); else if (!f1 || !f2) exit(1);

    while (c1 == c2)
    {
        // if both at end of file
        int size1 = read(f1, &c1, 1);
        int size2 = read(f2, &c2, 1);
        if (size1 == 0 && size2 == 0) { close(f1);close(f2);exit(2); }
    }
    close(f1); close(f2);
    exit(1);
}