#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "err.h"

[[noreturn]] void syserr(int v, const char* fmt, ...) {
    if(v < CRITICAL) {
        exit(1); // Silent exit.
    }
    va_list fmt_args;
    int org_errno = errno;

    fprintf(stderr, "\tERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);

    fprintf(stderr, " (%d; %s)\n", org_errno, strerror(org_errno));
    fflush(stderr);
    exit(1);
}

[[noreturn]] void fatal(int v, const char* fmt, ...) {
    if(v < CRITICAL) {
        exit(1); // Silent exit.
    }
    va_list fmt_args;

    fprintf(stderr, "\tERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);

    fprintf(stderr, "\n");
    fflush(stderr);
    exit(1);
}

void noncritical(int v, const char* fmt, ...) {
    if(v < NONCRITICAL) {
        return;
    }
    va_list fmt_args;

    fprintf(stderr, "\tRECOVERABLE ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);

    fprintf(stderr, "\n");
    fflush(stderr);
    return;
}
