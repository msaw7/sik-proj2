#ifndef SIKRADIO_ERR_H
#define SIKRADIO_ERR_H

#include <stdnoreturn.h>

/*
Consciously discards the return value of a function.
*/
#define IGNORE_RESULT(x) do { __typeof__(x) _r = (x); (void)_r; } while(0)

enum Verbosity {
    NONE,
    COMMUNICATION,
    CRITICAL,
    NONCRITICAL,
    DIAGNOSTIC,
};

/* 
Prints information about a system error and quits.
*/
[[noreturn]] void syserr(int v, const char* fmt, ...);

/*
Prints information about an error and quits.
*/
[[noreturn]] void fatal(int v, const char* fmt, ...);

/*
Prints information about a non-critical error (and does not quit).
*/
void noncritical(int v, const char* fmt, ...);

#endif
