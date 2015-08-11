#ifndef __COMMON_CODING_CODINGHANDLER_HH__
#define __COMMON_CODING_CODINGHANDLER_HH__

#include "coding.hh"
#include "coding_scheme.hh"
#include "all_coding.hh"

struct CodingHandler {
    CodingScheme scheme;   
    union {
        Raid5Coding2* raid5;
        RDPCoding* rdp;
        CauchyCoding* cauchy;
        RSCoding* rs;
        EvenOddCoding* evenodd;
    };
};

#endif
