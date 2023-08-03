#include <string>
class Status
{

public:
    enum Code : unsigned char
    {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
        kMergeInProgress = 6,
        kIncomplete = 7,
        kShutdownInProgress = 8,
        kTimedOut = 9,
        kAborted = 10,
        kBusy = 11,
        kExpired = 12,
        kTryAgain = 13,
        kCompactionTooLarge = 14,
        kColumnFamilyDropped = 15,
        kMaxCode
    };
    Status(/* args */) : code(kOk){};
    Status(Code c):code(c){};
    Status(Code c,std::string s):code(c),msg(s){};
    ~Status();
    static Status OK() { return Status(); }
    static Status NotFound() { return Status(kNotFound); }
    static Status Error(std::string msg) { return Status(kCorruption,msg); }

private:
    /* data */
    Code code;
    std::string msg;
};
