/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FLOCK_EXCEPTION_HPP
#define __FLOCK_EXCEPTION_HPP

#include <flock/flock-common.h>
#include <exception>

namespace flock {

class Exception : public std::exception {

    public:

    Exception(flock_return_t code)
    : m_code(code) {}

    const char* what() const noexcept override {
        #define X(__err__, __msg__) case __err__: return __msg__;
        switch(m_code) {
            FLOCK_RETURN_VALUES
        }
        #undef X
        return "Unknown error";
    }

    auto code() const {
        return m_code;
    }

    private:

    flock_return_t m_code;
};

#define FLOCK_CONVERT_AND_THROW(__err__) do { \
    if((__err__) != FLOCK_SUCCESS) {          \
        throw ::flock::Exception((__err__));  \
    }                                       \
} while(0)

}
#endif
