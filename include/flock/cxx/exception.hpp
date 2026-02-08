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

/**
 * @brief Exception class for the C++ Flock API.
 *
 * Wraps a flock_return_t error code (defined in flock-common.h) into
 * a standard C++ exception. Thrown by the C++ wrappers whenever the
 * underlying C function returns a non-success error code.
 */
class Exception : public std::exception {

    public:

    /**
     * @brief Construct an Exception from a Flock error code.
     *
     * @param code The flock_return_t error code.
     */
    Exception(flock_return_t code)
    : m_code(code) {}

    /**
     * @brief Return a human-readable description of the error.
     *
     * @return A null-terminated string describing the error.
     */
    const char* what() const noexcept override {
        #define X(__err__, __msg__) case __err__: return __msg__;
        switch(m_code) {
            FLOCK_RETURN_VALUES
        }
        #undef X
        return "Unknown error";
    }

    /**
     * @brief Return the underlying flock_return_t error code.
     *
     * @return The flock_return_t error code.
     */
    auto code() const {
        return m_code;
    }

    private:

    flock_return_t m_code;
};

/**
 * @brief Helper macro that throws a flock::Exception if the
 * given flock_return_t value is not FLOCK_SUCCESS.
 *
 * @param __err__ A flock_return_t value to check.
 */
#define FLOCK_CONVERT_AND_THROW(__err__) do { \
    if((__err__) != FLOCK_SUCCESS) {          \
        throw ::flock::Exception((__err__));  \
    }                                       \
} while(0)

}
#endif
