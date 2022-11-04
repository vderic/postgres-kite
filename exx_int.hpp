#pragma once

#include <cassert>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

class bad_sql : public std::runtime_error {
public:
  bad_sql(const std::string &msg);
  virtual ~bad_sql() noexcept {};
};

class exx_error : public std::runtime_error {
public:
  exx_error(const char *file, int line, const std::string &msg);
  virtual ~exx_error() noexcept {};
  const char *where() const { return m_fline.c_str(); }

private:
  std::string m_fline;
};

namespace exx {

// return a string {prefix}{uniquenumber}
std::string unique_string(std::string_view prefix);

std::string concat(std::string a, std::string_view b, std::string_view c = "",
                   std::string_view d = "", std::string_view e = "",
                   std::string_view f = "");

int trace(const char *fmt, ...);

template <typename... Args>
std::string format(const std::string &format, Args... args) {
  int size_s = std::snprintf(nullptr, 0, format.c_str(), args...) +
               1; // Extra space for '\0'
  if (size_s <= 0) {
    throw std::runtime_error("Error during formatting.");
  }
  auto size = static_cast<size_t>(size_s);
  std::unique_ptr<char[]> buf(new char[size]);
  std::snprintf(buf.get(), size, format.c_str(), args...);
  return std::string{buf.get(),
                     buf.get() + size - 1}; // We don't want the '\0' inside
}

inline int throw_exx_error(const char *fname, int lineno,
                           const std::string &msg) {
  throw exx_error(fname, lineno, msg);
}

inline int throw_runtime_error(const std::string &msg) {
  throw std::runtime_error(msg);
}

}; // namespace exx

// clang-format off
#define CHECK(cond, msg) \
  ((cond) ? 0 : exx::throw_exx_error(__FILE__, __LINE__, msg))

#define CHECKX(cond, msg) \
  ((cond) ? 0 : exx::throw_runtime_error(msg))


#ifdef NDEBUG
#define _TR(level, x) (void)0
#else
#define _TR(level, x) \
  ((level) <= exx_trace ? exx::trace x : 0)
#endif

/* note: to use trace, call with double (()). 
 * e.g. TR1(("a string %s and an int %d", msg, id))
 */
#define TR1(x)  _TR(1, x)
#define TR2(x)  _TR(2, x)
#define TR3(x)  _TR(3, x)

// clang-format on
