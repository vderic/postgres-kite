#include "exx_int.hpp"
#include <atomic>
#include <cstdarg>
#include <cstring>

namespace exx {

std::string concat(std::string a, std::string_view b, std::string_view c,
                   std::string_view d, std::string_view e, std::string_view f) {
  a += b;
  a += c;
  a += d;
  a += e;
  a += f;
  return a;
}

}; // namespace exx
