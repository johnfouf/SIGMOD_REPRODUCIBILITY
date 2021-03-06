#include <msgpack.hpp>
#include <sstream>

#define BOOST_TEST_MODULE MSGPACK_REFERENCE_WRAPPER
#include <boost/test/unit_test.hpp>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#if !defined(MSGPACK_USE_CPP03)

BOOST_AUTO_TEST_CASE(pack_convert)
{
    int i1 = 42;
    std::reference_wrapper<int> val1(i1);
    std::stringstream ss;
    msgpack::pack(ss, val1);
    std::string const& str = ss.str();
    msgpack::object_handle oh = msgpack::unpack(str.data(), str.size());
    int i2 = 0;
    std::reference_wrapper<int> val2(i2);
    oh.get().convert(val2);
    BOOST_CHECK_EQUAL(i1, i2);
}

BOOST_AUTO_TEST_CASE(pack_convert_const)
{
    const int i1 = 42;
    std::reference_wrapper<const int> val1(i1);
    std::stringstream ss;
    msgpack::pack(ss, val1);
    std::string const& str = ss.str();
    msgpack::object_handle oh = msgpack::unpack(str.data(), str.size());
    int i2 = 0;
    std::reference_wrapper<int> val2(i2);
    oh.get().convert(val2);
    BOOST_CHECK_EQUAL(i1, i2);
}

BOOST_AUTO_TEST_CASE(pack_vector)
{
    int i1 = 42;
    std::vector<std::reference_wrapper<int>> val1{i1};
    std::stringstream ss;
    msgpack::pack(ss, val1);
    std::string const& str = ss.str();
    msgpack::object_handle oh = msgpack::unpack(str.data(), str.size());
    std::vector<int> val2 = oh.get().as<std::vector<int>>();
    BOOST_CHECK_EQUAL(val2.size(), static_cast<size_t>(1));
    BOOST_CHECK_EQUAL(val1[0], val2[0]);
}

BOOST_AUTO_TEST_CASE(object)
{
    int i1 = 42;
    std::reference_wrapper<int> val1(i1);
    msgpack::object o(val1);
    int i2 = 0;
    std::reference_wrapper<int> val2(i2);
    o.convert(val2);
    BOOST_CHECK_EQUAL(i1, i2);
}

BOOST_AUTO_TEST_CASE(object_const)
{
    const int i1 = 42;
    std::reference_wrapper<const int> val1(i1);
    msgpack::object o(val1);
    int i2 = 0;
    std::reference_wrapper<int> val2(i2);
    o.convert(val2);
    BOOST_CHECK_EQUAL(i1, i2);
}

BOOST_AUTO_TEST_CASE(object_with_zone)
{
    std::string s1 = "ABC";
    std::reference_wrapper<std::string> val1(s1);
    msgpack::zone z;
    msgpack::object o(val1, z);
    std::string s2 = "DE";
    std::reference_wrapper<std::string> val2(s2);
    o.convert(val2);
    BOOST_CHECK_EQUAL(s1, s2);
}

BOOST_AUTO_TEST_CASE(object_with_zone_const)
{
    const std::string s1 = "ABC";
    std::reference_wrapper<const std::string> val1(s1);
    msgpack::zone z;
    msgpack::object o(val1, z);
    std::string s2 = "DE";
    std::reference_wrapper<std::string> val2(s2);
    o.convert(val2);
    BOOST_CHECK_EQUAL(s1, s2);
}

#endif // !defined(MSGPACK_USE_CPP03)
