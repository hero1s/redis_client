#pragma once

#include <asio.hpp>
#include <array>
#include <functional>
#include <queue>
#include <exception>
#include <tuple>
#include <stack>

class RedisBuffer
{
public:
    RedisBuffer()
        : ptr_(nullptr), size_(0)
    {
    }

    RedisBuffer(const char *ptr, size_t dataSize)
        : ptr_(ptr), size_(dataSize)
    {
    }

    RedisBuffer(const char *s)
        : ptr_(s), size_(s == nullptr ? 0 : strlen(s))
    {
    }

    RedisBuffer(const std::string &s)
        : ptr_(s.c_str()), size_(s.length())
    {
    }

    RedisBuffer(const std::vector<char> &buf)
        : ptr_(buf.empty() ? nullptr : &buf[0]), size_(buf.size())
    {
    }

    size_t size() const
    {
        return size_;
    }

    const char *data() const
    {
        return ptr_;
    }

private:
    const char *ptr_;
    size_t size_;
};

struct RedisValue
{
    static constexpr int REDIS_REPLY_ERR = 0;
    static constexpr int REDIS_REPLY_BULK = 1;
    static constexpr int REDIS_REPLY_SIMPLE = 2;
    static constexpr int REDIS_REPLY_NULL = 3;
    static constexpr int REDIS_REPLY_INT = 4;
    static constexpr int REDIS_REPLY_ARRAY = 5;

    enum class reply_type
    {
        error = REDIS_REPLY_ERR,
        bulk_string = REDIS_REPLY_BULK,
        simple_string = REDIS_REPLY_SIMPLE,
        null = REDIS_REPLY_NULL,
        integer = REDIS_REPLY_INT,
        array = REDIS_REPLY_ARRAY
    };

    enum class string_type
    {
        error = REDIS_REPLY_ERR,
        bulk_string = REDIS_REPLY_BULK,
        simple_string = REDIS_REPLY_SIMPLE
    };

    RedisValue() : type(reply_type::null) {}
    RedisValue(int64_t value) : type(reply_type::integer), integer(value) {}
    RedisValue(std::vector<RedisValue>&& value) : type(reply_type::array), elements(std::move(value)) {}
    RedisValue(std::vector<char>&& value, string_type stype) : type(static_cast<reply_type>(stype)), str(std::move(value)) {}
    RedisValue(RedisValue&& v)
    {
        type = v.type;
        elements = std::move(v.elements);
        str = std::move(v.str);
        integer = v.integer;
        v.type = reply_type::null;
    }

    reply_type type;
    std::vector<RedisValue> elements;
    std::vector<char> str;
    int64_t integer;
};

class RedisParser
{
public:
    RedisParser() : state(State::Start), bulkSize(0) {}

    struct ParseResult
    {
        int64_t index;
        enum
        {
            Completed,
            Incompleted,
            Error
        }result;
    };

    ParseResult parse(const char* ptr, size_t size);

    RedisValue result();

protected:
    ParseResult parseChunk(const char* ptr, size_t size);
    ParseResult parseArray(const char* ptr, size_t size);

    static inline bool isChar(int c)
    {
        return c >= 0 && c <= 127;
    }

    static inline bool isControl(int c)
    {
        return (c >= 0 && c <= 31) || (c == 127);
    }

private:
    enum State
    {
        Start = 0,

        String = 1,
        StringLF = 2,

        ErrorString = 3,
        ErrorLF = 4,

        Integer = 5,
        IntegerLF = 6,

        BulkSize = 7,
        BulkSizeLF = 8,
        Bulk = 9,
        BulkCR = 10,
        BulkLF = 11,

        ArraySize = 12,
        ArraySizeLF = 13,
    } state;

    int64_t bulkSize;
    std::vector<char> buf;
    std::stack<int64_t> arrayStack;
    std::stack<RedisValue> valueStack;

    enum Reply
    {
        string = '+',
        error = '-',
        integer = ':',
        bulk = '$',
        array = '*'
    };
};

class RedisLink : public std::enable_shared_from_this<RedisLink>
{
public:
    RedisLink(asio::io_service& loop);

    ~RedisLink();

    void close();

    void handleAsyncConnect(
        const asio::error_code& ec,
        const std::function<void(bool, const std::string&)>& handler);

    void startRead();

    void sendCommand(const std::vector<RedisBuffer>& cmd, const std::function<void(const RedisValue&)>& handler);

    void commit();

    std::vector<char> buildCommand(const std::vector<RedisBuffer>& cmd);

    void processMessage(const RedisValue& v);

    enum
    {
        NotConnected,
        Connected,
        Subscribed,
        Closed
    }state;

    asio::ip::tcp::socket socket;
    std::array<char, 4096> buf;

    struct QueueItem
    {
        std::vector<char> buff;
        std::function<void(const RedisValue&)> handler;
    };
    std::queue<std::shared_ptr<QueueItem>> queue;
    std::array<std::vector<std::shared_ptr<QueueItem>>, 2> cmdcache;

    std::function<void(const std::string &)> errorHandler;

    RedisParser redisParser;
};

struct Redis
{
    Redis(asio::io_service& service) : service(service) {}

    void connect(const std::string& host, int port, std::function<void(bool result, const std::string& v)> cb);

    using reply_callback_t = std::function<void(const RedisValue&)>;

    Redis& append(const std::string& key, const std::string& value, const reply_callback_t& cb = nullptr);
    Redis& auth(const std::string& password, const reply_callback_t& cb = nullptr);
    Redis& bgrewriteaof(const reply_callback_t& cb = nullptr);
    Redis& bgsave(const reply_callback_t& cb = nullptr);

    Redis& bitcount(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& bitcount(const std::string& key, int start, int end, const reply_callback_t& cb = nullptr);

    //hash
    Redis& hdel(const std::string& key, const std::vector<std::string>& fields, const reply_callback_t& cb = nullptr);
    Redis& hexists(const std::string& key, const std::string& field, const reply_callback_t& cb = nullptr);
    Redis& hget(const std::string& key, const std::string& field, const reply_callback_t& cb = nullptr);
    Redis& hgetall(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& hincrby(const std::string& key, const std::string& field, int incr, const reply_callback_t& cb = nullptr);
    Redis& hincrbyfloat(const std::string& key, const std::string& field, float incr, const reply_callback_t& cb = nullptr);
    Redis& hkeys(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& hlen(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& hmget(const std::string& key, const std::vector<std::string>& fields, const reply_callback_t& cb = nullptr);
    Redis& hmset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& field_vals, const reply_callback_t& cb = nullptr);
    Redis& hset(const std::string& key, const std::string& field, const std::string& value, const reply_callback_t& cb = nullptr);
    Redis& hscan(const std::string& key, const std::string& field, const std::string& value, const reply_callback_t& cb = nullptr);
    Redis& hsetnx(const std::string& key, const std::string& field, const std::string& value, const reply_callback_t& cb = nullptr);
    Redis& hstrlen(const std::string& key, const std::string& field, const reply_callback_t& cb = nullptr);
    Redis& hvals(const std::string& key, const reply_callback_t& cb = nullptr);

    //list
    Redis& blpop(const std::vector<std::string>& keys, int timeout, const reply_callback_t& cb = nullptr);
    Redis& brpop(const std::vector<std::string>& keys, int timeout, const reply_callback_t& cb = nullptr);
    Redis& brpoplpush(const std::string& src, std::string& dst, int timeout, const reply_callback_t& cb = nullptr);
    Redis& lindex(const std::string& key, int index, const reply_callback_t& cb = nullptr);
    Redis& linsert(const std::string& key, const std::string& before_after, const std::string& pivot, const std::string& value, const reply_callback_t& cb = nullptr);
    Redis& llen(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& lpop(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& lpush(const std::string& key, const std::vector<std::string>& values, const reply_callback_t& cb = nullptr);
    Redis& lpushx(const std::string& key, const std::string& value, const reply_callback_t& cb = nullptr);
    Redis& lrange(const std::string& key, int start, int stop, const reply_callback_t& cb = nullptr);
    Redis& lrem(const std::string& key, int count, const std::string& value, const reply_callback_t& cb = nullptr);
    Redis& lset(const std::string& key, int index, const std::string& value, const reply_callback_t& cb = nullptr);
    Redis& ltrim(const std::string& key, int start, int stop, const reply_callback_t& cb = nullptr);
    Redis& rpop(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& rpoplpush(const std::string& src, std::string& dst, const reply_callback_t& cb = nullptr);
    Redis& rpush(const std::string& key, const std::vector<std::string>& values, const reply_callback_t& cb = nullptr);
    Redis& rpushx(const std::string& key, const std::string& value, const reply_callback_t& cb = nullptr);

    //set
    Redis& sadd(const std::string& key, const std::vector<std::string>& members, const reply_callback_t& cb = nullptr);
    Redis& scard(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& sdiff(const std::vector<std::string>& keys, const reply_callback_t& cb = nullptr);
    Redis& sdiffstore(const std::string& dst, const std::vector<std::string>& keys, const reply_callback_t& cb = nullptr);
    Redis& sinter(const std::vector<std::string>& keys, const reply_callback_t& cb = nullptr);
    Redis& sinterstore(const std::string& dst, const std::vector<std::string>& keys, const reply_callback_t& cb = nullptr);
    Redis& sismember(const std::string& key, const std::string& member, const reply_callback_t& cb = nullptr);
    Redis& smembers(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& smove(const std::string& src, const std::string& dst, const std::string& member, const reply_callback_t& cb = nullptr);
    Redis& spop(const std::string& key, int count, const reply_callback_t& cb = nullptr);
    Redis& srandmember(const std::string& key, int count, const reply_callback_t& cb = nullptr);
    Redis& srem(const std::string& key, const std::vector<std::string>& members, const reply_callback_t& cb = nullptr);
    //Redis& sscan;
    Redis& sunion(const std::vector<std::string>& keys, const reply_callback_t& cb = nullptr);
    Redis& sunionstore(const std::string& dst, const std::vector<std::string>& keys, const reply_callback_t& cb = nullptr);

    //sorted sets
    Redis& zadd(const std::string& key, const std::string& score, const std::string& member, const reply_callback_t& cb = nullptr);
    Redis& zadd(const std::string& key, const std::vector<std::pair<std::string, std::string>>& score_members, const reply_callback_t& cb = nullptr);
    Redis& zcard(const std::string& key, const reply_callback_t& cb = nullptr);
    Redis& zcount(const std::string& key, int min, int max, const reply_callback_t& cb = nullptr);
    Redis& zincrby(const std::string& key, int inc, const std::string& member, const reply_callback_t& cb = nullptr);
    //Redis& zinterstore(const reply_callback_t& cb = nullptr);
    //Redis& zlexcount(const reply_callback_t& cb = nullptr);
    Redis& zrange(const std::string& key, const std::string& start, const std::string& stop, bool withscores, const reply_callback_t& cb = nullptr);
    //Redis& zrangebylex(const std::string& key, const std::string& min, const std::string& max, const std::string& limit,  const reply_callback_t& cb = nullptr);
    //Redis& zrangebyscore(const reply_callback_t& cb = nullptr);
    Redis& zrank(const std::string& key, const std::string& member, const reply_callback_t& cb = nullptr);
    Redis& zrem(const std::string& key, const std::vector<std::string>& members, const reply_callback_t& cb = nullptr);
    Redis& zremrangebylex(const std::string& key, const std::string& min, const std::string& max, const reply_callback_t& cb = nullptr);
    Redis& zremrangebyrank(const std::string& key, int start, int stop, const reply_callback_t& cb = nullptr);
    Redis& zremrangebyscore(const std::string& key, const std::string& min, const std::string& max, const reply_callback_t& cb = nullptr);
    Redis& zrevrange(const std::string& key, const std::string& start, const std::string& stop, bool withscores, const reply_callback_t& cb = nullptr);
    Redis& zrevrangebylex(const std::string& key, const std::string& min, const std::string& max, const reply_callback_t& cb = nullptr);
    //Redis& zrevrangebyscore(const reply_callback_t& cb = nullptr);
    Redis& zrevrank(const std::string& key, const std::string& member, const reply_callback_t& cb = nullptr);
    //Redis& zscan(const reply_callback_t& cb = nullptr);
    Redis& zscore(const std::string& key, const std::string& member, const reply_callback_t& cb = nullptr);
    //Redis& zunionstore(const reply_callback_t& cb = nullptr);

    asio::io_service& service;
    std::shared_ptr<RedisLink> link;
};