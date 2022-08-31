#include "redis.h"

static std::vector<char>& operator<<(std::vector<char> &vec, const RedisBuffer &buf)
{
    vec.insert(vec.end(), buf.data(), buf.data() + buf.size());
    return vec;
}

static std::vector<char>& operator<<(std::vector<char> &vec, const std::string &s)
{
    vec.insert(vec.end(), s.begin(), s.end());
    return vec;
}

static std::vector<char>& operator<<(std::vector<char> &vec, const char *s)
{
    vec.insert(vec.end(), s, s + strlen(s));
    return vec;
}

static std::vector<char>& operator<<(std::vector<char> &vec, char c)
{
    vec.push_back(c);
    return vec;
}

template<size_t size>
static std::vector<char>& operator<<(std::vector<char> &vec, const char(&s)[size])
{
    vec.insert(vec.end(), s, s + size);
    return vec;
}

RedisLink::RedisLink(asio::io_service & loop)
    : state(NotConnected)
    , socket(loop)
{

}

RedisLink::~RedisLink()
{
    close();
}

void RedisLink::close()
{
    if (state != RedisLink::Closed)
    {
        asio::error_code iec;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, iec);
        state = RedisLink::Closed;
    }
}

void RedisLink::handleAsyncConnect(const asio::error_code & ec, const std::function<void(bool, const std::string&)>& handler)
{
    if (!ec)
    {
        socket.non_blocking(true);
        socket.set_option(asio::ip::tcp::no_delay(true));
        state = RedisLink::Connected;
        handler(true, {});
        startRead();
    }
    else
    {
        handler(false, ec.message());
    }
}

void RedisLink::startRead()
{
    socket.async_read_some(asio::buffer(buf), [this, self = shared_from_this()](asio::error_code ec, size_t bytes_transferred)
    {
        if (ec || bytes_transferred == 0)
        {
            errorHandler(ec.message());
            return;
        }

        for (size_t pos = 0; pos < bytes_transferred;)
        {
            auto result = redisParser.parse(buf.data() + pos, bytes_transferred - pos);

            if (result.result == RedisParser::ParseResult::Completed)
            {
                processMessage(redisParser.result());
            }
            else if (result.result == RedisParser::ParseResult::Incompleted)
                break;
            else
            {
                errorHandler("[RedisClient] Parser error");
                return;
            }

            pos += result.index;
        }

        startRead();
    });
}

void RedisLink::sendCommand(const std::vector<RedisBuffer>& cmd, const std::function<void(const RedisValue&)>& handler)
{
    auto item = std::make_shared<QueueItem>();
    item->buff = buildCommand(cmd);
    item->handler = handler;
    queue.push(item);

    cmdcache[0].push_back(item);
}

void RedisLink::commit()
{
    if (!cmdcache[0].empty() && cmdcache[1].empty())
    {
        std::swap(cmdcache[0], cmdcache[1]);
        assert(cmdcache[0].empty());
        assert(!cmdcache[1].empty());

        std::vector<asio::const_buffer> buffers;
        size_t bytes_to_transferred = 0;
        for (auto & block : cmdcache[1])
        {
            buffers.emplace_back(asio::const_buffer(block->buff.data(), block->buff.size()));
            bytes_to_transferred += block->buff.size();
        }

        asio::async_write(socket, buffers,
            [this, self = shared_from_this(), bytes_to_transferred](asio::error_code ec, size_t bytes_transferred)
        {
            if (ec)
            {
                errorHandler(ec.message());
                return;
            }

            assert(bytes_transferred == bytes_to_transferred);
            cmdcache[1].clear();
            commit();
        });
    }
}

std::vector<char> RedisLink::buildCommand(const std::vector<RedisBuffer>& cmd)
{
    std::vector<char> cmdbuff; cmdbuff.reserve(4096);
    cmdbuff << '*' << std::to_string(cmd.size()) << "\r\n";
    for (const auto& c : cmd)
        cmdbuff << '$' << std::to_string(c.size()) << "\r\n" << c << "\r\n";
    return cmdbuff;
}

void RedisLink::processMessage(const RedisValue & v)
{
    if (state == RedisLink::Subscribed)
    {
        if (v.elements.size() == 3)
        {
            const RedisValue& command = v.elements.at(0);
            const RedisValue& queueName = v.elements.at(1);
            const RedisValue& value = v.elements.at(2);

            static const std::string message = "message";
            static const std::string subscribe = "subscribe";
            static const std::string unsubscribe = "unsubscribe";

            if (message == command.str.data())
            {

            }
            else if (subscribe == command.str.data())
            {

            }
            else if (unsubscribe == command.str.data())
            {

            }
            else
            {
                errorHandler("[RedisClient] invalid command: " + std::string(command.str.data(), command.str.size()));
                return;
            }
        }
        else
        {
            errorHandler("[RedisClient] protocol error");
            return;
        }
    }
    else
    {
        if (!queue.empty())
        {
            if (queue.front()->handler)
                queue.front()->handler(v);
            queue.pop();
        }
        else
        {
            errorHandler("[RedisClient] unexpected message: ");
        }
    }
}

//////////////////////////////////////

RedisParser::ParseResult RedisParser::parse(const char * ptr, size_t size)
{
    if (!arrayStack.empty())
        return parseArray(ptr, size);
    else
        return parseChunk(ptr, size);
}

RedisValue RedisParser::result()
{
    assert(valueStack.empty() == false);

    if (valueStack.empty() == false)
    {
        RedisValue value = std::move(valueStack.top());
        valueStack.pop();
        return value;
    }

    return RedisValue();
}

RedisParser::ParseResult RedisParser::parseChunk(const char * ptr, size_t size)
{
    int64_t i = 0;

    for (; i < (int64_t)size; ++i)
    {
        char c = ptr[i];
        switch (state)
        {
        case State::Start:
            buf.clear();
            switch (c)
            {
            case Reply::string:
                state = State::String;
                break;
            case Reply::error:
                state = State::ErrorString;
                break;
            case Reply::integer:
                state = State::Integer;
                break;
            case Reply::bulk:
                bulkSize = 0;
                state = State::BulkSize;
                break;
            case Reply::array:
                state = State::ArraySize;
                break;
            default:
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::String:
        case State::ErrorString:
            if (c == '\r')
                state = (state == State::String) ? State::StringLF : State::ErrorLF;
            else if (isChar(c) && !isControl(c))
                buf.push_back(c);
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::StringLF:
        case State::ErrorLF:
            if (c == '\n')
            {
                state = State::Start;
                valueStack.emplace(RedisValue(std::move(buf), (state == State::StringLF ? RedisValue::string_type::simple_string : RedisValue::string_type::error)));

                return{ i + 1, ParseResult::Completed };
            }
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };

            }
            break;
        case State::Integer:
            if (c == '\r')
            {
                if (buf.empty())
                {
                    state = State::Start;
                    return{ i + 1, ParseResult::Error };
                }
                else
                {
                    state = State::IntegerLF;
                }
            }
            else if (isdigit(c) || c == '-')
                buf.push_back(c);
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::IntegerLF:
            if (c == '\n')
            {
                auto value = std::stoll(std::string(buf.begin(), buf.end()));
                buf.clear();

                valueStack.push(value);
                state = State::Start;
                return{ i + 1, ParseResult::Completed };
            }
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::BulkSize:
            if (c == '\r')
            {
                if (buf.empty())
                {
                    state = State::Start;
                    return{ i + 1, ParseResult::Error };
                }
                else
                {
                    state = State::BulkSizeLF;
                }
            }
            else if (isdigit(c) || c == '-')
                buf.push_back(c);
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::BulkSizeLF:
            if (c == '\n')
            {
                bulkSize = std::stoll(std::string(buf.begin(), buf.end()));
                buf.clear();

                if (bulkSize == -1) //请求对象不存在
                {
                    valueStack.push(RedisValue());
                    state = State::Start;
                    return{ i + 1, ParseResult::Completed };
                }
                else if (bulkSize == 0)
                {
                    state = State::BulkCR;
                }
                else if (bulkSize < 0)
                {
                    state = State::Start;
                    return{ i + 1, ParseResult::Error };
                }
                else
                {
                    buf.reserve(bulkSize);

                    int64_t available = size - i - 1;
                    int64_t canRead = std::min<int64_t>(bulkSize, available);
                    if (canRead > 0)
                        buf.assign(ptr + i + 1, ptr + i + canRead + 1);

                    i += canRead;

                    if (bulkSize > available)
                    {
                        bulkSize -= canRead;
                        state = State::Bulk;
                        return{ i + 1, ParseResult::Incompleted };
                    }
                    else
                    {
                        state = State::BulkCR;
                    }
                }
            }
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::Bulk: {
            assert(bulkSize > 0);

            int64_t available = size - i;
            int64_t canRead = std::min<int64_t>(available, bulkSize);

            buf.insert(buf.end(), ptr + i, ptr + canRead);
            bulkSize -= canRead;
            i += canRead - 1;

            if (bulkSize > 0)
                return{ i + 1, ParseResult::Incompleted };
            else
            {
                state = State::BulkCR;

                if ((int64_t)size == i + 1)
                    return{ i + 1, ParseResult::Incompleted };
            }
            break;
        }
        case State::BulkCR:
            if (c == '\r')
                state = State::BulkLF;
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::BulkLF:
            if (c == '\n')
            {
                state = State::Start;
                valueStack.emplace(RedisValue(std::move(buf), RedisValue::string_type::bulk_string));
                return{ i + 1, ParseResult::Completed };
            }
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::ArraySize:
            if (c == '\r')
            {
                if (buf.empty())
                {
                    state = State::Start;
                    return{ i + 1, ParseResult::Error };
                }
                else
                {
                    state = State::ArraySizeLF;
                }
            }
            else if (isdigit(c) || c == '-')
            {
                buf.push_back(c);
            }
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
            break;
        case State::ArraySizeLF:
            if (c == '\n')
            {
                int64_t arraySize = std::stoll(std::string(buf.begin(), buf.end()));
                buf.clear();

                if (arraySize == -1 || arraySize == 0)
                {
                    state = State::Start;
                    valueStack.emplace(std::vector<RedisValue>());
                    return{ i + 1, ParseResult::Completed };
                }
                else if (arraySize < 0)
                {
                    state = State::Start;
                    return{ i + 1, ParseResult::Error };
                }
                else
                {
                    std::vector<RedisValue> array;
                    array.reserve(arraySize);
                    arrayStack.push(arraySize);
                    valueStack.emplace(std::move(array));

                    state = State::Start;
                    if ((i + 1) != int64_t(size))
                    {
                        auto result = parseArray(ptr + i + 1, size - i - 1);
                        result.index += i + 1;
                        return result;
                    }
                    else
                    {
                        return{ i + 1, ParseResult::Incompleted };
                    }
                }
            }
            else
            {
                state = State::Start;
                return{ i + 1, ParseResult::Error };
            }
        default:
            state = State::Start;
            return{ i + 1, ParseResult::Error };
        }
    }

    return{ i, ParseResult::Incompleted };
}

RedisParser::ParseResult RedisParser::parseArray(const char * ptr, size_t size)
{
    assert(!arrayStack.empty());
    assert(!valueStack.empty());

    auto arraySize = arrayStack.top();
    auto arrayValue = std::move(valueStack.top());

    arrayStack.pop();
    valueStack.pop();

    int64_t i = 0;

    if (!arrayStack.empty())
    {
        auto result = parseArray(ptr, size);
        if (result.result != ParseResult::Completed)
        {
            valueStack.emplace(std::move(arrayValue));
            arrayStack.emplace(arraySize);
            return result;
        }
        else
        {
            arrayValue.elements.emplace_back(std::move(valueStack.top()));
            valueStack.pop();
            --arraySize;
        }

        i += result.index;
    }

    if (i == int64_t(size))
    {
        valueStack.emplace(std::move(arrayValue));
        if (arraySize == 0)
            return{ i, ParseResult::Completed };
        else
        {
            arrayStack.push(arraySize);
            return{ i, ParseResult::Incompleted };
        }
    }

    long int x = 0;

    for (; x < arraySize; ++x)
    {
        auto result = parse(ptr + i, size - i);
        i += result.index;

        if (result.result == ParseResult::Error)
            return{ i, ParseResult::Error };
        else if (result.result == ParseResult::Incompleted)
        {
            arraySize -= x;
            valueStack.emplace(std::move(arrayValue));
            arrayStack.emplace(arraySize);
            return{ i, ParseResult::Incompleted };
        }
        else
        {
            assert(valueStack.empty() == false);
            arrayValue.elements.push_back(std::move(valueStack.top()));
            valueStack.pop();
        }
    }

    assert(x == arraySize);
    valueStack.emplace(std::move(arrayValue));
    return{ i, ParseResult::Completed };
}

//////////////////////////////////////////

void Redis::connect(const std::string & host, int port, std::function<void(bool result, const std::string&v)> cb)
{
    link = std::make_shared<RedisLink>(service);
    link->socket.async_connect(asio::ip::tcp::endpoint(asio::ip::address::from_string(host), port),
        std::bind(&RedisLink::handleAsyncConnect, link, std::placeholders::_1, cb));
}

Redis & Redis::append(const std::string & key, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "APPEND", key, value }, cb);
    return *this;
}

Redis & Redis::auth(const std::string & password, const reply_callback_t & cb)
{
    link->sendCommand({ "AUTH", password }, cb);
    return *this;
}

Redis & Redis::bgrewriteaof(const reply_callback_t & cb)
{
    link->sendCommand({ "BGREWRITEAOF" }, cb);
    return *this;
}

Redis & Redis::bgsave(const reply_callback_t & cb)
{
    link->sendCommand({ "BGSAVE" }, cb);
    return *this;
}

Redis & Redis::bitcount(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "BITCOUNT", key }, cb);
    return *this;
}

Redis & Redis::bitcount(const std::string & key, int start, int end, const reply_callback_t & cb)
{
    link->sendCommand({ "BITCOUNT", key, std::to_string(start), std::to_string(end) }, cb);
    return *this;
}

Redis & Redis::hdel(const std::string & key, const std::vector<std::string>& fields, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "HDEL", key };
    cmd.insert(cmd.end(), fields.begin(), fields.end());
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::hexists(const std::string & key, const std::string & field, const reply_callback_t & cb)
{
    link->sendCommand({ "HEXISTS", key, field }, cb);
    return *this;
}

Redis & Redis::hget(const std::string & key, const std::string & field, const reply_callback_t & cb)
{
    link->sendCommand({ "HGET", key, field }, cb);
    return *this;
}

Redis & Redis::hgetall(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "HGETALL", key }, cb);
    return *this;
}

Redis & Redis::hincrby(const std::string & key, const std::string & field, int incr, const reply_callback_t & cb)
{
    link->sendCommand({ "HINCRBY", key, field, std::to_string(incr) }, cb);
    return *this;
}

Redis & Redis::hincrbyfloat(const std::string & key, const std::string & field, float incr, const reply_callback_t & cb)
{
    link->sendCommand({ "HINCRBYFLOAT", key, field, std::to_string(incr) }, cb);
    return *this;
}

Redis & Redis::hkeys(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "HKEYS", key }, cb);
    return *this;
}

Redis & Redis::hlen(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "HLEN", key }, cb);
    return *this;
}

Redis & Redis::hmget(const std::string & key, const std::vector<std::string>& fields, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "HMGET", key };
    cmd.insert(cmd.end(), fields.begin(), fields.end());
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::hmset(const std::string & key, const std::vector<std::pair<std::string, std::string>>& field_vals, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "HMSET", key };
    for (const auto& obj : field_vals)
    {
        cmd.push_back(obj.first);
        cmd.push_back(obj.second);
    }
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::hset(const std::string & key, const std::string & field, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "HSET", key, field, value }, cb);
    return *this;
}

Redis & Redis::hscan(const std::string & key, const std::string & field, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "HSCAN", key, field, value }, cb);
    return *this;
}

Redis & Redis::hsetnx(const std::string & key, const std::string & field, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "HSETNX", key, field, value }, cb);
    return *this;
}

Redis & Redis::hstrlen(const std::string & key, const std::string & field, const reply_callback_t & cb)
{
    link->sendCommand({ "HSTRLEN", key, field }, cb);
    return *this;
}

Redis & Redis::hvals(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "HVALS", key }, cb);
    return *this;
}

////////////////////////list///////////////////////

Redis & Redis::blpop(const std::vector<std::string> & keys, int timeout, const reply_callback_t & cb)
{
    auto timeouts = std::to_string(timeout);
    std::vector<RedisBuffer> cmd = { "BLOP" };
    for (const auto& key : keys)
        cmd.push_back(key);
    cmd.push_back(timeouts);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::brpop(const std::vector<std::string>& keys, int timeout, const reply_callback_t & cb)
{
    auto timeouts = std::to_string(timeout);
    std::vector<RedisBuffer> cmd = { "BRPOP" };
    for (const auto& key : keys)
        cmd.push_back(key);
    cmd.push_back(timeouts);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::brpoplpush(const std::string & src, std::string & dst, int timeout, const reply_callback_t & cb)
{
    link->sendCommand({ "BRPOPLPUSH", src, dst, std::to_string(timeout) }, cb);
    return *this;
}

Redis & Redis::lindex(const std::string & key, int index, const reply_callback_t & cb)
{
    link->sendCommand({ "LINDEX", key, std::to_string(index) }, cb);
    return *this;
}

Redis & Redis::linsert(const std::string & key, const std::string & before_after, const std::string & pivot, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "LINSERT", key, before_after, pivot, value }, cb);
    return *this;
}

Redis & Redis::llen(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "LLEN", key }, cb);
    return *this;
}

Redis & Redis::lpop(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "LPOP", key }, cb);
    return *this;
}

Redis & Redis::lpush(const std::string & key, const std::vector<std::string>& values, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "LPUSH", key };
    for (const auto& v : values)
        cmd.push_back(v);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::lpushx(const std::string & key, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "LPUSHX", key, value }, cb);
    return *this;
}

Redis & Redis::lrange(const std::string & key, int start, int stop, const reply_callback_t & cb)
{
    link->sendCommand({ "LRANGE", key, std::to_string(start), std::to_string(stop) }, cb);
    return *this;
}

Redis & Redis::lrem(const std::string & key, int count, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "LREM", key, std::to_string(count), value }, cb);
    return *this;
}

Redis & Redis::lset(const std::string & key, int index, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "LSET", key, std::to_string(index), value }, cb);
    return *this;
}

Redis & Redis::ltrim(const std::string & key, int start, int stop, const reply_callback_t & cb)
{
    link->sendCommand({ "LTRIM", key, std::to_string(start), std::to_string(stop) }, cb);
    return *this;
}

Redis & Redis::rpop(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "RPOP", key }, cb);
    return *this;
}

Redis & Redis::rpoplpush(const std::string & src, std::string & dst, const reply_callback_t & cb)
{
    link->sendCommand({ "RPOPLPUSH", src, dst }, cb);
    return *this;
}

Redis & Redis::rpush(const std::string & key, const std::vector<std::string>& values, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "RPUSH", key };
    for (const auto& v : values)
        cmd.push_back(v);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::rpushx(const std::string & key, const std::string & value, const reply_callback_t & cb)
{
    link->sendCommand({ "RPUSHX", key, value }, cb);
    return *this;
}

////////////////////set////////////////////
Redis & Redis::sadd(const std::string & key, const std::vector<std::string>& members, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "SADD", key };
    for (const auto& m : members)
        cmd.push_back(m);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::scard(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "SCARD", key }, cb);
    return *this;
}

Redis & Redis::sdiff(const std::vector<std::string>& keys, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "SDIFF" };
    for (const auto& k : keys)
        cmd.push_back(k);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::sdiffstore(const std::string & dst, const std::vector<std::string>& keys, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "SDIFFSTORE", dst };
    for (const auto& k : keys)
        cmd.push_back(k);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::sinter(const std::vector<std::string>& keys, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "SINTER" };
    for (const auto& k : keys)
        cmd.push_back(k);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::sinterstore(const std::string & dst, const std::vector<std::string>& keys, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "SINTERSTORE", dst };
    for (const auto& k : keys)
        cmd.push_back(k);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::sismember(const std::string & key, const std::string & member, const reply_callback_t & cb)
{
    link->sendCommand({ "SISMEMBER", key, member }, cb);
    return *this;
}

Redis & Redis::smembers(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "SMEMBERS", key }, cb);
    return *this;
}

Redis & Redis::smove(const std::string & src, const std::string & dst, const std::string & member, const reply_callback_t & cb)
{
    link->sendCommand({ "SMOVE", src, dst, member }, cb);
    return *this;
}

Redis & Redis::spop(const std::string & key, int count, const reply_callback_t & cb)
{
    link->sendCommand({ "SPOP", key, std::to_string(count) }, cb);
    return *this;
}

Redis & Redis::srandmember(const std::string & key, int count, const reply_callback_t & cb)
{
    link->sendCommand({ "SRANDMEMBER", key, std::to_string(count) }, cb);
    return *this;
}

Redis & Redis::srem(const std::string & key, const std::vector<std::string>& members, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "SREM", key };
    for (const auto& m : members)
        cmd.push_back(m);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::sunion(const std::vector<std::string>& keys, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "SUNION" };
    for (const auto& k : keys)
        cmd.push_back(k);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::sunionstore(const std::string & dst, const std::vector<std::string>& keys, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "SUNIONSTORE", dst };
    for (const auto& k : keys)
        cmd.push_back(k);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::zadd(const std::string & key, const std::string & score, const std::string & member, const reply_callback_t & cb)
{
    link->sendCommand({ "ZADD", key, score, member }, cb);
    return *this;
}

Redis & Redis::zadd(const std::string & key, const std::vector<std::pair<std::string, std::string>>& score_members, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "ZADD", key };
    for (const auto& m : score_members)
    {
        cmd.push_back(m.first);
        cmd.push_back(m.second);
    }
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::zcard(const std::string & key, const reply_callback_t & cb)
{
    link->sendCommand({ "ZCARD", key }, cb);
    return *this;
}

Redis & Redis::zcount(const std::string & key, int min, int max, const reply_callback_t & cb)
{
    link->sendCommand({ "ZCOUNT", key, std::to_string(min), std::to_string(max) }, cb);
    return *this;
}

Redis & Redis::zincrby(const std::string & key, int inc, const std::string & member, const reply_callback_t & cb)
{
    link->sendCommand({ "ZINCRBY", key, std::to_string(inc), member }, cb);
    return *this;
}

Redis & Redis::zrange(const std::string & key, const std::string & start, const std::string & stop, bool withscores, const reply_callback_t & cb)
{
    if (withscores)
        link->sendCommand({ "ZRANGE", key, start, stop, "WITHSCORES" }, cb);
    else
        link->sendCommand({ "ZRANGE", key, start, stop }, cb);
    return *this;
}

Redis & Redis::zrank(const std::string & key, const std::string & member, const reply_callback_t & cb)
{
    link->sendCommand({ "ZRANK", key, member }, cb);
    return *this;
}

Redis & Redis::zrem(const std::string & key, const std::vector<std::string>& members, const reply_callback_t & cb)
{
    std::vector<RedisBuffer> cmd = { "ZREM", key };
    for (const auto& m : members)
        cmd.emplace_back(m);
    link->sendCommand(cmd, cb);
    return *this;
}

Redis & Redis::zremrangebylex(const std::string & key, const std::string & min, const std::string & max, const reply_callback_t & cb)
{
    link->sendCommand({ "ZREMRANGEBYLEX", key, min, max }, cb);
    return *this;
}

Redis & Redis::zremrangebyrank(const std::string & key, int start, int stop, const reply_callback_t & cb)
{
    link->sendCommand({ "ZREMRANGEBYRANK", key, std::to_string(start), std::to_string(stop) }, cb);
    return *this;
}

Redis & Redis::zremrangebyscore(const std::string & key, const std::string & min, const std::string & max, const reply_callback_t & cb)
{
    link->sendCommand({ "ZREMRANGEBYSCORE", key, min, max }, cb);
    return *this;
}

Redis & Redis::zrevrange(const std::string & key, const std::string & start, const std::string & stop, bool withscores, const reply_callback_t & cb)
{
    if (withscores)
        link->sendCommand({ "ZREVRANGE", key, start, stop, "WITHSCORES" }, cb);
    else
        link->sendCommand({ "ZREVRANGE", key, start, stop }, cb);
    return *this;
}

Redis & Redis::zrevrangebylex(const std::string & key, const std::string & min, const std::string & max, const reply_callback_t & cb)
{
    link->sendCommand({ "ZREVRANGEBYLEX", key, min, max }, cb);
    return *this;
}

Redis & Redis::zrevrank(const std::string & key, const std::string & member, const reply_callback_t & cb)
{
    link->sendCommand({ "ZREVRANK", key, member }, cb);
    return *this;
}

Redis & Redis::zscore(const std::string & key, const std::string & member, const reply_callback_t & cb)
{
    link->sendCommand({ "ZSCORE", key, member }, cb);
    return *this;
}

