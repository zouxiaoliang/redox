/*
* Redox - A modern, asynchronous, and wicked fast C++11 client for Redis
*
*    https://github.com/hmartiro/redox
*
* Copyright 2015 - Hayk Martirosyan <hayk.mart at gmail dot com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include <vector>
#include <set>
#include <unordered_set>
#include <string.h>

#include "command.hpp"

#include "utils/helper.hpp"

using namespace std;

namespace redox {

template<class ReplyT>
Command<ReplyT>::Command(void *handle, int64_t id, const std::vector<string> &cmd,
                         const std::function<void (Command<ReplyT> &)> &callback,
                         const std::function<void (Command<ReplyT> &, int)> &callback_error,
                         const std::function<void (int)> &notify_free,
                         double repeat, double after, bool free_memory, log::Logger &logger):
    handle_(handle),
    id_(id),
    cmd_(cmd),
    repeat_(repeat),
    after_(after),
    free_memory_(free_memory),
    callback_(callback),
    callback_error_(callback_error),
    notify_free_(notify_free),
    last_error_(),
    logger_(logger) {
    timer_guard_.lock();
}

template <class ReplyT> void Command<ReplyT>::wait() {
    unique_lock<mutex> lk(waiter_lock_);
    waiter_.wait(lk, [this]() { return waiting_done_.load(); });
    waiting_done_ = {false};
}

template<class ReplyT> void Command<ReplyT>::notifyAll() {
    {
        unique_lock<mutex> lk(waiter_lock_);
        waiting_done_ = true;
    }
    waiter_.notify_all();
}

template <class ReplyT> bool Command<ReplyT>::moved() {
    if (!ok()) {
        if (0 == ::strncmp(lastError().c_str(), "MOVED", strlen("MOVED"))) {
            return true;
        }
    }
    return false;
}

template <class ReplyT> bool Command<ReplyT>::moved(std::pair<string, int32_t> &where) {
    if (!ok()) {
        const char * last_error =  lastError().c_str();
        if (0 == ::strncmp(lastError().c_str(), "MOVED", strlen("MOVED"))) {
            vector<string> items;
            util::split(last_error, items, " ");

            const string &hostinfo = *items.rbegin();

            vector<string> ip_port;
            util::split(hostinfo, ip_port, ":");

            if (ip_port.size() < 2)
                return false;

            where.first = ip_port[0];
            where.second = ::atoi(ip_port[1].c_str());
            return true;
        }
    }
    return false;
}

template <class ReplyT> void Command<ReplyT>::processReply(redisReply *r, std::function<void(const string, int32_t)> move_to_other) {

    last_error_.clear();
    reply_obj_ = r;

    if (reply_obj_ == nullptr) {
        reply_status_ = ERROR_REPLY;
        last_error_ = "Received null redisReply* from hiredis.";
        logger_.error() << last_error_;

        if (callback_error_)
            callback_error_(*this, REDIS_ERR);

    } else {
        lock_guard<mutex> lg(reply_guard_);
        parseReplyObject();
    }

    std::pair<string, int32_t> where;
    if (moved(where) && move_to_other) {
        move_to_other(where.first, where.second);
    } else {
        invoke();
    }

    pending_--;

    notifyAll();

    // Always free the reply object for repeating commands
    if (repeat_ > 0) {
        freeReply();

    } else {

        // User calls .free()
        if (!free_memory_)
            return;

        // Free non-repeating commands automatically
        // once we receive expected replies
        if (pending_ == 0)
            free();
    }
}

template <class ReplyT> void Command<ReplyT>::free() {

    if (notify_free_)
        notify_free_(id_);
}

template <class ReplyT> void Command<ReplyT>::freeReply() {

    waiter_.notify_all();

    if (reply_obj_ == nullptr)
        return;

    freeReplyObject(reply_obj_);
    reply_obj_ = nullptr;
}

template <class ReplyT> Command<ReplyT> *Command<ReplyT>::createCommand(
        void *handle,
        int64_t id,
        const std::vector<std::string> &cmd,
        const std::function<void(Command<ReplyT> &)> &callback,
        const std::function<void(Command<ReplyT> &, int)> &callback_error,
        const std::function<void(int)> &notify_free,
        double repeat,
        double after,
        bool free_memory,
        log::Logger &logger) {
    return new (std::nothrow) Command<ReplyT>(
                handle, id, cmd, callback, callback_error, notify_free, repeat, after, free_memory, logger);
}

template <class ReplyT> ReplyT Command<ReplyT>::reply() {
    lock_guard<mutex> lg(reply_guard_);
    if (!ok()) {
        logger_.warning() << cmd() << ": Accessing reply value while status != OK.";
    }
    return reply_val_;
}

template <class ReplyT> string Command<ReplyT>::cmd() const {
    return redox::util::vecToStr(cmd_);
}

template <class ReplyT> bool Command<ReplyT>::isExpectedReply(int type) {

    if (reply_obj_->type == type) {
        reply_status_ = OK_REPLY;
        return true;
    }

    if (checkErrorReply() || checkNilReply())
        return false;

    stringstream errorMessage;
    errorMessage << "Received reply of type " << reply_obj_->type << ", expected type " << type
                 << ".";
    last_error_ = errorMessage.str();
    logger_.error() << cmd() << ": " << last_error_;
    reply_status_ = WRONG_TYPE;
    return false;
}

template <class ReplyT> bool Command<ReplyT>::isExpectedReply(int typeA, int typeB) {

    if ((reply_obj_->type == typeA) || (reply_obj_->type == typeB)) {
        reply_status_ = OK_REPLY;
        return true;
    }

    if (checkErrorReply() || checkNilReply())
        return false;

    stringstream errorMessage;
    errorMessage << "Received reply of type " << reply_obj_->type << ", expected type " << typeA
                 << " or " << typeB << ".";
    last_error_ = errorMessage.str();
    logger_.error() << cmd() << ": " << last_error_;
    reply_status_ = WRONG_TYPE;
    return false;
}

template <class ReplyT> bool Command<ReplyT>::checkErrorReply() {

    if (reply_obj_->type == REDIS_REPLY_ERROR) {
        if (reply_obj_->str != 0) {
            last_error_ = reply_obj_->str;
        }

        logger_.error() << "checkErrorReply: " << cmd() << ": " << last_error_;
        reply_status_ = ERROR_REPLY;
        return true;
    }
    return false;
}

template <class ReplyT> bool Command<ReplyT>::checkNilReply() {

    if (reply_obj_->type == REDIS_REPLY_NIL) {
        logger_.warning() << cmd() << ": Nil reply.";
        reply_status_ = NIL_REPLY;
        return true;
    }
    return false;
}

template <class ReplyT> void Command<ReplyT>::invoke() {
    if (callback_)
        callback_(*this);
}

// ----------------------------------------------------------------------------
// Specializations of parseReplyObject for all expected return types
// 模版特例化 parseReplyObject
// ----------------------------------------------------------------------------

template <> void Command<redisReply *>::parseReplyObject() {
    if (!checkErrorReply())
        reply_status_ = OK_REPLY;
    reply_val_ = reply_obj_;
}

template <> void Command<string>::parseReplyObject() {
    if (!isExpectedReply(REDIS_REPLY_STRING, REDIS_REPLY_STATUS))
        return;
    reply_val_ = {reply_obj_->str, static_cast<size_t>(reply_obj_->len)};
}

template <> void Command<char *>::parseReplyObject() {
    if (!isExpectedReply(REDIS_REPLY_STRING, REDIS_REPLY_STATUS))
        return;
    reply_val_ = reply_obj_->str;
}

template <> void Command<int>::parseReplyObject() {

    if (!isExpectedReply(REDIS_REPLY_INTEGER))
        return;
    reply_val_ = (int)reply_obj_->integer;
}

template <> void Command<long long int>::parseReplyObject() {

    if (!isExpectedReply(REDIS_REPLY_INTEGER))
        return;
    reply_val_ = reply_obj_->integer;
}

template <> void Command<nullptr_t>::parseReplyObject() {

    if (!isExpectedReply(REDIS_REPLY_NIL))
        return;
    reply_val_ = nullptr;
}

template <> void Command<vector<string>>::parseReplyObject() {

    if (!isExpectedReply(REDIS_REPLY_ARRAY))
        return;

    for (size_t i = 0; i < reply_obj_->elements; i++) {
        redisReply *r = *(reply_obj_->element + i);
        reply_val_.emplace_back(r->str, r->len);
    }
}

template <> void Command<unordered_set<string>>::parseReplyObject() {

    if (!isExpectedReply(REDIS_REPLY_ARRAY))
        return;

    for (size_t i = 0; i < reply_obj_->elements; i++) {
        redisReply *r = *(reply_obj_->element + i);
        reply_val_.emplace(r->str, r->len);
    }
}

template <> void Command<set<string>>::parseReplyObject() {

    if (!isExpectedReply(REDIS_REPLY_ARRAY))
        return;

    for (size_t i = 0; i < reply_obj_->elements; i++) {
        redisReply *r = *(reply_obj_->element + i);
        reply_val_.emplace(r->str, r->len);
    }
}

// ----------------------------------------------------------------------------
// Explicit template instantiation for available types, so that the generated
// library contains them and we can keep the method definitions out of the
// header file.
// ----------------------------------------------------------------------------

template class Command<redisReply *>;
template class Command<string>;
template class Command<char *>;
template class Command<int>;
template class Command<long long int>;
template class Command<nullptr_t>;
template class Command<vector<string>>;
template class Command<set<string>>;
template class Command<unordered_set<string>>;

} // End namespace redox
