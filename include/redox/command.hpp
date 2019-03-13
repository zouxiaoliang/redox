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

#pragma once

#include <string>
#include <vector>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <hiredis/adapters/libev.h>
#include <hiredis/async.h>

#include "utils/logger.hpp"

namespace redox {

namespace cluster {
class Cluster;
}

/**
 * The Command class represents a single command string to be sent to
 * a Redis server, for both synchronous and asynchronous usage. It manages
 * all of the state relevant to a single command string. A Command can also
 * represent a deferred or looping command, in which case the success or
 * error callbacks are invoked more than once.
 */
template <class ReplyT> class Command {

public:
    // Reply codes
    static const int NO_REPLY = -1;   // No reply yet
    static const int OK_REPLY = 0;    // Successful reply of the expected type
    static const int NIL_REPLY = 1;   // Got a nil reply
    static const int ERROR_REPLY = 2; // Got an error reply
    static const int SEND_ERROR = 3;  // Could not send to server
    static const int WRONG_TYPE = 4;  // Got reply, but it was not the expected type
    static const int TIMEOUT = 5;     // No reply, timed out

    static Command<ReplyT> * createCommand(redisAsyncContext *ctx,
                                           int64_t id,
                                           const std::vector<std::string> &cmd,
                                           const std::function<void(Command<ReplyT> &)> &callback,
                                           const std::function<void(redisAsyncContext*, int)> &callback_error,
                                           const std::function<void(redisAsyncContext*, int)> &notify_free,
                                           double repeat,
                                           double after,
                                           bool free_memory,
                                           log::Logger &logger);

    /**
     * Returns the reply status of this command.
     */
    int status() const { return reply_status_; }

    /**
     * @brief lastError 最后的错误信息
     * @return
     */
    std::string lastError() const { return last_error_; }

    /**
     * Returns true if this command got a successful reply.
     */
    bool ok() const { return reply_status_ == OK_REPLY; }

    /**
     * Returns the reply value, if the reply was successful (ok() == true).
     *
     * Create a copy of the reply and return it. Use a guard
     * to make sure we don't return a reply while it is being
     * modified.
     */
    ReplyT reply();

    /**
     * Tells the event loop to free memory for this command. The user is
     * responsible for calling this on synchronous or looping commands,
     * AKA when free_memory_ = false.
     * This is the only method in Command that has access to private members of Redox
     */
    void free();

    /**
     * This method returns once this command's callback has been invoked
     * (or would have been invoked if there is none) since the last call
     * to wait(). If it is the first call, then returns once the callback
     * is invoked for the first time.
     */
    void wait();

    /**
     * @brief notifyAll 通知当前命令已经处理完成
     */
    void notifyAll();

    /**
     * @brief moved 在集群环境下，如果连接的节点发生切换或者slot重新分配，会返回错误，并提示move到其他节点
     * @return
     */
    bool moved();

    /**
     * @brief moved 获取当前指令需要到那个节点运行
     * @return 节点信息
     */
    bool moved(std::pair<std::string, int32_t> &where);

    /**
     * Returns the command string represented by this object.
     */
    std::string cmd() const;

    /**
     * @brief processReply Handles a new reply from the server
     * @param r
     */
    void processReply(redisReply *r, std::function<void(const std::string, int32_t)> move_to_other = nullptr);

    /**
     * @brief parseReplyObject Invoke a user callback from the reply object.
     * This method is specialized for each ReplyT of Command.
     */
    void parseReplyObject();

    /**
     * @brief invoke Directly invoke the user callback if it exists
     */
    void invoke();

    /**
     * @brief checkErrorReply
     * @return
     */
    bool checkErrorReply();

    /**
     * @brief checkNilReply
     * @return
     */
    bool checkNilReply();

    /**
     * @brief isExpectedReply
     * @param type
     * @return
     */
    bool isExpectedReply(int type);

    /**
     * @brief isExpectedReply
     * @param typeA
     * @param typeB
     * @return
     */
    bool isExpectedReply(int typeA, int typeB);

    /**
     * @brief freeReply If needed, free the redisReply
     */
    void freeReply();

private:
    /**
     * @brief Command
     * @note Explicitly delete copy constructor and assignment operator,
     *  Command objects should never be copied because they hold
     *  state with a network resource.
     */
    Command(const Command &) = delete;
    Command &operator=(const Command &) = delete;

    /**
     * @brief Command constructor
     * @param ctx redis async context
     * @param id command id
     * @param cmd what is you command string
     * @param callback
     * @param callback_error when the command failed the function called
     * @param notity_free notify the client free this command object
     * @param repeat
     * @param after
     * @param free_memory is free?
     * @param logger
     */
    Command(redisAsyncContext *ctx,
            int64_t id,
            const std::vector<std::string> &cmd,
            const std::function<void(Command<ReplyT> &)> &callback,
            const std::function<void(redisAsyncContext*, int)> &callback_error,
            const std::function<void(redisAsyncContext*, int)> &notify_free,
            double repeat,
            double after,
            bool free_memory,
            log::Logger &logger);

public:
    // Allow public access to constructed data
    redisAsyncContext *ctx_;
    const int64_t id_;
    const std::vector<std::string> cmd_;
    const double repeat_;
    const double after_;
    const bool free_memory_;

    // The last server reply
    redisReply *reply_obj_ = nullptr;

    // User callback
    const std::function<void(Command<ReplyT> &)> callback_;
    const std::function<void(redisAsyncContext*, int)> callback_error_;
    const std::function<void(redisAsyncContext*, int)> notify_free_;

    // Place to store the reply value and status.
    ReplyT reply_val_;
    int reply_status_;
    std::string last_error_;

    // How many messages sent to server but not received reply
    std::atomic_int pending_ = {0};

    // Whether a repeating or delayed command is canceled
    std::atomic_bool canceled_ = {false};

    // libev timer watcher
    ev_timer timer_;
    std::mutex timer_guard_;

    // Access the reply value only when not being changed
    std::mutex reply_guard_;

    // For synchronous use
    std::condition_variable waiter_;
    std::mutex waiter_lock_;
    std::atomic_bool waiting_done_ = {false};

    // Passed on from Redox class
    log::Logger &logger_;

    // friend class
    friend class cluster::Cluster;
};

} // End namespace redis
