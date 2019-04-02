#ifndef REDOX_EV_HPP
#define REDOX_EV_HPP

#include <hiredis/async.h>
#include <hiredis/adapters/libev.h>

#include <unordered_map>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include "utils/rwlock.hpp"

namespace redox { namespace util {

#if 0
typedef void(*TWatchHandler)(struct ev_loop *loop, ev_async *async, int revent);
#else
typedef std::function<void(int revents)> TWatchHandler;
#endif

class EV
{
public:

private:
    EV(bool nowait,
       std::function<void()> prefixCallback = nullptr,
       std::function<void()> postfixCallbask = nullptr);

public:
    ~EV();

    /**
     * @brief createRedoxEv
     * @param nowait
     * @param prefixCallback
     * @param postfixCallbask
     * @return
     */
    static std::shared_ptr<EV> createRedoxEv(
            bool nowait = true,
            std::function<void()> prefixCallback = nullptr,
            std::function<void()> postfixCallbask = nullptr);


    /**
     * @brief registerWatcher 注册观察者
     * @param watcher
     * @return sig
     */
    int64_t registerWatcher(TWatchHandler watcher);

    /**
     * @brief emitSig
     * @param sig
     */
    void emitSig(int64_t sig);

    /**
     * @brief startup 启动事件环
     * @return
     */
    bool startup();

    /**
     * @brief shutdown 关闭事件环
     */
    void shutdown();

    /**
     * @brief wait
     */
    void wait();

    /**
     * @brief getExited 检查事件环是否推出
     * @return
     */
    bool getExited()
    {
        std::lock_guard<std::mutex> lg(m_exit_lock);
        return m_exited;
    }

    /**
     * @brief setExited 设置事件环退出标记
     * @param exited
     */
    void setExited(bool exited)
    {
        {
            std::lock_guard<std::mutex> lg(m_exit_lock);
            m_exited = exited;
        }
        m_exit_waiter.notify_one();
    }

    /**
     * @brief runOne 执行一次
     */
    void runOne();

private:
    /**
     * @brief runEventLoop 运行事件环
     */
    void runEventLoop();

private:
    EV(const EV &) = delete ;
    EV & operator = (const EV) = delete;

private:
    // No-wait mode for high-performance
    std::atomic_bool m_nowait = {false};

    // Variable and CV to know when the event loop stops running
    std::atomic_bool m_to_exit = {false}; // Signal to exit
    bool m_exited = false;  // Event thread exited
    std::mutex m_exit_lock;
    std::condition_variable m_exit_waiter;

    ev_async m_watcher_stop;            // For breaking the loop

    // pre/post callback
    std::function<void()> m_prefix_callback;
    std::function<void()> m_postfix_callback;

    // Dynamically allocated libev event loop
    struct ev_loop *m_evloop;

    // asynchronous watchers
    std::unordered_map<int64_t, std::shared_ptr<ev_async>> m_watcher;
    std::unordered_map<std::shared_ptr<ev_async>, TWatchHandler> m_watch_handler;
    std::atomic_int64_t m_watcher_id_generator;
    util::RWLock m_watcher_lock;

    // Separate thread to have a non-blocking event loop
    std::thread m_event_loop_thread;
};

}}
#endif // REDOX_EV_HPP
