#ifndef REDOX_EV_HPP
#define REDOX_EV_HPP

// NOTE: 可以替换称对应的libev头文件
#include <hiredis/adapters/libev.h>

#include <unordered_map>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include "utils/rwlock.hpp"

namespace redox { namespace util {

/**
 * @brief The EV class 针对libev的包装，让libev用起来更简单点
 */
class EV
{
public:
    typedef std::function<void(int revents)> TSlotHandler;

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
     * @brief registerSlot 注册slot
     * @param watcher
     * @return sig
     */
    int64_t registerSlot(TSlotHandler watcher);

    /**
     * @brief emitSig 发射sig
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

    /**
     * @brief evloop 获取ev实例
     * @return
     */
    struct ev_loop *evloop();
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
    // Signal to exit
    std::atomic_bool m_to_exit = {false};
    // Event thread exited
    bool m_exited = false;
    std::mutex m_exit_lock;
    std::condition_variable m_exit_waiter;

    // For breaking the loop
    ev_async m_watcher_stop;

    // pre/post callback
    std::function<void()> m_prefix_callback;
    std::function<void()> m_postfix_callback;

    // Dynamically allocated libev event loop
    struct ev_loop *m_evloop;

    // asynchronous watchers
    std::unordered_map<int64_t, std::shared_ptr<ev_async>> m_watcher;
    std::unordered_map<std::shared_ptr<ev_async>, TSlotHandler> m_watch_handler;
    std::atomic_int64_t m_watcher_id_generator;
    util::RWLock m_watcher_lock;

    // Separate thread to have a non-blocking event loop
    std::thread m_event_loop_thread;
};

}}
#endif // REDOX_EV_HPP
