#include "utils/redox_ev.hpp"

using namespace redox::util;

static void onBreakEventLoop(struct ev_loop *loop, ev_async *async, int revents)
{
    ev_break(loop, EVBREAK_ALL);
}

EV::EV(bool nowait, std::function<void ()> prefixCallback, std::function<void ()> postfixCallbask):
    m_nowait(nowait),
    m_prefix_callback(prefixCallback),
    m_postfix_callback(postfixCallbask)
{

}

EV::~EV()
{

}

std::shared_ptr<EV> EV::createRedoxEv(
        bool nowait,
        std::function<void ()> prefixCallback,
        std::function<void ()> postfixCallbask)
{
    std::shared_ptr<EV> ev_shared_ptr;

    auto ev = new (std::nothrow)EV(nowait, prefixCallback, postfixCallbask);
    if (nullptr == ev)
        return nullptr;

    signal(SIGPIPE, SIG_IGN);

    ev->m_evloop = ev_loop_new(EVFLAG_AUTO);
    if (nullptr == ev->m_evloop)
        return nullptr;



    ev_shared_ptr.reset(ev);
    return ev_shared_ptr;
}

int64_t EV::registerWatcher(TWatchHandler watcher)
{
    util::WriterGuard wg(m_watcher_lock);

    // 关联信号id与事件
    auto ew = std::make_shared<ev_async>();
    if (nullptr == ew)
        return -1;

    int64_t watcher_id = m_watcher_id_generator.fetch_add(1);

    m_watcher[watcher_id] = ew;
    m_watch_handler[ew] = watcher;

    return watcher_id;
}

void EV::emitSig(int64_t sig)
{
    util::ReaderGuard rg(m_watcher_lock);

    auto iter = m_watcher.find(sig);
    if (iter != m_watcher.end())
        ev_async_send(m_evloop, iter->second.get());
}

bool EV::startup()
{
    m_event_loop_thread = std::thread([this] { runEventLoop(); });
    return true;
}

void EV::shutdown()
{
    ev_async_send(m_evloop, &m_watcher_stop);;
}

void EV::wait()
{
    std::unique_lock<std::mutex> ul(m_exit_lock);
    m_exit_waiter.wait(ul, [this] {return m_exited;});
}

void EV::runOne()
{
    ev_run(m_evloop, EVRUN_NOWAIT);
}

void EV::runEventLoop()
{
    // Set up an async watcher to break the loop
    ev_async_init(&m_watcher_stop, onBreakEventLoop);
    ev_async_start(m_evloop, &m_watcher_stop);

    if (m_prefix_callback) m_prefix_callback();

    while (!m_to_exit)
    {
        if (m_nowait)
            ev_run(m_evloop, EVRUN_NOWAIT);
        else
            ev_run(m_evloop);
    }

    setExited(true);

    if (m_postfix_callback) m_postfix_callback();
}
