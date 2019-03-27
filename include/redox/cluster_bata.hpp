#ifndef REDOX_CLIENT_CLUSTER_XX_H
#define REDOX_CLIENT_CLUSTER_XX_H

#include <iostream>
#include <functional>
#include <algorithm>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include <string>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libev.h>

#include "client.hpp"
#include "command.hpp"

#include "utils/logger.hpp"
#include "utils/helper.hpp"
#include "utils/rwlock.hpp"

namespace redox {

namespace cluster_bata {

namespace {

template<typename tev, typename tcb>
void redox_ev_async_init(tev ev, tcb cb)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    ev_async_init(ev,cb);
#pragma GCC diagnostic pop
}

template<typename ttimer, typename tcb, typename tafter, typename trepeat>
void redox_ev_timer_init(ttimer timer, tcb cb, tafter after, trepeat repeat)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    ev_timer_init(timer,cb,after,repeat);
#pragma GCC diagnostic pop
}

} // anonymous

class Cluster;

/**
 * @brief The ClusterNode struct  集群节点信息
 */
struct ClusterNode
{
    // Connection states
    enum EConnectionStates{
        EN_NOT_YET_CONNECTED = 0,   // Starting state
        EN_CONNECTED = 1,           // Successfully connected
        EN_DISCONNECTED = 2,        // Successfully disconnected
        EN_CONNECT_ERROR = 3,       // Error connecting
        EN_DISCONNECT_ERROR = 4,    // Disconnected on error
        EN_INIT_ERROR = 5,          // Failed to init data structures
    };

    /// @ref https://redis.io/commands/cluster-nodes
    enum {
        myself      = (uint32_t)1 << 0,     // The node you are contacting.
        master      = (uint32_t)1 << 1,     // Node is a master.
        slave       = (uint32_t)1 << 2,     // Node is a slave.
        fail        = (uint32_t)1 << 3,     // Node is in FAIL state.
                                            // It was not reachable for multiple nodes that promoted the PFAIL state to FAIL.
        fail_maybe  = (uint32_t)1 << 4,     // Node is in PFAIL state.
                                            // Not reachable for the node you are contacting,
                                            // but still logically reachable (not in FAIL state).
        handshake   = (uint32_t)1 << 5,     // Untrusted node, we are handshaking.
        noaddr      = (uint32_t)1 << 6,     // No address known for this node.
        noflags     = 0                     // No flags at all.
    };

    ClusterNode(Cluster *from):
        m_cluster(from), m_ctx(nullptr),
        m_id(), m_client_host(), m_client_port(), m_flag(0), m_master_id(), m_link_state(), m_slots(),
        m_connect_state(EN_NOT_YET_CONNECTED)
    {}

    /**
     * @brief init 初始化集群节点信息
     * @param items 节点信息
     * @param connection_callback 连接会调函数
     * @return true/false
     */
    bool init(std::vector<std::string> &items, std::function<void(int)> connection_callback=nullptr);

    /**
     * @brief fini 对象释放
     */
    void fini();

    /**
     * @brief getConnectState 获取当前连接状态
     * @return 连接状态
     * @return: EConnectionStates
     */
    int getConnectState()
    {
        std::lock_guard<std::mutex> lk(m_connect_lock);
        return m_connect_state;
    }

    /**
     * @brief setConnectState 设置连接状态
     * @param connect_state 连接状态 EConnectionStates
     */
    void setConnectState(int connect_state)
    {
        {
            std::lock_guard<std::mutex> lk(m_connect_lock);
            m_connect_state = connect_state;
        }
        m_connect_waiter.notify_all();
    }

    /**
     * @brief waitConnected 等待连接完成
     */
    void waitConnected()
    {
        std::unique_lock<std::mutex> ul(m_connect_lock);
        m_connect_waiter.wait(ul, [this] { return m_connect_state != EN_NOT_YET_CONNECTED; });
    }

    // ---------- members ----------
    Cluster* m_cluster;                     // 保存所在的集群节点
    redisAsyncContext* m_ctx;               // hiredis context

    std::string     m_id;                   // 节点id
    std::string     m_client_host;          // 节点ip
    uint16_t        m_client_port;          // 节点端口
    uint32_t        m_flag;                 // 节点角色
    std::string     m_master_id;            // 如果是备节点，则保存主节点id
    std::string     m_link_state;           // 连接状态
    std::pair<uint32_t, uint32_t> m_slots;  // slots

    // Manage connection state
    int m_connect_state = EN_NOT_YET_CONNECTED;
    std::mutex m_connect_lock;
    std::condition_variable m_connect_waiter;
};

typedef std::vector<std::shared_ptr<ClusterNode>> TClusterNodes;

/**
 * @brief The Cluster class 集群客户端
 */
class Cluster {
public:
    Cluster(std::ostream &log_stream = std::cout, log::Level log_level = log::Debug);
    ~Cluster();

    /**
     * Enables or disables 'no-wait' mode. If enabled, no-wait mode means that the
     * event loop does not pause in between processing events. It can greatly increase
     * the throughput (commands per second),but means that the event thread will run at
     * 100% CPU. Enable when performance is critical and you can spare a core. Default
     * is off.
     *
     * Implementation note: When enabled, the event thread calls libev's ev_run in a
     * loop with the EVRUN_NOWAIT flag.
     */
    void noWait(bool state)
    {
        if (state)
            m_logger.info() << "No-wait mode enabled.";
        else
            m_logger.info() << "No-wait mode disabled.";
        m_nowait = state;
    }

    /**
     * @brief connect 连接redis 集群
     * @param host 集群中的一个节点
     * @param port 集群中的一个节点的端口
     * @param connection_callback 连接错误处理函数
     * @return true/false 连接结果
     */
    bool connect(const char *host, uint32_t port, std::function<void(int)> connection_callback = nullptr);

    /**
     * @brief connect 连接redis 集群
     * @param nodes 节点连接信息，例如 {"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"}
     * @param node_count 节点个数
     * @param connection_callback 连接错误处理函数
     * @return true/false 连接结果
     */
    bool connect(const char *nodes[] , uint32_t node_count, std::function<void(int)> connection_callback = nullptr);

    /**
     * @brief connect
     * @param nodes
     * @param connection_callback
     * @return
     */
    bool connect(const std::vector<std::string> &nodes, std::function<void(int)> connection_callback = nullptr);

    /**
     * @brief disconnect 断开与集群的所有连接
     */
    void disconnect();
    
    /**
     * @brief start 启动集群客户端线程
     */
    void start();

    /**
     * @brief stop 停止事件环
     */
    void stop();

    /**
     * @brief flush 等待所有命令执行完成
     */
    void flush();

    /**
     * @brief wait 等待事件环推出
     */
    void wait();

    /**
     * @brief reflashRouteSelf 根据现有连接信息，刷新路由信息
     * @param connection_callback 连接错误处理函数
     * @return true/false 连接结果
     */
    //bool reflashRouteSelf(std::function<void(int)> connection_callback = nullptr);

    /**
     * @brief connectNodes 连接到集群节点
     * @return
     */
    bool connectNodes(const std::string &cluster_nodes/*redisAsyncContext *ctx*/, std::function<void(int)> connection_callback = nullptr);

    /**
     * @brief isCluster 判断当前是否集群的redis环境
     * @param rdx
     * @return
     */
    static bool isCluster(Redox &rdx);

    /**
     * @brief isClusterOk 判断当前集群状态是否OK
     * @param rdx
     * @return
     */
    static bool isClusterOk(Redox &rdx);

    /**
     * @brief The EOK_STATUE enum 集群的操作状态
     */
    enum EOK_STATUE {
        EN_WRITE_MASK       = 0x0000fffff,
        EN_READ_MASK        = 0xffff00000,

        EN_NO_OK            = 0x0,
        EN_PART_WRITE_OK    = 0x1 << 0,
        EN_WRITE_OK         = 0x1 << 15 | EN_PART_WRITE_OK,

        EN_PART_READ_OK     = 0x1 << 16,
        EN_READ_OK          = 0x1 << 31 | EN_PART_READ_OK,
        EN_OK               = EN_WRITE_OK | EN_READ_OK,
    };

    /**
     * @brief
     * 判断集群的所有节点的连接都是OK的
     * 判断集群的所有节点写状态都是ok的
     * 判断集群的所有节点读状态都是ok的，如果master节点连接不上，则读取slave节点数据
     * @return true on ok, false on not ok
     */
    int ok();

    // Callbacks invoked on server connection/disconnection
    static void connectedCallback(const redisAsyncContext *c, int status);
    static void disconnectedCallback(const redisAsyncContext *c, int status);

    /**
     * @brief asyncFreeCallback 调用Command类的free接口，会执行该回掉函数
     * @param c 异步redis对象指针
     * @param id 命令id
     */
    static void asyncFreeCallback(const redisAsyncContext *c, int id);

    // Return the command map corresponding to the templated reply type
    template <class ReplyT>
    std::unordered_map<long, Command<ReplyT> *> &getCommandMap();

    template <class ReplyT>
    Command<ReplyT> *createCommand(
            redisAsyncContext *ctx,
            const std::vector<std::string> &cmd,
            const std::function<void(Command<ReplyT> &)> &callback = nullptr,
            double repeat = 0.0, double after = 0.0, bool free_memory = true) {
        {
            std::unique_lock<std::mutex> ul(m_running_lock);
            if (!m_running) {
                // throw std::runtime_error("[ERROR] Need to connect Redox before running commands!");
                m_logger.error() << "Need to connect Redox before running commands!";
                return nullptr;
            }
        }

        auto c = Command<ReplyT>::createCommand(ctx, m_commands_created.fetch_add(1), cmd,
                                                callback,
                                                std::bind(Cluster::disconnectedCallback, std::placeholders::_1, std::placeholders::_2),
                                                std::bind(Cluster::asyncFreeCallback, std::placeholders::_1, std::placeholders::_2),
                                                repeat, after, free_memory, m_logger);

        std::lock_guard<std::mutex> lg(m_queue_guard);
        std::lock_guard<std::mutex> lg2(m_command_map_guard);

        /* debug
        if (!getCommandMap<ReplyT>().insert({c->id_, c}).second)
        {
            std::string cmdline;
            util::join(c->cmd_.begin(), c->cmd_.end(), cmdline);
            m_logger.error() << "insert into commamd map failed, cmd: " << cmdline;
            delete c;
            return nullptr;
        }
        */
        getCommandMap<ReplyT>()[c->id_] = c;
        m_command_queue.push(c->id_);

        // Signal the event loop to process this command
        ev_async_send(m_evloop, &m_watcher_command);

        return c;
    }

    /**
     * @brief breakEventLoop
     * @param loop
     * @param async
     * @param revents
     */
    static void breakEventLoop(struct ev_loop *loop, ev_async *async, int revents);

    /**
     * @brief getRunning 获取事件环运行标记
     * @return
     */
    bool getRunning()
    {
        std::lock_guard<std::mutex> lg(m_running_lock);
        return m_running;
    }

    /**
     * @brief setRunning 设置事件环运行标记
     * @param running
     */
    void setRunning(bool running)
    {
        {
            std::lock_guard<std::mutex> lg(m_running_lock);
            m_running = running;
        }
        m_running_waiter.notify_one();
    }

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
     * @brief runEventLoop 执行libev事件环
     */
    void runEventLoop();

    /**
     * @brief processConnection 处理连接请求
     * @param loop
     * @param async
     * @param revents
     */
    static void processConnection(struct ev_loop *loop, ev_async *async, int revents);

    /**
     * @brief processDisconnection 处理断连
     * @param loop
     * @param async
     * @param revents
     */
    static void processDisconnection(struct ev_loop *loop, ev_async *async, int revents);

    /**
     * Return the given Command from the relevant command map, or nullptr if not there
     */
    template <class ReplyT> Command<ReplyT> *findCommand(long id)
    {
        std::lock_guard<std::mutex> lg(m_command_map_guard);

        auto &command_map = getCommandMap<ReplyT>();
        auto it = command_map.find(id);
        if (it == command_map.end())
            return nullptr;
        return it->second;
    }

    /**
     * @brief processQueuedCommands Send all commands in the command queue to the server
     * @param loop
     * @param async
     * @param revents
     */
    static void processQueuedCommands(struct ev_loop *loop, ev_async *async, int revents);

    /**
     * Process the command with the given ID. Return true if the command had the
     * templated type, and false if it was not in the command map of that type.
     */
    template <class ReplyT> bool processQueuedCommand(long id)
    {
        Command<ReplyT> *c = findCommand<ReplyT>(id);
        if (c == nullptr)
            return false;

        if ((c->repeat_ == 0) && (c->after_ == 0)) {
            submitToServer<ReplyT>(c);

        } else {

            c->timer_.data = (void *)c->id_;
            redox_ev_timer_init(&c->timer_, submitCommandCallback<ReplyT>, c->after_, c->repeat_);
            ev_timer_start(m_evloop, &c->timer_);

            c->timer_guard_.unlock();
        }

        return true;
    }

    /**
     * Callback given to libev for a Command's timer watcher, to be processed in
     * a deferred or looping state
     */
    template <class ReplyT>
    static void submitCommandCallback(struct ev_loop *loop, ev_timer *timer, int revents)
    {
        Cluster *cluster = (Cluster *)ev_userdata(loop);
        long id = (long)timer->data;

        Command<ReplyT> *c = cluster->findCommand<ReplyT>(id);
        if (c == nullptr) {
            cluster->m_logger.error()
                    << "Couldn't find Command " << id
                    << " in command_map (submitCommandCallback).";
            return;
        }

        submitToServer<ReplyT>(c);
    }

    /**
     * Submit an asynchronous command to the Redox server. Return true if succeeded, false otherwise.
     */
    template <class ReplyT> static bool submitToServer(Command<ReplyT> *c)
    {
        redisAsyncContext *ctx = c->ctx_;
        Cluster *cluster = ((ClusterNode *)ctx->data)->m_cluster;
        c->pending_++;

        // Construct a char** from the vector
        std::vector<const char *> argv;
        std::transform(c->cmd_.begin(), c->cmd_.end(), back_inserter(argv),
                       [](const std::string &s) { return s.c_str(); });

        // Construct a size_t* of string lengths from the vector
        std::vector<size_t> argvlen;
        std::transform(c->cmd_.begin(), c->cmd_.end(), back_inserter(argvlen),
                       [](const std::string &s) { return s.size(); });

        if (redisAsyncCommandArgv(ctx, commandCallback<ReplyT>, (void *)c->id_, argv.size(),
                                  &argv[0], &argvlen[0]) != REDIS_OK)
        {
            cluster->m_logger.error() << "Could not send \"" << c->cmd() << "\": " << ctx->errstr;
            c->reply_status_ = Command<ReplyT>::SEND_ERROR;
            c->invoke();
            return false;
        }

        return true;
    }

    /**
     * Callback given to hiredis to invoke when a reply is received
     */
    template <class ReplyT>
    static void commandCallback(redisAsyncContext *ctx, void *r, void *privdata)
    {
        ClusterNode *node = (ClusterNode *)ctx->data;
        Cluster *cluster = node->m_cluster;
        long id = (long)privdata;
        redisReply *reply_obj = (redisReply *)r;

        Command<ReplyT> *c = cluster->findCommand<ReplyT>(id);
        if (c == nullptr) {
            cluster->m_logger.error() << "command callback not find command in command maps!!!";
            freeReplyObject(reply_obj);
            return;
        }

        // TODO: 需要对moved的操作进行处理，重新执行该命令
        auto move_to_other = [cluster, node, c](const std::string &host, int32_t port) {
            util::ReaderGuard guard(cluster->m_nodes_lock);
            for (auto n: cluster->m_nodes) {
                if (n->m_client_host == host && n->m_client_port == port) {
                    cluster->m_logger.debug()  << "("
                                               << node->m_client_host << ":" << node->m_client_port
                                               << ") moved to node ("
                                               << n->m_client_host << ":" << n->m_client_port
                                               << ")";
                    c->ctx_ = n->m_ctx;
                    ++ c->pending_;
                    // 重新提交到事件环中
                    std::lock_guard<std::mutex> lg(cluster->m_queue_guard);
                    cluster->m_command_queue.push(c->id_);

                    // Signal the event loop to process this command
                    ev_async_send(cluster->m_evloop, &cluster->m_watcher_command);
                }
            }
        };

        cluster->m_commands_callbacked.fetch_add(1);
        c->processReply(reply_obj, move_to_other);
    }

    /**
     * @brief freeQueuedCommands Free all commands in the commands_to_free_ queue
     * @param loop
     * @param async
     * @param revents
     */
    static void freeQueuedCommands(struct ev_loop *loop, ev_async *async, int revents)
    {
        Cluster *rdx = (Cluster *)ev_userdata(loop);

        std::lock_guard<std::mutex> lg(rdx->m_free_queue_guard);

        while (!rdx->m_commands_to_free.empty()) {
            long id = rdx->m_commands_to_free.front();
            rdx->m_commands_to_free.pop();

            if (rdx->freeQueuedCommand<redisReply *>(id)) {
            } else if (rdx->freeQueuedCommand<std::string>(id)) {
            } else if (rdx->freeQueuedCommand<char *>(id)) {
            } else if (rdx->freeQueuedCommand<int>(id)) {
            } else if (rdx->freeQueuedCommand<long long int>(id)) {
            } else if (rdx->freeQueuedCommand<std::nullptr_t>(id)) {
            } else if (rdx->freeQueuedCommand<std::vector<std::string>>(id)) {
            } else if (rdx->freeQueuedCommand<std::set<std::string>>(id)) {
            } else if (rdx->freeQueuedCommand<std::unordered_set<std::string>>(id)) {
            } else {
            }
        }
    }

    /**
     * Free the command with the given ID. Return true if the command had the templated
     * type, and false if it was not in the command map of that type.
     */
    template <class ReplyT> bool freeQueuedCommand(long id)
    {
        Command<ReplyT> *c = findCommand<ReplyT>(id);
        if (c == nullptr)
            return false;

        c->freeReply();

        // Stop the libev timer if this is a repeating command
        if ((c->repeat_ != 0) || (c->after_ != 0)) {
            std::lock_guard<std::mutex> lg(c->timer_guard_);
            Cluster *node = ((ClusterNode *)c->ctx_->data)->m_cluster;
            ev_timer_stop(node->m_evloop, &c->timer_);
        }

        deregisterCommand<ReplyT>(c->id_);

        delete c;

        return true;
    }

    /**
     * Invoked by Command objects when they are completed. Removes them from the command map.
     */
    template <class ReplyT> void deregisterCommand(const long id)
    {
        std::lock_guard<std::mutex> lg1(m_command_map_guard);
        /* debug
        if (getCommandMap<ReplyT>().find(id) == getCommandMap<ReplyT>().end())
            m_logger.warning() << "id:" << id << " not in command map";
        */
        getCommandMap<ReplyT>().erase(id);
        m_commands_deleted += 1;
    }

    /**
     * @brief freeAllCommands Free all commands remaining in the command maps
     * @return
     */
    long freeAllCommands()
    {
        return freeAllCommandsOfType<redisReply *>()
                + freeAllCommandsOfType<std::string>()
                + freeAllCommandsOfType<char *>()
                + freeAllCommandsOfType<int>()
                + freeAllCommandsOfType<long long int>()
                + freeAllCommandsOfType<std::nullptr_t>()
                + freeAllCommandsOfType<std::vector<std::string> >()
                + freeAllCommandsOfType<std::set<std::string> >()
                + freeAllCommandsOfType<std::unordered_set<std::string> >();
    }

    /**
     * Helper function for freeAllCommands to access a specific command map
     */
    template <class ReplyT> long freeAllCommandsOfType()
    {
        std::lock_guard<std::mutex> lg(m_free_queue_guard);
        std::lock_guard<std::mutex> lg2(m_queue_guard);
        std::lock_guard<std::mutex> lg3(m_command_map_guard);

        auto &command_map = getCommandMap<ReplyT>();
        long len = command_map.size();

        for (auto &pair : command_map) {
            Command<ReplyT> *c = pair.second;

            c->freeReply();

            // Stop the libev timer if this is a repeating command
            if ((c->repeat_ != 0) || (c->after_ != 0)) {
                std::lock_guard<std::mutex> lg3(c->timer_guard_);
                Cluster *cluster = ((ClusterNode *)c->ctx_->data)->m_cluster;
                ev_timer_stop(cluster->m_evloop, &c->timer_);
            }

            delete c;
        }

        command_map.clear();
        m_commands_deleted += len;

        return len;
    }

    /**
     * @brief command 异步执行命令，通过callback函数对，结果进行处理
     * @param cmdline 命令
     * @param callback 结果处理函数
     */
    template<typename ReplyT>
    void command(const std::vector<std::string> &cmdline,
                 const std::function<void(Command<ReplyT>&)> &callback = nullptr)
    {
        std::string key = cmdline[1];
        uint32_t slot = util::hash_slot(key.c_str(), key.length());
        uint32_t node_index = this->findNodeBySlot(slot);

        util::ReaderGuard guard(m_nodes_lock);
        if (node_index >= m_nodes.size())
        {
            m_logger.error() << "key: " << key << ", "
                             << "slot:" << slot << ", "
                             << "node index:" << node_index << ", "
                             << "node size:"  << m_nodes.size();
            return ;
        }

        std::shared_ptr<ClusterNode> node = m_nodes[node_index];
        if (node == nullptr)
        {
            m_logger.error() << "cluster node id: " << node_index << " is nullptr";
            return ;
        }
        createCommand<ReplyT>(node->m_ctx, cmdline, callback);
    }

    /**
     * @brief commandSync 同步执行命令
     * @param cmdline 命令
     */
    template<typename ReplyT>
    bool commandSync(const std::vector<std::string> &cmdline,
                     const std::function<void(Command<ReplyT>&)> &callback = nullptr)
    {
        uint32_t node_index = 0;
        util::ReaderGuard guard(m_nodes_lock);
        if (cmdline.size() > 1)
        {
            std::string key = cmdline[1];
            uint32_t slot = util::hash_slot(key.c_str(), key.length());
            node_index = this->findNodeBySlot(slot);

            if (node_index >= m_nodes.size())
            {
                m_logger.error() << "key: " << key << ", "
                                 << "slot:" << slot << ", "
                                 << "node index:" << node_index << ", "
                                 << "node size:"  << m_nodes.size();
                 return false;
            }
        }

        std::shared_ptr<ClusterNode> node = m_nodes[node_index];
        if (node == nullptr)
        {
            return false;
        }
        auto c = createCommand<ReplyT>(node->m_ctx, cmdline, nullptr, 0, 0, false);
        if (nullptr == c) return false;

        c->wait();
        bool successed = c->ok();

        if (callback) {
            callback(*c);
        }
        c->free();
        return successed;
    }

    /**
     * @brief async evaluate scripts using the Lua interpreter
     * @param script, does not need to define a Lua function (and should not).
     *          It is just a Lua program that will run in the context of the Redis server.
     * @param keys, The arguments can be accessed by Lua using the KEYS global
     *          variable in the form of a one-based array (so KEYS[1], KEYS[2], ...).
     * @param args, All the additional arguments should not represent key names
     *          and can be accessed by Lua using the ARGV global variable,
     *          very similarly to what happens with keys (so ARGV[1], ARGV[2], ...).
     * @param callback, define a function that process the result.
     */
    template<typename ReplyT>
    void eval(const char *script,
              const std::vector<std::string> &keys = {},
              const std::vector<std::string> &args = {},
              const std::function<void(Command<ReplyT>&)> &callback = nullptr) {

        std::string key = util::join(keys.begin(), keys.end(), std::string("_"));
        uint32_t slot = util::hash_slot(key.c_str(), key.length());
        uint32_t node_index = this->findNodeBySlot(slot);

        std::vector<std::string> cmdline = {"eval", script};
        cmdline.push_back(std::to_string(keys.size()));
        for (std::vector<std::string>::const_iterator iter = keys.cbegin(); iter != keys.cend(); ++iter) {
            cmdline.push_back(*iter);
        }

        for (std::vector<std::string>::const_iterator iter = args.cbegin(); iter != args.cend(); ++iter) {
            cmdline.push_back(*iter);
        }

        util::ReaderGuard guard(m_nodes_lock);
        if (node_index >= m_nodes.size()) {
            m_logger.error() << "key: " << key << ", " << "slot:" << slot << ", "
                             << "node index:" << node_index << ", " << "node size:"  << m_nodes.size();
            return ;
        }

        std::shared_ptr<ClusterNode> node = m_nodes[node_index];
        if (node == nullptr) { return ; }

        createCommand<ReplyT>(node->m_ctx, cmdline);
    }

    /**
     * @brief evaluate scripts using the Lua interpreter
     * @param script, does not need to define a Lua function (and should not).
     *          It is just a Lua program that will run in the context of the Redis server.
     * @param keys, The arguments can be accessed by Lua using the KEYS global
     *          variable in the form of a one-based array (so KEYS[1], KEYS[2], ...).
     * @param args, All the additional arguments should not represent key names
     *          and can be accessed by Lua using the ARGV global variable,
     *          very similarly to what happens with keys (so ARGV[1], ARGV[2], ...).
     * @return redis return
     */
    template<typename ReplyT>
    bool evalSync(const char *script,
                  const std::vector<std::string> &keys = {},
                  const std::vector<std::string> &args = {},
                  const std::function<void(Command<ReplyT>&)> &callback = nullptr) {
        std::string key = util::join(keys.begin(), keys.end(), std::string("_"));
        uint32_t slot = util::hash_slot(key.c_str(), key.length());
        uint32_t node_index = this->findNodeBySlot(slot);

        std::vector<std::string> cmdline = {"eval", script};
        cmdline.push_back(std::to_string(keys.size()));
        for (std::vector<std::string>::const_iterator iter = keys.cbegin(); iter != keys.cend(); ++iter) {
            cmdline.push_back(*iter);
        }

        for (std::vector<std::string>::const_iterator iter = args.cbegin(); iter != args.cend(); ++iter) {
            cmdline.push_back(*iter);
        }

        util::ReaderGuard guard(m_nodes_lock);
        if (node_index >= m_nodes.size()) {
            m_logger.error() << "key: " << key << ", " << "slot:" << slot << ", "
                             << "node index:" << node_index << ", " << "node size:"  << m_nodes.size();
            return false;
        }

        std::shared_ptr<ClusterNode> node = m_nodes[node_index];
        if (node == nullptr) {
            return false;
        }

        Command<ReplyT> &c = commandSync<ReplyT>(cmdline);
        bool successed = c.ok();
        if (callback)
            callback(c);
        c.free();

        return successed;
    }

private:
    /**
     * @brief findNodeBySlot 根据solt，计算出需要在那个节点执行命令
     * @param slot
     * @return 节点的索引
     */
    uint32_t findNodeBySlot(uint32_t slot);

    /**
     * @brief initEv 初始化evloop
     * @return true/false
     */
    bool initEv();

    /**
     * @brief initClusterNode 绑定cluster node到evloop中
     * @return true/false
     */
    bool initClusterNode(ClusterNode* node);

private:
    // 集群节点
    TClusterNodes   m_nodes;
    util::RWLock    m_nodes_lock;

    // 日志记录器
    log::Logger     m_logger;
    // 日志流
    std::ostream&   m_log_stream;
    // 日志级别
    log::Level      m_log_level;

    // connection callback function
    std::function<void (int)> m_user_connection_callback;

    std::mutex m_connect_lock;
    std::condition_variable m_connect_waiter;

    // Dynamically allocated libev event loop
    struct ev_loop *m_evloop;

    // No-wait mode for high-performance
    std::atomic_bool m_nowait = {false};

    // Asynchronous watchers
    ev_async m_watcher_connection;      // for processing connection
    ev_async m_watcher_disconnection;   // for processing disconnection
    ev_async m_watcher_command;         // For processing commands
    ev_async m_watcher_stop;            // For breaking the loop
    ev_async m_watcher_free;            // For freeing commands

    // Track of Command objects allocated. Also provides unique Command IDs.
    std::atomic_int64_t m_commands_created = {0};
    std::atomic_int64_t m_commands_deleted = {0};
    std::atomic_int64_t m_commands_callbacked = {0};

    // Separate thread to have a non-blocking event loop
    std::thread m_event_loop_thread;

    // Variable and CV to know when the event loop starts running
    bool m_running = false;
    std::mutex m_running_lock;
    std::condition_variable m_running_waiter;

    // Variable and CV to know when the event loop stops running
    std::atomic_bool m_to_exit = {false}; // Signal to exit
    bool m_exited = false;  // Event thread exited
    std::mutex m_exit_lock;
    std::condition_variable m_exit_waiter;

    // Maps of each Command, fetchable by the unique ID number
    // In C++14, member variable templates will replace all of these types
    // with a single templated declaration
    // ---------
    // template<class ReplyT>
    // std::unordered_map<long, Command<ReplyT>*> commands_;
    // ---------
    std::unordered_map<long, Command<redisReply *> *> m_commands_redis_reply;
    std::unordered_map<long, Command<std::string> *> m_commands_string;
    std::unordered_map<long, Command<char *> *> m_commands_char_p;
    std::unordered_map<long, Command<int> *> m_commands_int;
    std::unordered_map<long, Command<long long int> *> m_commands_long_long_int;
    std::unordered_map<long, Command<std::nullptr_t> *> m_commands_null;
    std::unordered_map<long, Command<std::vector<std::string>> *> m_commands_vector_string;
    std::unordered_map<long, Command<std::set<std::string>> *> m_commands_set_string;
    std::unordered_map<long, Command<std::unordered_set<std::string>> *> m_commands_unordered_set_string;
    std::mutex m_command_map_guard; // Guards access to all of the above

    // Command IDs pending to be sent to the server
    std::queue<int64_t> m_command_queue;
    std::mutex m_queue_guard;

    // Commands IDs pending to be freed by the event loop
    std::queue<int64_t> m_commands_to_free;
    std::mutex m_free_queue_guard;

    friend struct ClusterNode;
};

}}

#endif //REDOX_CLIENT_CLUSTER_XX_H
