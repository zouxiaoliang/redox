#include "cluster_bata.hpp"

#include <string.h>
#include <cstddef>

redox::cluster_bata::Channel::Channel(redox::cluster_bata::Cluster *from):
    m_cluster(from), m_ctx(nullptr),
    m_id(), m_client_host(), m_client_port(), m_flag(0), m_master_id(), m_link_state(), m_slots(),
    m_connect_state(EN_NOT_YET_CONNECTED)
{}

redox::cluster_bata::Channel::~Channel()
{
    disconnect();
    fini();
}

bool redox::cluster_bata::Channel::init(std::vector<std::string> &items, std::function<void (int)> connection_callback)
{
    if (items.size() < 8)
    {
        return false;
    }

    // [0] Node ID
    m_id = items[0];

    // [1] ip:port
    {
        std::string::size_type pos = items[1].find(':');
        if (pos != std::string::npos)
        {

            const std::string port_str = items[1].substr(pos + 1);
            m_client_port = atoi(port_str.c_str());
            m_client_host = items[1].substr(0, pos);
        }
    }

    // [2] flags: master, slave, myself, fail, ...
    std::vector<std::string> flags;
    util::split(items[2], flags, ",");
    for (auto f: flags)
    {
        if (f == "myself") {
            m_flag |= myself;
        } else if (f == "master") {
            m_flag |= master;
        } else if (f == "slave") {
            m_flag |= slave;
        } else if (f == "fail") {
            m_flag |= fail;
        } else if (f == "fail?") {
            m_flag |= fail_maybe;
        } else if (f == "handshake") {
            m_flag |= handshake;
        } else if (f == "noaddr") {
            m_flag |= noaddr;
        } else if (f == "noflags") {
            m_flag |= noflags;
        }
    }

    // [3] if it is a slave, the Node ID of the master
    m_master_id = items[3];

    // [4] Time of the last pending PING still waiting for a reply.

    // [5] Time of the last PONG received.

    // [6] Configuration epoch for this node (see the Cluster specification).

    // [7] Status of the link to this node.
    m_link_state = items[7];

    // [8] Slots served...
    if (master == (m_flag & master))
    {
        std::string::size_type pos = items[8].find('-');
        if (pos == std::string::npos) {
            m_slots.first = atoi(items[8].c_str());
            m_slots.second = m_slots.first;
        } else {
            m_slots.first = atoi(items[8].substr(0, pos).c_str());
            m_slots.second = atoi(items[8].substr(pos + 1).c_str());
        }
    }

    return true;
}

void redox::cluster_bata::Channel::fini()
{
    setConnectState(EN_NOT_YET_CONNECTED);
    m_cluster = nullptr;
    m_ctx = nullptr;
}

bool redox::cluster_bata::Channel::connect()
{
    m_ctx = redisAsyncConnect(m_client_host.c_str(), m_client_port);

    return (nullptr != m_ctx);
}

void redox::cluster_bata::Channel::disconnect()
{
    std::cout << "redox::cluster_bata::Channel::disconnect (" << m_client_host << ":" << m_client_port << ")" << std::endl;
    if (nullptr != m_ctx)
        redisAsyncDisconnect(m_ctx);
}

redox::cluster_bata::Cluster::Cluster(std::ostream &log_stream, redox::log::Level log_level):
    m_logger(log_stream, log_level),
    m_log_stream(log_stream),
    m_log_level(log_level),
    m_user_connection_callback(nullptr)
{
    initEv();
}

redox::cluster_bata::Cluster::~Cluster()
{
    if (getRunning())
        stop();

    if(m_event_loop_thread.joinable())
        m_event_loop_thread.join();

    if (nullptr != m_evloop)
        ev_loop_destroy(m_evloop);
}

bool redox::cluster_bata::Cluster::connect(const char *host, uint32_t port, std::function<void (int)> connection_callback)
{
    // store the connection callback
    m_user_connection_callback = connection_callback;

    // connect to redis cluster node, use tcp port;
    Redox rdx(m_log_stream, m_log_level);
    if (!rdx.connect(host, port))
    {
        m_logger.error() << "connect to redis node failed, redis node:(" << host << ":" << port << ")";
        return false;
    }

    // check redis mode is cluster
    if (!isCluster(rdx))
    {
        m_logger.error() << "redis node is not cluster, redis node:(" << host << ":" << port << ")";
        rdx.disconnect();
        return false;
    }

    // check redis cluster status is ok?
    if (!isClusterOk(rdx))
    {
        m_logger.error() << "redis cluster node is not OK, redis node:(" << host << ":" << port << ")";
        rdx.disconnect();
        return false;
    }

    auto &c = rdx.commandSync<std::string>({"cluster", "nodes"});

    // slot range
    return connectNodes(c.reply(), m_user_connection_callback);
}

bool redox::cluster_bata::Cluster::connect(const char *nodes[], uint32_t node_count, std::function<void (int)> connection_callback)
{
    for (uint32_t i = 0; i < node_count; ++i)
    {
        std::vector<std::string> host_port;
        util::split(nodes[i], host_port, ":");

        if (this->connect(host_port[0].c_str(), atoi(host_port[1].c_str()), connection_callback))
        {
            return true;
        }
    }

    return false;
}

bool redox::cluster_bata::Cluster::connect(const std::vector<std::string> &nodes, std::function<void (int)> connection_callback)
{
    for (std::vector<std::string>::const_iterator iter = nodes.cbegin(); iter != nodes.cend(); ++iter)
    {
        std::vector<std::string> host_port;
        util::split(*iter, host_port, ":");

        if (this->connect(host_port[0].c_str(), atoi(host_port[1].c_str()), connection_callback))
        {
            return true;
        }
    }
    return false;
}

void redox::cluster_bata::Cluster::disconnect()
{
    // Signal the event loop to process this event
    ev_async_send(m_evloop, &m_watcher_disconnection);
}

void redox::cluster_bata::Cluster::start()
{
    m_event_loop_thread = std::thread([this] { runEventLoop(); });
}

void redox::cluster_bata::Cluster::stop()
{
    m_to_exit = true;
    m_logger.debug() << "cluster stop() called, breaking event loop";
    ev_async_send(m_evloop, &m_watcher_stop);
}

void redox::cluster_bata::Cluster::flush()
{

}

void redox::cluster_bata::Cluster::wait()
{
    std::unique_lock<std::mutex> ul(m_exit_lock);
    m_exit_waiter.wait(ul, [this] {return m_exited;});
}

bool redox::cluster_bata::Cluster::connectNodes(const std::string &cluster_nodes/*redisAsyncContext *ctx*/, std::function<void (int)> connection_callback)
{
    // if libev event loop not init, must init libe
    if (nullptr == m_evloop)
        if (!initEv())
            return false;

    // reset user_connection_callback;
    if (nullptr == connection_callback)
        connection_callback = m_user_connection_callback;

    // 提取节点信息
    std::vector<std::string> nodes;
    util::split(cluster_nodes, nodes, "\r\n");

    // 创建新的节点连接
    TClusterNodes cluster_node;
    for (std::string node: nodes)
    {
        std::vector<std::string> infos;
        util::split(node, infos, " ");

        std::shared_ptr<Channel> n = std::make_shared<Channel>(this);
        if (nullptr == n)
        {
            m_logger.warning() << "create cluster node connector failed, make_shared<ClusterNode> => nullptr, node info '"<< node << "'";
            continue;
        }

        // node init failed?
        if (!n->init(infos, connection_callback)) continue;

        if (!n->connect()) continue;

        // set note to ev loop failed?
        if (!initClusterNode(n.get())) continue;

        // send the signal to the event loop, has an new async connection.
        ev_async_send(m_evloop, &m_watcher_connection);

        // wait connect to the node successed.
        m_logger.info() << "waiting connect to the node: (" << n->m_client_host << ":" << n->m_client_port << ")";
        n->waitConnected();

        // it's ok!!
        if (n->getConnectState() != Channel::EN_CONNECTED)
            continue;
        else
            cluster_node.push_back(n);
    }

    // 更新集群的连接信息
    util::WriterGuard guard(m_nodes_lock);
    m_nodes.swap(cluster_node);

    return true;
}

bool redox::cluster_bata::Cluster::isCluster(redox::Redox &rdx)
{
    auto &r1 = rdx.commandSync<std::string>({"info"});
    if (!r1.ok())
    {
        return false;
    }
    // std::cout << r1.reply() << std::endl;
    std::vector<std::string> lines;
    util::split(r1.reply(), lines, "\r\n");
    for (std::vector<std::string>::iterator iter = lines.begin(); iter != lines.end(); ++iter)
    {
        std::string &line = *iter;
        // std::cout << "->" << line << "<-" << std::endl;
        if (line[0] == '#')
            continue;
        if (line.empty() || line[0] == '\n')
            continue;

        std::vector<std::string> kv;
        util::split(line, kv, ":");
        if (kv[0] == "cluster_enabled" && atoi(kv[1].c_str()) == 1)
            return true;
    }
    return false;
}

bool redox::cluster_bata::Cluster::isClusterOk(redox::Redox &rdx)
{
    auto &r2 = rdx.commandSync<std::string>({"cluster", "info"});
    if (!r2.ok())
    {
        rdx.logger_.debug() << "exec command: 'cluster info' failed";
        return false;
    }
    std::vector<std::string> lines;
    util::split(r2.reply(), lines, "\r\n");
    for (std::vector<std::string>::iterator iter = lines.begin(); iter != lines.end(); ++iter)
    {
        std::string &line = *iter;
        if (line[0] == '#')
            continue;
        if (line.empty() || line[0] == '\n')
            continue;

        std::vector<std::string> kv;
        util::split(line, kv, ":");

        if (kv.size() < 2)
            continue;

        if (kv[0] == "cluster_state" && kv[1] == "ok")
            return true;
    }
    return false;
}

int redox::cluster_bata::Cluster::ok()
{

    util::ReaderGuard guard(m_nodes_lock);

    // 获取最新的节点信息，并将节点信息与当前的集群信息进行比对，更新节点状态
    std::shared_ptr<Channel> self_node;
    for(auto n: m_nodes) {
        // 在self节点获取集群的节点信息，并与当前的节点信息进行比对
        if (Channel::myself == (n->m_flag & Channel::myself)) {
            self_node = n;
        }
    }

    if (nullptr == self_node) return EN_NO_OK;

    // 通过self节点，执行cluster node, 获取各个节点，检查节点状态是否有变化

    // 通过ping/pang检查节点通讯
    // 如果是slave则检查的是read状态

    // 如果是master则检查的是write状态

    // 返回最后的处理结果

    return EN_NO_OK;
}

bool redox::cluster_bata::Cluster::isNeedRefresh()
{
    return false;
}

void redox::cluster_bata::Cluster::channelConnectedCallback(const redisAsyncContext *c, int status) {
    Channel *node = (Channel*) c->data;
    Cluster *cluster = node->m_cluster;

    if (status != REDIS_OK) {
        cluster->m_logger.fatal() << "Could not connect to Redis cluster node: (" << node->m_client_host << ":" << node->m_client_port << ") error: "<< c->errstr;
        cluster->m_logger.fatal() << "Status: " << status;
        node->setConnectState(Channel::EN_CONNECT_ERROR);
        node->m_ctx = nullptr;

    } else {
        cluster->m_logger.info() << "Connected to Redis cluster node: (" << node->m_client_host << ":" << node->m_client_port << ")";
        // Disable hiredis automatically freeing reply objects
        c->c.reader->fn->freeObject = [](void *reply) {};
        node->setConnectState(Channel::EN_CONNECTED);
    }
}

void redox::cluster_bata::Cluster::channelDisconnectedCallback(const redisAsyncContext *c, int status) {
    Channel *node = (Channel*) c->data;
    Cluster *cluster = node->m_cluster;

    if (status != REDIS_OK) {
        cluster->m_logger.error() << "Disconnected from Redis on error, cluster node: (" << node->m_client_host << ":" << node->m_client_port << ") error: "<< c->errstr;
        node->setConnectState(Channel::EN_DISCONNECT_ERROR);
    } else {
        cluster->m_logger.info() << "Disconnected from Redis as planned.";
        node->setConnectState(Channel::EN_DISCONNECTED);
    }
    // 移除管理关联的上下文，hiredis内部会释放该信息
    node->m_ctx = nullptr;

    // 移除节点
    util::WriterGuard lock(cluster->m_nodes_lock);
    for (TClusterNodes::iterator iter = cluster->m_nodes.begin();
         iter != cluster->m_nodes.end();
         )
    {
        if ((*iter).get() == node)
        {
            iter = cluster->m_nodes.erase(iter);
        }
        else
        {
            ++iter;
        }
    }
}

void redox::cluster_bata::Cluster::asyncFreeCallback(int id)
{
    std::lock_guard<std::mutex> lg(this->m_free_queue_lock);
    this->m_commands_to_free.push(id);
    ev_async_send(this->m_evloop, &this->m_watcher_free);
}

void redox::cluster_bata::Cluster::breakEventLoop(struct ev_loop *loop, ev_async *async, int revents)
{
    ev_break(loop, EVBREAK_ALL);
}

bool redox::cluster_bata::Cluster::getRunning() {
    std::lock_guard<std::mutex> lg(m_running_lock);
    return m_running;
}

void redox::cluster_bata::Cluster::setRunning(bool running) {
    {
        std::lock_guard<std::mutex> lg(m_running_lock);
        m_running = running;
    }
    m_running_waiter.notify_one();
}

void redox::cluster_bata::Cluster::runEventLoop()
{
    // Set up asynchronous watcher which we signal every time we add a connection
    redox_ev_async_init(&m_watcher_connection, processConnection);
    ev_async_start(m_evloop, &m_watcher_connection);

    // Set up an async watcher which we signal every time we add a disconnection
    redox_ev_async_init(&m_watcher_disconnection, processDisconnection);
    ev_async_start(m_evloop, &m_watcher_disconnection);

    // Set up an async watcher which we signal every time we add a command
    redox_ev_async_init(&m_watcher_command, processQueuedCommands);
    ev_async_start(m_evloop, &m_watcher_command);

    // Set up an async watcher to break the loop
    redox_ev_async_init(&m_watcher_stop, breakEventLoop);
    ev_async_start(m_evloop, &m_watcher_stop);

    // Set up an async watcher which we signal every time we want a command freed
    redox_ev_async_init(&m_watcher_free, freeQueuedCommands);
    ev_async_start(m_evloop, &m_watcher_free);

    setRunning(true);
    m_logger.info() << "Run the event loop, using NOWAIT if enabled for maximum throughput by avoiding any sleepin. nowait: " << (m_nowait? "enabled" : "disabled");
    // Run the event loop, using NOWAIT if enabled for maximum throughput by avoiding any sleepin
    while (!m_to_exit) {
        if (m_nowait) {
            ev_run(m_evloop, EVRUN_NOWAIT);
        } else {
            m_logger.info() << "waiting the event ...";
            ev_run(m_evloop);
            m_logger.info() << "the event arrived!!!";
        }
    }

    m_logger.info() << "Stop signal detected. Closing down event loop.";

    // Signal event loop to free all commands
    freeAllCommands();
    // Wait to receive server replies for clean hiredis disconnect
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ev_run(m_evloop, EVRUN_NOWAIT);

    // disconnection redis async conections

    // check created and deleted
    long created = m_commands_created;
    long deleted = m_commands_deleted;
    if (created != deleted)
    {
        m_logger.error() << "All commands were not freed! " << deleted << "/" << created;
    }
    m_logger.debug() << "created: " << created << ", deleted: " << deleted << ", processed: " << m_commands_callbacked;

    // Let go for block_until_stopped method
    setExited(true);
    setRunning(false);

    m_logger.info() << "Event cluster thread exited.";
}

void redox::cluster_bata::Cluster::processConnection(struct ev_loop *loop, ev_async *async, int revents)
{
    auto self = (Cluster*)ev_userdata(loop);
    self->m_logger.debug() << "plan to create an async connection ...";
}

void redox::cluster_bata::Cluster::processDisconnection(struct ev_loop *loop, ev_async *async, int revents)
{
    Cluster *cluster = (Cluster *)ev_userdata(loop);
    util::WriterGuard guard(cluster->m_nodes_lock);
    for (auto node: cluster->m_nodes)
    {
        if (nullptr == node)
        {
            continue;
        }
    }
    cluster->m_nodes.clear();
}

void redox::cluster_bata::Cluster::processQueuedCommands(struct ev_loop *loop, ev_async *async, int revents)
{
    Cluster *cluster = (Cluster *)ev_userdata(loop);

    std::lock_guard<std::mutex> lg(cluster->m_queue_lock);

    while (!cluster->m_command_queue.empty()) {

        int64_t id = cluster->m_command_queue.front();
        cluster->m_command_queue.pop();

        if (cluster->processQueuedCommand<redisReply *>(id)) {
        } else if (cluster->processQueuedCommand<std::string>(id)) {
        } else if (cluster->processQueuedCommand<char *>(id)) {
        } else if (cluster->processQueuedCommand<int>(id)) {
        } else if (cluster->processQueuedCommand<long long int>(id)) {
        } else if (cluster->processQueuedCommand<std::nullptr_t>(id)) {
        } else if (cluster->processQueuedCommand<std::vector<std::string>>(id)) {
        } else if (cluster->processQueuedCommand<std::set<std::string>>(id)) {
        } else if (cluster->processQueuedCommand<std::unordered_set<std::string>>(id)) {
        } else {
            // throw std::runtime_error("Command pointer not found in any queue!");
            cluster->m_logger.error() << "Command pointer not found in any queue, id: " << id;
        }
    }
}

void redox::cluster_bata::Cluster::freeQueuedCommands(struct ev_loop *loop, ev_async *async, int revents)
{
    Cluster *rdx = (Cluster *)ev_userdata(loop);

    std::lock_guard<std::mutex> lg(rdx->m_free_queue_lock);

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

long redox::cluster_bata::Cluster::freeAllCommands()
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

void redox::cluster_bata::Cluster::debug_ping()
{
    auto f = [&](Command<std::string> & c) -> void {
        if(c.ok()) {
            m_logger.info() << "ping reply: " << c.reply();
        } else {
            m_logger.error() << "Command ping has error code " << c.status();
        }
    };

    for(auto n: m_nodes) {
        m_logger.debug() << "ping (" << n->m_client_host << ": " << n->m_client_port <<")";
        if (!commandSyncOn<std::string>(n, {"ping"}, f))
        {
            m_logger.error() << "exec command ping failed!!!";
        }
    }
}

std::shared_ptr<redox::cluster_bata::Channel> redox::cluster_bata::Cluster::findNodeBySlot(uint32_t slot)
{
    uint32_t node_id = 0;
    std::shared_ptr<Channel> node_ptr;
    for (TClusterNodes::iterator iter = m_nodes.begin(); iter != m_nodes.end(); ++iter)
    {
        const TClusterNodes::value_type &client = *iter;
        // 只在主节点中执行命令
        if (Channel::master != (client->m_flag & Channel::master))
        {
            continue;
        }
        if (slot >= client->m_slots.first && slot <= client->m_slots.second)
        {
            node_ptr = client;
            break;
        }
    }

    return node_ptr;
}

bool redox::cluster_bata::Cluster::initEv()
{
    signal(SIGPIPE, SIG_IGN);
    m_evloop = ev_loop_new(EVFLAG_AUTO);
    if (m_evloop == nullptr) {
        m_logger.fatal() << "Could not create a libev event loop.";
        return false;
    }
    ev_set_userdata(m_evloop, (void *)this); // Back-reference
    return true;
}

bool redox::cluster_bata::Cluster::initClusterNode(redox::cluster_bata::Channel *node)
{
    if (nullptr == node)
        return false;

    redisAsyncContext * ctx = node->m_ctx;
    ctx->data = (void *)node; // Back-reference

    if (ctx->err) {
        m_logger.fatal() << "Could not create a hiredis context: " << ctx->errstr;
        node->setConnectState(Channel::EN_INIT_ERROR);
        return false;
    }

    // Attach event loop to hiredis
    if (redisLibevAttach(m_evloop, ctx) != REDIS_OK) {
        m_logger.fatal() << "Could not attach libev event loop to hiredis.";
        node->setConnectState(Channel::EN_INIT_ERROR);
        return false;
    }

    // Set the callbacks to be invoked on server connection/disconnection
    if (redisAsyncSetConnectCallback(ctx, Cluster::channelConnectedCallback) != REDIS_OK) {
        m_logger.fatal() << "Could not attach connect callback to hiredis.";
        node->setConnectState(Channel::EN_INIT_ERROR);
        return false;
    }

    if (redisAsyncSetDisconnectCallback(ctx, Cluster::channelDisconnectedCallback) != REDIS_OK) {
        m_logger.fatal() << "Could not attach disconnect callback to hiredis.";
        node->setConnectState(Channel::EN_INIT_ERROR);
        return false;
    }

    return true;
}

// ---------------------------------
// get_command_map specializations
// ---------------------------------

template <>
std::unordered_map<long, redox::Command<redisReply *> *> &redox::cluster_bata::Cluster::getCommandMap<redisReply *>() {
    return m_commands_redis_reply;
}

template <>
std::unordered_map<long, redox::Command<std::string> *> &redox::cluster_bata::Cluster::getCommandMap<std::string>() {
    return m_commands_string;
}

template <>
std::unordered_map<long, redox::Command<char *> *> &redox::cluster_bata::Cluster::getCommandMap<char *>() {
    return m_commands_char_p;
}

template <>
std::unordered_map<long, redox::Command<int> *> &redox::cluster_bata::Cluster::getCommandMap<int>() {
    return m_commands_int;
}

template <>
std::unordered_map<long, redox::Command<long long int> *> &redox::cluster_bata::Cluster::getCommandMap<long long int>() {
    return m_commands_long_long_int;
}

template <>
std::unordered_map<long, redox::Command<std::nullptr_t> *> &redox::cluster_bata::Cluster::getCommandMap<std::nullptr_t>() {
    return m_commands_null;
}

template <>
std::unordered_map<long, redox::Command<std::vector<std::string>> *> &redox::cluster_bata::Cluster::getCommandMap<std::vector<std::string>>() {
    return m_commands_vector_string;
}

template <>
std::unordered_map<long, redox::Command<std::set<std::string>> *> &redox::cluster_bata::Cluster::getCommandMap<std::set<std::string>>() {
    return m_commands_set_string;
}

template <>
std::unordered_map<long, redox::Command<std::unordered_set<std::string>> *> & redox::cluster_bata::Cluster::getCommandMap<std::unordered_set<std::string>>() {
    return m_commands_unordered_set_string;
}

