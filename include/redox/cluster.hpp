#ifndef REDOX_CLIENT_CLUSTER_H
#define REDOX_CLIENT_CLUSTER_H

#include <iostream>

#include "client.hpp"
#include "command.hpp"

#include "utils/logger.hpp"
#include "utils/helper.hpp"

namespace redox {

namespace cluster {

struct ClusterNode
{
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

    bool init(std::vector<std::string> &items, std::function<void(int)> connection_callback=nullptr);

    Redox           m_handle;               // 连接对象

    std::string     m_id;                   // 节点id
    std::string     m_client_host;          // 节点ip
    uint16_t        m_client_port;          // 节点端口
    uint32_t        m_flag;                 // 节点角色
    std::string     m_master_id;            // 如果是备节点，则保存主节点id
    std::string     m_link_state;           // 连接状态
    std::pair<uint32_t, uint32_t> m_slots;  // slots
};

typedef std::vector<std::shared_ptr<ClusterNode>> TClusterNodes;

class Cluster {
public:
    Cluster(std::ostream &log_stream = std::cout, log::Level log_level = log::Debug);
    ~Cluster();

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
     * @brief reflashRouteSelf 根据现有连接信息，刷新路由信息
     * @param connection_callback 连接错误处理函数
     * @return true/false 连接结果
     */
    bool reflashRouteSelf(std::function<void(int)> connection_callback = nullptr);

    /**
     * @brief reflashRoute 更新集群节点信息，以及状态，执行`CLUSTER INFO`得到集群状态
     * @return
     */
    bool reflashRoute(Redox &rdx, std::function<void(int)> connection_callback = nullptr);

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

        if (node_index >= m_nodes.size())
        {
            m_logger.error() << "key: " << key << ", "
                             << "slot:" << slot << ", "
                             << "node index:" << node_index << ", "
                             << "node size:"  << m_nodes.size();
            return ;
        }

        std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
        if (rdx == nullptr)
        {
            return ;
        }
        rdx->m_handle.command<ReplyT>(cmdline, callback);
    }

    /**
     * @brief command 异步执行命令
     * @param cmdline 命令
     */
    void command(const std::vector<std::string> &cmdline);

    /**
     * @brief commandSync 同步执行命令
     * @param cmdline 命令
     */
    template<typename ReplyT>
    bool commandSync(const std::vector<std::string> &cmdline,
                     const std::function<void(Command<ReplyT>&)> &callback = nullptr)
    {
        uint32_t node_index = 0;
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

        std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
        if (rdx == nullptr)
        {
            return false;
        }

        Command<ReplyT>& c = rdx->m_handle.commandSync<ReplyT>(cmdline);
        if (callback) {
            callback(c);
        }
        return true;
    }

    /**
     * @brief commandSync 同步执行命令
     * @param cmdline 命令
     */
    bool commandSync(const std::vector<std::string> &cmdline);

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

        if (node_index >= m_nodes.size()) {
            m_logger.error() << "key: " << key << ", " << "slot:" << slot << ", "
                             << "node index:" << node_index << ", " << "node size:"  << m_nodes.size();
            return ;
        }

        std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
        if (rdx == nullptr) { return ; }

        rdx->m_handle.command<ReplyT>(cmdline);
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

        if (node_index >= m_nodes.size()) {
            m_logger.error() << "key: " << key << ", " << "slot:" << slot << ", "
                             << "node index:" << node_index << ", " << "node size:"  << m_nodes.size();
            return false;
        }

        std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
        if (rdx == nullptr) {
            return false;
        }

        Command<ReplyT> &c = rdx->m_handle.commandSync<ReplyT>(cmdline);
        if (callback)
            callback(c);

        return true;
    }

    /**
     * @brief get
     * @param key
     * @return
     */
    std::string get(const std::string &key);

    /**
     * @brief set
     * @param key
     * @param value
     * @return
     */
    bool set(const std::string &key, const std::string &value);

    /**
     * @brief del
     * @param key
     * @return
     */
    bool del(const std::string &key);

    /**
     * @brief publish
     * @param topic
     * @param msg
     * @return
     */
    void publish(const std::string &topic, const std::string &msg);
private:
    /**
     * @brief findNodeBySlot 根据solt，计算出需要在那个节点执行命令
     * @param slot
     * @return 节点的索引
     */
    uint32_t findNodeBySlot(uint32_t slot);

private:
    TClusterNodes   m_nodes;
    // Logger
    log::Logger     m_logger;
    // stream
    std::ostream&   m_log_stream;
    // level
    log::Level      m_log_level;
    // connection callback function
    std::function<void (int)> m_connection_callback;
};

}}

#endif //REDOX_CLIENT_CLUSTER_H
