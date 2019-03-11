#include "cluster.hpp"

#include <string.h>

redox::cluster::Cluster::Cluster(std::ostream &log_stream, redox::log::Level log_level):
    m_logger(log_stream, log_level),
    m_log_stream(log_stream),
    m_log_level(log_level),
    m_connection_callback(nullptr)
{
    // do nothing
}

redox::cluster::Cluster::~Cluster()
{
    disconnect();
}

bool redox::cluster::Cluster::connect(const char *host, uint32_t port, std::function<void (int)> connection_callback)
{
    // store the connection callback
    m_connection_callback = connection_callback;

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

    // slot range
    return reflashRoute(rdx, m_connection_callback);
}

bool redox::cluster::Cluster::connect(const char *nodes[], uint32_t node_count, std::function<void (int)> connection_callback)
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

bool redox::cluster::Cluster::connect(const std::vector<std::string> &nodes, std::function<void (int)> connection_callback)
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

void redox::cluster::Cluster::disconnect()
{
    for (auto node: m_nodes)
    {
        if (nullptr == node)
        {
            continue;
        }
        node->m_handle.disconnect();
    }
    m_nodes.clear();
}

bool redox::cluster::Cluster::reflashRouteSelf(std::function<void (int)> connection_callback)
{
    for (auto node : m_nodes)
    {
        if (reflashRoute(node->m_handle, connection_callback))
        {
            return true;
        }
    }

    return false;
}

bool redox::cluster::Cluster::reflashRoute(redox::Redox &rdx, std::function<void (int)> connection_callback)
{
    if (nullptr == connection_callback)
        connection_callback = m_connection_callback;

    auto &r = rdx.commandSync<std::string>({"cluster", "nodes"});
    if (!r.ok())
    {
        m_logger.debug() << "get redis cluster nodes failed, redis node:("
                         << rdx.host_ << ":" << rdx.port_ << ")";
        rdx.disconnect();
        return false;
    }

    std::vector<std::string> nodes;
    util::split(r.reply(), nodes, "\r\n");

    TClusterNodes cluster_node;
    for (std::string node: nodes)
    {
        std::vector<std::string> infos;
        util::split(node, infos, " ");

        std::shared_ptr<ClusterNode> n = std::make_shared<ClusterNode>();
        if (nullptr == n)
        {
            m_logger.warning() << "create cluster node connector failed, make_shared<ClusterNode> => nullptr, node info '"<< node << "'";
            continue;
        }
        n->init(infos, connection_callback);
        cluster_node.push_back(n);
    }

    m_nodes.swap(cluster_node);
    for (auto node: cluster_node)
    {
        if (nullptr != node)
        {
            node->m_handle.disconnect();
        }
    }
    cluster_node.clear();
    rdx.disconnect();

    return true;
}

bool redox::cluster::Cluster::isCluster(redox::Redox &rdx)
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

bool redox::cluster::Cluster::isClusterOk(redox::Redox &rdx)
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

void redox::cluster::Cluster::command(const std::vector<std::string> &cmdline)
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
        m_logger.error() << "node ptr is null" << ", "
                         << "key: " << key << ", "
                         << "slot:" << slot << ", "
                         << "node index:" << node_index << ", "
                         << "node size:"  << m_nodes.size();
        return;
    }
    rdx->m_handle.command(cmdline);
}

bool redox::cluster::Cluster::commandSync(const std::vector<std::string> &cmdline)
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
        return false;
    }

    std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
    if (rdx == nullptr)
    {
        return false;
    }
    return rdx->m_handle.commandSync(cmdline);
}

std::string redox::cluster::Cluster::get(const std::string &key)
{
    uint32_t slot = util::hash_slot(key.c_str(), key.length());
    uint32_t node_index = this->findNodeBySlot(slot);

    if (node_index >= m_nodes.size())
    {
        m_logger.error() << "key: " << key << ", "
                         << "slot:" << slot << ", "
                         << "node index:" << node_index << ", "
                         << "node size:"  << m_nodes.size();
        return "";
    }

    std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
    if (rdx == nullptr)
    {
        return "";
    }
    return rdx->m_handle.get(key);
}

bool redox::cluster::Cluster::set(const std::string &key, const std::string &value)
{
    uint32_t slot = util::hash_slot(key.c_str(), key.length());
    uint32_t node_index = this->findNodeBySlot(slot);

    if (node_index >= m_nodes.size())
    {
        m_logger.error() << "key: " << key << ", "
                         << "slot:" << slot << ", "
                         << "node index:" << node_index << ", "
                         << "node size:"  << m_nodes.size();
        return false;
    }

    std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
    if (rdx == nullptr)
    {
        return false;
    }
    return rdx->m_handle.set(key, value);
}

bool redox::cluster::Cluster::del(const std::string &key)
{
    uint32_t slot = util::hash_slot(key.c_str(), key.length());
    uint32_t node_index = this->findNodeBySlot(slot);

    if (node_index >= m_nodes.size())
    {
        m_logger.error() << "key: " << key << ", "
                         << "slot:" << slot << ", "
                         << "node index:" << node_index << ", "
                         << "node size:"  << m_nodes.size();
        return false;
    }

    std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
    if (rdx == nullptr)
    {
        return false;
    }
    return rdx->m_handle.del(key);
}

void redox::cluster::Cluster::publish(const std::string &topic, const std::string &msg)
{
    uint32_t slot = util::hash_slot(topic.c_str(), topic.length());
    uint32_t node_index = this->findNodeBySlot(slot);

    if (node_index >= m_nodes.size())
    {
        m_logger.error() << "key: " << topic << ", "
                         << "slot:" << slot << ", "
                         << "node index:" << node_index << ", "
                         << "node size:"  << m_nodes.size();
        return;
    }

    std::shared_ptr<ClusterNode> rdx = m_nodes[node_index];
    if (rdx == nullptr)
    {
        return;
    }
    rdx->m_handle.publish(topic, msg);
}

uint32_t redox::cluster::Cluster::findNodeBySlot(uint32_t slot)
{
    uint32_t node_id = 0;
    for (TClusterNodes::iterator iter = m_nodes.begin(); iter != m_nodes.end(); ++iter)
    {
        const TClusterNodes::value_type &client = *iter;
        if (slot >= client->m_slots.first && slot <= client->m_slots.second)
        {
            break;
        }
        ++node_id;
    }

    return node_id;
}

bool redox::cluster::ClusterNode::init(std::vector<std::string> &items, std::function<void (int)> connection_callback)
{
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

    return m_handle.connect(m_client_host, m_client_port, connection_callback);
}

void redox::cluster::ClusterNode::fini()
{

}
