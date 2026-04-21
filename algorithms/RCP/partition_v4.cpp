#include "rcp_common.hpp"

#include <numeric>
#include <set>
#include <unordered_set>

using namespace rcp;

struct Args {
    fs::path work_dir;
    int partition_cnt = 8;
    fs::path assignment_out;
    fs::path metrics_out;
};

struct Component {
    int id = 0;
    long long cost = 0;
    std::vector<int> nodes;
};

struct Candidate {
    int partition_id = 0;
    long long overlap = 0;
    long long cost = 0;
};

static Args parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto need = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error(std::string("Missing value for ") + flag);
            }
            return argv[++i];
        };
        if (arg == "--work-dir") {
            args.work_dir = need("--work-dir");
        } else if (arg == "--k") {
            args.partition_cnt = std::stoi(need("--k"));
        } else if (arg == "--assignment-out") {
            args.assignment_out = need("--assignment-out");
        } else if (arg == "--metrics-out") {
            args.metrics_out = need("--metrics-out");
        } else if (arg == "--help") {
            std::cout << "Usage: partition_v4 --work-dir <dir> --k <partitions> --assignment-out <file> --metrics-out <file>\n";
            std::exit(0);
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }
    if (args.work_dir.empty() || args.partition_cnt <= 0 || args.assignment_out.empty() || args.metrics_out.empty()) {
        throw std::runtime_error("--work-dir, --k, --assignment-out and --metrics-out are required");
    }
    return args;
}

static std::vector<int> parse_nodes(const std::string& rhs) {
    std::vector<int> nodes;
    std::stringstream ss(rhs);
    std::string cell;
    while (std::getline(ss, cell, ',')) {
        cell = trim(cell);
        if (!cell.empty()) {
            nodes.push_back(std::stoi(cell));
        }
    }
    return nodes;
}

int main(int argc, char** argv) {
    try {
        Args args = parse_args(argc, argv);

        std::unordered_map<int, int> node_property_cost;
        int max_node_id = 0;
        std::unordered_map<std::string, int> property_count_by_label;
        {
            std::ifstream props_in(args.work_dir / "node_label_properties.tsv");
            std::string line;
            while (std::getline(props_in, line)) {
                if (line.empty()) {
                    continue;
                }
                auto parts = split_tsv(line);
                if (parts.size() >= 2) {
                    property_count_by_label[parts[0]] = std::stoi(parts[1]);
                }
            }
        }
        {
            std::ifstream nodes_in(args.work_dir / "new_output_nodes.txt");
            std::string line;
            std::getline(nodes_in, line);
            while (std::getline(nodes_in, line)) {
                if (line.empty()) {
                    continue;
                }
                auto parts = split_tsv(line);
                if (parts.size() < 2) {
                    continue;
                }
                int node_id = std::stoi(parts[0]);
                max_node_id = std::max(max_node_id, node_id);
                node_property_cost[node_id] = property_count_by_label[parts[1]];
            }
        }

        std::unordered_map<int, long long> component_costs;
        {
            std::ifstream in(args.work_dir / "connected_size_all_v4.txt");
            if (!in) {
                throw std::runtime_error("Missing connected_size_all_v4.txt");
            }
            int id;
            long long cost;
            while (in >> id >> cost) {
                component_costs[id] = cost;
            }
        }

        std::vector<Component> components;
        {
            std::ifstream in(args.work_dir / "region_component_v4.txt");
            if (!in) {
                throw std::runtime_error("Missing region_component_v4.txt");
            }
            std::string line;
            while (std::getline(in, line)) {
                if (line.empty()) {
                    continue;
                }
                auto pos = line.find(':');
                if (pos == std::string::npos) {
                    continue;
                }
                int id = std::stoi(line.substr(0, pos));
                Component c;
                c.id = id;
                c.cost = component_costs[id];
                c.nodes = parse_nodes(line.substr(pos + 1));
                components.push_back(std::move(c));
            }
        }
        std::sort(components.begin(), components.end(), [](const Component& a, const Component& b) {
            if (a.cost != b.cost) {
                return a.cost > b.cost;
            }
            return a.id < b.id;
        });

        long long total_cost = 0;
        for (const auto& component : components) {
            total_cost += component.cost;
        }
        long long threshold = args.partition_cnt > 0 ? total_cost / args.partition_cnt : total_cost;

        std::vector<long long> partition_costs(args.partition_cnt, 0);
        std::vector<std::vector<int>> partition_components(args.partition_cnt);
        std::vector<std::set<int>> partition_nodes(args.partition_cnt);
        std::vector<std::vector<int>> node_memberships(max_node_id + 1);

        int seed_count = std::min<int>(args.partition_cnt, components.size());
        for (int i = 0; i < seed_count; ++i) {
            const auto& component = components[i];
            partition_components[i].push_back(component.id);
            partition_costs[i] += component.cost;
            for (int node : component.nodes) {
                partition_nodes[i].insert(node);
                node_memberships[node].push_back(i);
            }
        }

        for (size_t idx = seed_count; idx < components.size(); ++idx) {
            const auto& component = components[idx];
            std::vector<Candidate> candidates;
            candidates.reserve(args.partition_cnt);
            for (int p = 0; p < args.partition_cnt; ++p) {
                long long overlap = 0;
                for (int node : component.nodes) {
                    if (partition_nodes[p].count(node)) {
                        ++overlap;
                    }
                }
                candidates.push_back({p, overlap, partition_costs[p]});
            }
            std::sort(candidates.begin(), candidates.end(), [](const Candidate& a, const Candidate& b) {
                if (a.overlap != b.overlap) {
                    return a.overlap > b.overlap;
                }
                if (a.cost != b.cost) {
                    return a.cost < b.cost;
                }
                return a.partition_id < b.partition_id;
            });

            int chosen = candidates.front().partition_id;
            for (const auto& candidate : candidates) {
                if (partition_costs[candidate.partition_id] + component.cost < threshold) {
                    chosen = candidate.partition_id;
                    break;
                }
            }

            long long duplicate_savings = 0;
            for (int node : component.nodes) {
                if (partition_nodes[chosen].count(node)) {
                    duplicate_savings += node_property_cost[node];
                }
            }
            partition_costs[chosen] += component.cost - duplicate_savings;
            total_cost -= duplicate_savings;
            threshold = args.partition_cnt > 0 ? total_cost / args.partition_cnt : total_cost;
            partition_components[chosen].push_back(component.id);
            for (int node : component.nodes) {
                partition_nodes[chosen].insert(node);
                node_memberships[node].push_back(chosen);
            }
        }

        std::ofstream part_out(args.work_dir / ("partition_result_all_" + std::to_string(args.partition_cnt) + "_v2.txt"));
        std::ofstream nodes_out(args.work_dir / ("region_node_component_" + std::to_string(args.partition_cnt) + "_v2.txt"));
        if (!part_out || !nodes_out) {
            throw std::runtime_error("Could not create partition outputs");
        }
        for (int p = 0; p < args.partition_cnt; ++p) {
            part_out << "partition_id: " << p << '\n';
            part_out << "partition_size: " << partition_costs[p] << '\n';
            part_out << "partition_element: ";
            for (size_t i = 0; i < partition_components[p].size(); ++i) {
                if (i) {
                    part_out << ",";
                }
                part_out << partition_components[p][i];
            }
            part_out << '\n';

            nodes_out << "partition_id: " << p << '\n';
            nodes_out << "partition_size: " << partition_nodes[p].size() << '\n';
            nodes_out << "partition_node_element: ";
            bool first = true;
            for (int node : partition_nodes[p]) {
                if (!first) {
                    nodes_out << ',';
                }
                first = false;
                nodes_out << node;
            }
            nodes_out << '\n';
        }

        std::vector<int> primary_assignment(max_node_id + 1, 0);
        int duplicated_nodes = 0;
        int duplicated_copies = 0;
        for (int node = 1; node <= max_node_id; ++node) {
            auto& memberships = node_memberships[node];
            if (memberships.empty()) {
                continue;
            }
            std::sort(memberships.begin(), memberships.end());
            memberships.erase(std::unique(memberships.begin(), memberships.end()), memberships.end());
            primary_assignment[node] = memberships.front();
            if (memberships.size() > 1) {
                ++duplicated_nodes;
                duplicated_copies += static_cast<int>(memberships.size()) - 1;
            }
        }

        std::ofstream assign_out(args.assignment_out);
        if (!assign_out) {
            throw std::runtime_error("Could not create assignment output");
        }
        for (int node = 1; node <= max_node_id; ++node) {
            assign_out << primary_assignment[node] << '\n';
        }

        std::ofstream metrics_out(args.metrics_out);
        if (!metrics_out) {
            throw std::runtime_error("Could not create metrics output");
        }
        metrics_out << "{\n";
        metrics_out << "  \"k\": " << args.partition_cnt << ",\n";
        metrics_out << "  \"component_count\": " << components.size() << ",\n";
        metrics_out << "  \"duplicated_nodes\": " << duplicated_nodes << ",\n";
        metrics_out << "  \"duplicated_node_copies\": " << duplicated_copies << ",\n";
        metrics_out << "  \"total_nodes\": " << max_node_id << ",\n";
        metrics_out << "  \"partition_costs\": [";
        for (int p = 0; p < args.partition_cnt; ++p) {
            if (p) {
                metrics_out << ", ";
            }
            metrics_out << partition_costs[p];
        }
        metrics_out << "]\n";
        metrics_out << "}\n";

        std::cerr << "[RCP] partition_v4 duplicated_nodes=" << duplicated_nodes << " copies=" << duplicated_copies << "\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[RCP][partition_v4] " << ex.what() << "\n";
        return 1;
    }
}
