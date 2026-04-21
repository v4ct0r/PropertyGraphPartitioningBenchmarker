#include "rcp_common.hpp"

#include <numeric>
#include <unordered_set>

using namespace rcp;

struct Args {
    fs::path work_dir;
};

static Args parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--work-dir") {
            if (i + 1 >= argc) {
                throw std::runtime_error("Missing value for --work-dir");
            }
            args.work_dir = argv[++i];
        } else if (arg == "--help") {
            std::cout << "Usage: graph_v4 --work-dir <dir>\n";
            std::exit(0);
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }
    if (args.work_dir.empty()) {
        throw std::runtime_error("--work-dir is required");
    }
    return args;
}

struct Dsu {
    std::vector<int> parent;
    std::vector<int> rank;

    explicit Dsu(size_t n) : parent(n), rank(n, 0) {
        std::iota(parent.begin(), parent.end(), 0);
    }

    int find(int x) {
        if (parent[x] != x) {
            parent[x] = find(parent[x]);
        }
        return parent[x];
    }

    void unite(int a, int b) {
        a = find(a);
        b = find(b);
        if (a == b) {
            return;
        }
        if (rank[a] < rank[b]) {
            std::swap(a, b);
        }
        parent[b] = a;
        if (rank[a] == rank[b]) {
            ++rank[a];
        }
    }
};

int main(int argc, char** argv) {
    try {
        Args args = parse_args(argc, argv);
        std::unordered_map<std::string, int> property_count_by_label;
        std::ifstream props_in(args.work_dir / "node_label_properties.tsv");
        if (!props_in) {
            throw std::runtime_error("Missing node_label_properties.tsv");
        }
        std::string line;
        while (std::getline(props_in, line)) {
            if (line.empty()) {
                continue;
            }
            auto parts = split_tsv(line);
            if (parts.size() < 2) {
                continue;
            }
            property_count_by_label[parts[0]] = std::stoi(parts[1]);
        }

        std::unordered_map<int, std::string> node_label_by_idx;
        std::ifstream nodes_in(args.work_dir / "new_output_nodes.txt");
        if (!nodes_in) {
            throw std::runtime_error("Missing new_output_nodes.txt");
        }
        std::getline(nodes_in, line);
        while (std::getline(nodes_in, line)) {
            if (line.empty()) {
                continue;
            }
            auto parts = split_tsv(line);
            if (parts.size() < 2) {
                continue;
            }
            node_label_by_idx[std::stoi(parts[0])] = parts[1];
        }

        std::ofstream size_out(args.work_dir / "connected_size_all_v4.txt");
        std::ofstream region_out(args.work_dir / "region_component_v4.txt");
        std::ofstream label_out(args.work_dir / "connected_label_v4.txt");
        std::ofstream done_out(args.work_dir / "done_file_name_v4.txt");
        if (!size_out || !region_out || !label_out || !done_out) {
            throw std::runtime_error("Could not create graph_v4 outputs");
        }

        int component_id = 1;
        fs::path rel_dir = args.work_dir / "new_output_relationships";
        for (const auto& path : collect_csv_files(rel_dir)) {
            (void)path;
        }
        std::vector<fs::path> rel_files;
        for (const auto& entry : fs::directory_iterator(rel_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".txt") {
                rel_files.push_back(entry.path());
            }
        }
        std::sort(rel_files.begin(), rel_files.end());

        for (const auto& rel_file : rel_files) {
            std::ifstream in(rel_file);
            if (!in) {
                continue;
            }
            std::getline(in, line);
            std::vector<std::pair<int, int>> edges;
            std::unordered_map<int, int> degree;
            std::unordered_set<int> node_set;
            while (std::getline(in, line)) {
                if (line.empty()) {
                    continue;
                }
                auto parts = split_tsv(line);
                if (parts.size() < 3) {
                    continue;
                }
                int src = std::stoi(parts[1]);
                int dst = std::stoi(parts[2]);
                edges.push_back({src, dst});
                node_set.insert(src);
                node_set.insert(dst);
                degree[src] += 1;
                degree[dst] += 1;
            }
            if (node_set.empty()) {
                done_out << rel_file.filename().string() << '\n';
                continue;
            }

            std::vector<int> nodes(node_set.begin(), node_set.end());
            std::sort(nodes.begin(), nodes.end());
            std::unordered_map<int, int> local_index;
            local_index.reserve(nodes.size());
            for (size_t i = 0; i < nodes.size(); ++i) {
                local_index[nodes[i]] = static_cast<int>(i);
            }
            Dsu dsu(nodes.size());
            for (const auto& edge : edges) {
                dsu.unite(local_index[edge.first], local_index[edge.second]);
            }

            std::unordered_map<int, std::vector<int>> components;
            for (int node : nodes) {
                components[dsu.find(local_index[node])].push_back(node);
            }

            std::string rel_type = rel_file.stem().string();
            for (auto& kv : components) {
                auto& members = kv.second;
                std::sort(members.begin(), members.end());
                long long cost = 0;
                for (int node : members) {
                    const std::string& label = node_label_by_idx[node];
                    cost += property_count_by_label[label];
                    cost += degree[node];
                }
                size_out << component_id << ' ' << cost << '\n';
                region_out << component_id << ':';
                for (size_t i = 0; i < members.size(); ++i) {
                    if (i) {
                        region_out << ',';
                    }
                    region_out << members[i];
                }
                region_out << '\n';
                label_out << component_id << ':' << rel_type << '\n';
                ++component_id;
            }
            done_out << rel_file.filename().string() << '\n';
        }

        std::cerr << "[RCP] graph_v4 components=" << (component_id - 1) << "\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[RCP][graph_v4] " << ex.what() << "\n";
        return 1;
    }
}
