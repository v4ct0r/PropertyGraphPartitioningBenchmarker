#include "rcp_common.hpp"

#include <set>

using namespace rcp;

struct Args {
    fs::path input_dir;
    fs::path work_dir;
    fs::path node_index;
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
        if (arg == "--input-dir") {
            args.input_dir = need("--input-dir");
        } else if (arg == "--work-dir") {
            args.work_dir = need("--work-dir");
        } else if (arg == "--node-index") {
            args.node_index = need("--node-index");
        } else if (arg == "--help") {
            std::cout << "Usage: csv2txt --input-dir <dir> --work-dir <dir> [--node-index <path>]\n";
            std::exit(0);
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }
    if (args.input_dir.empty() || args.work_dir.empty()) {
        throw std::runtime_error("--input-dir and --work-dir are required");
    }
    return args;
}

int main(int argc, char** argv) {
    try {
        Args args = parse_args(argc, argv);
        ensure_dir(args.work_dir);
        fs::path rel_dir = args.work_dir / "new_output_relationships";
        ensure_dir(rel_dir);

        auto existing_mapping = load_node_index(args.node_index);
        int generated_next = next_generated_index(existing_mapping);
        std::unordered_map<std::string, int> node_mapping = existing_mapping;
        std::unordered_map<int, std::string> node_label_by_idx;
        std::unordered_map<std::string, int> property_count_by_label;
        std::set<int> emitted_nodes;

        std::ofstream nodes_out(args.work_dir / "new_output_nodes.txt");
        std::ofstream rel_all_out(args.work_dir / "new_output_relationships.txt");
        std::ofstream node_meta_out(args.work_dir / "node_label_properties.tsv");
        std::ofstream schema_out(args.work_dir / "input_schema.tsv");
        if (!nodes_out || !rel_all_out || !node_meta_out || !schema_out) {
            throw std::runtime_error("Could not create work files under " + args.work_dir.string());
        }

        nodes_out << "idx\tlabel\n";
        rel_all_out << "rel_id\trel_type\tsrc_label\tdst_label\tsrc_idx\tdst_idx\n";
        schema_out << "kind\tfile_key\tlabel_or_rel\tproperty_columns\n";

        std::unordered_map<std::string, std::ofstream> per_rel_files;
        long long rel_id = 0;

        for (const auto& path : collect_csv_files(args.input_dir)) {
            CsvSchema schema = inspect_csv_schema(path);
            if (!schema.is_node && !schema.is_relationship) {
                continue;
            }
            std::ifstream in(path);
            std::string header_line;
            std::getline(in, header_line);

            if (schema.is_node) {
                property_count_by_label[schema.label] += schema.property_columns;
                schema_out << "node\t" << schema.file_key << "\t" << schema.label << "\t" << schema.property_columns << "\n";
                std::string line;
                while (std::getline(in, line)) {
                    if (line.empty()) {
                        continue;
                    }
                    auto fields = split_delimited(line, schema.delim);
                    if (schema.id_col >= static_cast<int>(fields.size())) {
                        continue;
                    }
                    std::string raw_id = fields[schema.id_col];
                    std::string key = node_key(schema.label, raw_id);
                    auto it = node_mapping.find(key);
                    int idx;
                    if (it == node_mapping.end()) {
                        idx = generated_next++;
                        node_mapping.emplace(key, idx);
                    } else {
                        idx = it->second;
                    }
                    node_label_by_idx[idx] = schema.label;
                    if (emitted_nodes.insert(idx).second) {
                        nodes_out << idx << '\t' << schema.label << '\n';
                    }
                }
                continue;
            }

            schema_out << "relationship\t" << schema.file_key << "\t" << schema.rel_type << "\t" << schema.property_columns << "\n";
            auto& rel_file = per_rel_files[schema.rel_type];
            if (!rel_file.is_open()) {
                rel_file.open(rel_dir / (schema.rel_type + ".txt"));
                rel_file << "rel_id\tsrc_idx\tdst_idx\n";
            }

            std::string line;
            while (std::getline(in, line)) {
                if (line.empty()) {
                    continue;
                }
                auto fields = split_delimited(line, schema.delim);
                if (schema.start_col >= static_cast<int>(fields.size()) || schema.end_col >= static_cast<int>(fields.size())) {
                    continue;
                }
                std::string src_raw = fields[schema.start_col];
                std::string dst_raw = fields[schema.end_col];
                std::string src_key = node_key(schema.src_label, src_raw);
                std::string dst_key = node_key(schema.dst_label, dst_raw);
                auto src_it = node_mapping.find(src_key);
                auto dst_it = node_mapping.find(dst_key);
                if (src_it == node_mapping.end() || dst_it == node_mapping.end()) {
                    continue;
                }
                int src_idx = src_it->second;
                int dst_idx = dst_it->second;
                ++rel_id;
                rel_all_out << rel_id << '\t' << schema.rel_type << '\t' << schema.src_label << '\t' << schema.dst_label << '\t' << src_idx << '\t' << dst_idx << '\n';
                rel_file << rel_id << '\t' << src_idx << '\t' << dst_idx << '\n';
            }
        }

        for (const auto& kv : property_count_by_label) {
            node_meta_out << kv.first << '\t' << kv.second << '\n';
        }

        if (args.node_index.empty()) {
            std::ofstream generated_index(args.work_dir / "generated_node_index.tsv");
            generated_index << "idx\ttype\traw_id\n";
            std::vector<std::pair<int, std::string>> rows;
            rows.reserve(node_mapping.size());
            for (const auto& kv : node_mapping) {
                rows.push_back({kv.second, kv.first});
            }
            std::sort(rows.begin(), rows.end());
            for (const auto& row : rows) {
                auto pos = row.second.find('\x1f');
                generated_index << row.first << '\t' << row.second.substr(0, pos) << '\t' << row.second.substr(pos + 1) << '\n';
            }
        }

        std::cerr << "[RCP] csv2txt nodes=" << emitted_nodes.size() << " rels=" << rel_id << "\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[RCP][csv2txt] " << ex.what() << "\n";
        return 1;
    }
}
