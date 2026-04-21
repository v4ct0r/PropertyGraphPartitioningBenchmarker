#pragma once

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace rcp {
namespace fs = std::filesystem;

inline std::string trim(std::string s) {
    auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
    s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
    return s;
}

inline bool starts_with(const std::string& s, const std::string& prefix) {
    return s.rfind(prefix, 0) == 0;
}

inline std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return s;
}

inline char detect_delimiter(const std::string& header) {
    size_t pipes = std::count(header.begin(), header.end(), '|');
    size_t commas = std::count(header.begin(), header.end(), ',');
    return pipes > commas ? '|' : ',';
}

inline std::vector<std::string> split_delimited(const std::string& line, char delim) {
    std::vector<std::string> fields;
    std::string cur;
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char ch = line[i];
        if (ch == '"') {
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                cur.push_back('"');
                ++i;
            } else {
                in_quotes = !in_quotes;
            }
            continue;
        }
        if (ch == delim && !in_quotes) {
            fields.push_back(cur);
            cur.clear();
            continue;
        }
        cur.push_back(ch);
    }
    fields.push_back(cur);
    return fields;
}

inline std::string extract_type_from_column(const std::string& column) {
    auto open = column.find('(');
    auto close = column.find(')', open == std::string::npos ? 0 : open + 1);
    if (open != std::string::npos && close != std::string::npos && close > open + 1) {
        return column.substr(open + 1, close - open - 1);
    }
    auto colon = column.find(':');
    std::string base = colon == std::string::npos ? column : column.substr(0, colon);
    auto dot = base.find('.');
    if (dot != std::string::npos) {
        base = base.substr(0, dot);
    }
    base = trim(base);
    if (!base.empty()) {
        return base;
    }
    return "Unknown";
}

inline std::string sanitize_name(std::string s) {
    for (char& ch : s) {
        bool ok = std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' || ch == '-';
        if (!ok) {
            ch = '_';
        }
    }
    return s;
}

inline std::string stem_without_csv(const fs::path& path) {
    return sanitize_name(path.stem().string());
}

inline std::vector<fs::path> collect_csv_files(const fs::path& root) {
    std::vector<fs::path> files;
    for (const auto& entry : fs::directory_iterator(root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() == ".csv") {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

struct CsvSchema {
    fs::path path;
    std::string file_key;
    char delim = ',';
    std::vector<std::string> header;
    bool is_node = false;
    bool is_relationship = false;
    int id_col = -1;
    int start_col = -1;
    int end_col = -1;
    std::string label;
    std::string src_label;
    std::string dst_label;
    std::string rel_type;
    int property_columns = 0;
};

inline CsvSchema inspect_csv_schema(const fs::path& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("Could not open " + path.string());
    }
    std::string header_line;
    if (!std::getline(in, header_line)) {
        throw std::runtime_error("Empty CSV file: " + path.string());
    }

    CsvSchema schema;
    schema.path = path;
    schema.file_key = stem_without_csv(path);
    schema.delim = detect_delimiter(header_line);
    schema.header = split_delimited(header_line, schema.delim);
    schema.rel_type = schema.file_key;

    for (int i = 0; i < static_cast<int>(schema.header.size()); ++i) {
        const std::string col = schema.header[i];
        if (col.find(":START_ID") != std::string::npos) {
            schema.start_col = i;
            schema.src_label = extract_type_from_column(col);
        } else if (col.find(":END_ID") != std::string::npos) {
            schema.end_col = i;
            schema.dst_label = extract_type_from_column(col);
        } else if (col.find(":ID") != std::string::npos) {
            schema.id_col = i;
            schema.label = extract_type_from_column(col);
        }
    }

    schema.is_relationship = schema.start_col >= 0 && schema.end_col >= 0;
    schema.is_node = !schema.is_relationship && schema.id_col >= 0;
    if (schema.label.empty() && schema.is_node) {
        schema.label = schema.file_key;
    }
    if (schema.src_label.empty() && schema.is_relationship) {
        schema.src_label = "UnknownSrc";
    }
    if (schema.dst_label.empty() && schema.is_relationship) {
        schema.dst_label = "UnknownDst";
    }

    int property_columns = 0;
    for (const auto& col : schema.header) {
        if (col.find(":ID") != std::string::npos || col.find(":START_ID") != std::string::npos ||
            col.find(":END_ID") != std::string::npos || col == ":LABEL" || col == ":Label") {
            continue;
        }
        ++property_columns;
    }
    schema.property_columns = property_columns;
    return schema;
}

inline std::string node_key(const std::string& type, const std::string& raw_id) {
    return type + "\x1f" + raw_id;
}

inline std::unordered_map<std::string, int> load_node_index(const fs::path& path) {
    std::unordered_map<std::string, int> mapping;
    if (path.empty()) {
        return mapping;
    }
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("Could not open node index: " + path.string());
    }
    std::string line;
    if (!std::getline(in, line)) {
        return mapping;
    }
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::stringstream ss(line);
        std::string idx, type, raw_id;
        std::getline(ss, idx, '\t');
        std::getline(ss, type, '\t');
        std::getline(ss, raw_id, '\t');
        if (idx.empty() || type.empty() || raw_id.empty()) {
            continue;
        }
        mapping[node_key(type, raw_id)] = std::stoi(idx);
    }
    return mapping;
}

inline int next_generated_index(const std::unordered_map<std::string, int>& mapping) {
    int best = 0;
    for (const auto& kv : mapping) {
        best = std::max(best, kv.second);
    }
    return best + 1;
}

inline void ensure_dir(const fs::path& path) {
    fs::create_directories(path);
}

inline std::vector<std::string> split_tsv(const std::string& line) {
    std::vector<std::string> out;
    std::stringstream ss(line);
    std::string cell;
    while (std::getline(ss, cell, '\t')) {
        out.push_back(cell);
    }
    return out;
}

}  // namespace rcp
