#include "db.hpp"

void Database::CreateTable(const std::string& table_name) {
    tables_.insert({table_name, new DbTable()});
}

void Database::DropTable(const std::string& table_name) {
    if (!tables_.contains(table_name)) {
        throw std::invalid_argument("");
    } else {
        DbTable* del = tables_.at(table_name);
        delete del;
        del = nullptr;
        tables_.erase(table_name);
    }
}

DbTable& Database::GetTable(const std::string& table_name) {
    return *tables_.at(table_name);
}

Database::Database(const Database& rhs) {
    for (auto const& [name, table] : rhs.tables_) {
        tables_[name] = new DbTable(*rhs.tables_.at(name));
    }
    // for (auto const& [name, table] : rhs.tables_) {
    //     DbTable* tmp = nullptr;
    //     try {
    //         tmp = new DbTable()
    //         tmp.row_col_capacity_ = table.row_col_capacity_;
    //         tmp.next_unique_id_ = table.next_unique_id_;
    //         tmp.col_descs_ = table.col_descs_;
    //         void** tmp2 = new void*[tmp.row_col_capacity_];
    //         // for (unsigned int i = 0; i < row_col_capacity_; ++i) {
    //         //     tmp2[i] = table.rows_[i];
    //         // }
    //     } catch (...) {
    //         throw;
    //     }
    // }
    // tables_ = rhs.tables_;
}

Database& Database::operator=(const Database& rhs) {
    if (this == &rhs) {
        return *this;
    }
    for (auto const& [name, table] : tables_) {
        delete tables_.at(name);
    }
    tables_.clear();
    for (auto const& [name, table] : rhs.tables_) {
        tables_[name]= new DbTable(*rhs.tables_.at(name));
    }
    return *this;
}

Database::~Database() {
    std::vector<std::string> names;
    for (auto const& [name, table] : tables_) {
        names.push_back(name);
    }
    for (unsigned int i = 0; i < names.size(); ++i) {
        DbTable* del = tables_.at(names.at(i));
        delete del;
        del = nullptr;
        tables_.erase(names.at(i));
    }
}

std::ostream& operator<<(std::ostream& os, const Database& db) {
    os << db.temp_;
    return os;
}