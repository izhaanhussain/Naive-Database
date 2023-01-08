#include "db_table.hpp"

// const unsigned int kRowGrowthRate = 2;

void DbTable::AddColumn(const std::pair<std::string, DataType>& col_desc) {
    if (col_descs_.size() == row_col_capacity_) {
        for (auto const& [key, value] : rows_) {
            unsigned int tmpcap = row_col_capacity_ * 2;
            void** array = new void*[tmpcap];
            for (unsigned int j = 0; j < (row_col_capacity_); ++j) {
                switch (col_descs_.at(j).second) {
                    case DataType::kString :  {
                        std::string& str = *(static_cast<std::string*>(value[j])); 
                        array[j] = static_cast<void*>(new std::string(str));
                        break;
                    }
                    case DataType::kInt : {
                        int& i = *(static_cast<int*>(value[j]));
                        array[j] = static_cast<void*>(new int(i));
                        break;
                    }
                    case DataType::kDouble : {
                        double& d = *(static_cast<double*>(value[j]));
                        array[j] = static_cast<void*>(new double(d));
                        break;
                    }
                }
            }
            for (unsigned int j = 0; j < col_descs_.size(); ++j) {
                switch (col_descs_.at(j).second) {
                    case DataType::kString :  {
                        auto* str = static_cast<std::string*>(rows_.at(key)[j]);
                        delete str;
                        str = nullptr;
                        break;
                    }
                    case DataType::kInt : {
                        int* i = static_cast<int*>(rows_.at(key)[j]);
                        delete i;
                        i = nullptr;
                        break;
                    }
                    case DataType::kDouble : {
                        auto* d = static_cast<double*>(rows_.at(key)[j]);
                        delete d;
                        d = nullptr;
                        break;
                    }
                }
            }
            delete[] rows_.at(key);
            rows_.at(key) = nullptr;
            rows_.at(key) = array;
        }
        row_col_capacity_ *= 2;
    }
    col_descs_.push_back(col_desc);
    for (auto const& [key, value] : rows_) {
        AddVal(value, col_desc.second);
    }
}

void DbTable::DeleteColumnByIdx(unsigned int col_idx) {
    if (col_idx > col_descs_.size() - 1) {
        throw std::out_of_range("");
    }
    if (col_descs_.size() == 1 && !rows_.empty()) {
        throw std::runtime_error("");
    }
    for (auto const& [key, row] : rows_) {
        switch(col_descs_.at(col_idx).second) {
            case DataType::kString : {
                auto* tmp = static_cast<std::string*>(row[col_idx]);
                delete tmp;
                tmp = nullptr;
                break;
            }
            case DataType::kInt : {
                int* tmp = static_cast<int*>(row[col_idx]);
                delete tmp;
                tmp = nullptr;
                break;
            }
            case DataType::kDouble : {
                auto* tmp = static_cast<double*>(row[col_idx]);
                delete tmp;
                tmp = nullptr;
                break;
            }
        }
        row[col_idx] = nullptr;
        for (unsigned int i = col_idx; i < col_descs_.size() - 1; ++i) {
            row[i] = row[i + 1];
        }
    }
    col_descs_.erase(col_descs_.begin() + col_idx);
}

void DbTable::AddRow(const std::initializer_list<std::string>& col_data) {
    if (col_data.size() != col_descs_.size()) {
        throw std::runtime_error("");
    }
    unsigned int counter = 0;
    void** array = new void*[row_col_capacity_];
    for (const std::string& str : col_data) {
        switch (col_descs_.at(counter).second) {
            case DataType::kString :  {
                array[counter] = static_cast<void*>(new std::string(str));
                break;
            }
            case DataType::kInt : {
                const int& i = std::stoi(str);
                array[counter] = static_cast<void*>(new int(i));
                break;
            }
            case DataType::kDouble : {
                const double& d = std::stod(str);
                array[counter] = static_cast<void*>(new double(d));
                break;
            }
        }
        counter++;
    }
    rows_.insert({next_unique_id_, array});
    next_unique_id_++;
}

void DbTable::DeleteRowById(unsigned int id) {
    if (!rows_.contains(id)) {
        throw std::runtime_error("");
    }
    for (unsigned int j = 0; j < col_descs_.size(); ++j) {
        switch (col_descs_.at(j).second) {
            case DataType::kString :  {
                auto* str = static_cast<std::string*>(rows_.at(id)[j]);
                delete str;
                str = nullptr;
                break;
            }
            case DataType::kInt : {
                int* i = static_cast<int*>(rows_.at(id)[j]);
                delete i;
                i = nullptr;
                break;
            }
            case DataType::kDouble : {
                auto* d = static_cast<double*>(rows_.at(id)[j]);
                delete d;
                d = nullptr;
                break;
            }
        }
    }
    delete[] rows_.at(id);
    rows_.at(id) = nullptr;
    rows_.erase(id);
}

DbTable::DbTable(const DbTable& rhs) {
    *this = rhs;
    temp_ = rhs.row_col_capacity_;
}

DbTable& DbTable::operator=(const DbTable& rhs) {
    if (this == &rhs) {
        return *this;
    }
    row_col_capacity_ = rhs.row_col_capacity_;
    next_unique_id_ = rhs.next_unique_id_;
    for (auto const& [key, row] : rows_) {
        for (unsigned int j = 0; j < col_descs_.size(); ++j) {
            switch (col_descs_.at(j).second) {
                case DataType::kString :  {
                    auto* str = static_cast<std::string*>(rows_.at(key)[j]);
                    delete str;
                    str = nullptr;
                    break;
                }
                case DataType::kInt : {
                    int* i = static_cast<int*>(rows_.at(key)[j]);
                    delete i;
                    i = nullptr;
                    break;
                }
                case DataType::kDouble : {
                    auto* d = static_cast<double*>(rows_.at(key)[j]);
                    delete d;
                    d = nullptr;
                    break;
                }
            }
        }
        delete[] rows_.at(key);
    }
    col_descs_ = rhs.col_descs_;
    rows_.clear();
    for (auto const& [key, row] : rhs.rows_) {
        void ** tmp = new void*[row_col_capacity_];
        rows_[key]= tmp;


        for (unsigned i = 0; i < col_descs_.size(); i++) {
            switch (col_descs_.at(i).second) {
            case DataType::kString :  {
                std::string* m = static_cast<std::string*>((rhs.rows_.at(key)[i]));
                void* toPass = static_cast<void*>(new std::string(*m));
                tmp[i] = toPass;
                
                break;
            }
            case DataType::kInt : {
                int* m = static_cast<int*>((rhs.rows_.at(key)[i]));
                void* toPass = static_cast<void*>(new int(*m));
                tmp[i] = toPass;
                break;
            }
            case DataType::kDouble : {
                double* m = static_cast<double*>((rhs.rows_.at(key)[i]));
                void* toPass = static_cast<void*>(new double(*m));
                tmp[i] = toPass;
                break;
            }
        }

        }
    }
    // for (auto const& [key, row] : rhs.rows_) {
    //     void** tmp = nullptr;
    //     try {
    //         tmp = new void*[rhs.row_col_capacity_];
    //     } catch(...) {
    //         throw;
    //     }
    //     delete[] rows_.at(key);
    //     rows_.at(key) = tmp;
    //     row_col_capacity_ = rhs.row_col_capacity_;
    //     next_unique_id_ = rhs.next_unique_id_;
    //     col_descs_ = rhs.col_descs_;
    //     for (unsigned int i = 0; i < row_col_capacity_; ++i) {
    //         rows_.at(key) = rhs.rows_.at(key);
    //     }
    //     rows_.insert({key, tmp});
    // }

    // temp_ = rhs.next_unique_id_;

    return *this;
}

DbTable::~DbTable() {
    for (unsigned int row_id = 0; row_id < next_unique_id_; ++row_id) {
        if(rows_.contains(row_id)) {
            for (unsigned int j = 0; j < col_descs_.size(); ++j) {
                switch (col_descs_.at(j).second) {
                    case DataType::kString :  {
                        auto* str = static_cast<std::string*>(rows_.at(row_id)[j]);
                        delete str;
                        str = nullptr;
                        break;
                    }
                    case DataType::kInt : {
                        int* i = static_cast<int*>(rows_.at(row_id)[j]);
                        delete i;
                        i = nullptr;
                        break;
                    }
                    case DataType::kDouble : {
                        auto* d = static_cast<double*>(rows_.at(row_id)[j]);
                        delete d;
                        d = nullptr;
                        break;
                    }
                }
            }
            delete[] rows_.at(row_id);
            rows_.at(row_id) = nullptr;
            rows_.erase(row_id);
        }
    }
}

std::ostream& operator<<(std::ostream& os, const DbTable& table) {
    for (unsigned int i = 0; i < table.col_descs_.size(); ++i) {
        if (i == table.col_descs_.size() - 1) {
            os << table.col_descs_.at(i).first << GetType(table.col_descs_.at(i).second);
        } else {
            os << table.col_descs_.at(i).first << GetType(table.col_descs_.at(i).second) << ", ";
        }
    }
    for (unsigned int i = 0; i < table.next_unique_id_; ++i) {
        os << '\n';
        for (unsigned int j = 0; j < table.col_descs_.size(); ++j) {
            switch (table.col_descs_.at(j).second) {
                case DataType::kString : {
                    os << *(static_cast<std::string*>(table.rows_.at(i)[j]));
                    break;
                }
                case DataType::kInt : {
                    os << *(static_cast<int*>(table.rows_.at(i)[j]));
                    break;
                }
                case DataType::kDouble : {
                    os << *(static_cast<double*>(table.rows_.at(i)[j]));
                    break;
                }
            }
            if (j != table.col_descs_.size() - 1) {
                os << ", ";
            }
        }
    }
    return os;
}

std::string GetType(DataType input) {
    std::string ret;
    switch (input) {
        case DataType::kString :  {
            ret = "(std::string)";
            break;
        }
        case DataType::kInt : {
            ret = "(int)";
            break;
        }
        case DataType::kDouble : {
            ret = "(double)";
            break;
        }
    }
    return ret;
}

void DbTable::AddVal(void** arr, DataType type) {
    switch (type) {
        case DataType::kString : 
            arr[col_descs_.size() - 1] = static_cast<void*>(new std::string(""));
            break;
        case DataType::kInt :
            arr[col_descs_.size() - 1] = static_cast<void*>(new int(0));
            break;
        case DataType::kDouble :
            arr[col_descs_.size() - 1] = static_cast<void*>(new double(0.0));
            break;
    }
}
