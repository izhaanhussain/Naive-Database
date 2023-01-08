#include <iostream>

#include "db.hpp"
#include "db_table.hpp"

int main() {
  DbTable example;
  std::pair<std::string, DataType> pair1;
  pair1.first = "Fruit";
  pair1.second = DataType::kString;
  std::pair<std::string, DataType> pair2;
  pair2.first = "Weight";
  pair2.second = DataType::kDouble;
  std::pair<std::string, DataType> pair3;
  pair3.first = "Amount";
  pair3.second = DataType::kInt;
  example.AddColumn(pair1);
  example.AddColumn(pair2);
  example.AddColumn(pair3);
  std::initializer_list<std::string> row1 = {"Apple", "45.22", "7"};
  std::initializer_list<std::string> row2 = {"Banana", "60.01", "9"};
  example.AddRow(row1);
  example.AddRow(row2);
  std::cout << example << std::endl;
  DbTable two;
  two = example;
  std::cout << two << std::endl;

}