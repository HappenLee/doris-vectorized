// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "vec/functions/function_totype.h"

#include <gtest/gtest.h>

#include "util/bitmap_value.h"
#include "function_test_util.h"

namespace doris {

using vectorized::Null;
using vectorized::DataSet;
using vectorized::TypeIndex;

TEST(function_bitmap_test, function_bitmap_min_test) {
    std::string func_name = "bitmap_min";
    std::vector<std::any> input_types = {TypeIndex::BitMap};

    auto bitmap1 = new BitmapValue(1);
    auto bitmap2 = new BitmapValue(std::vector<uint64_t>({1, 9999999}));
    auto empty_bitmap = new BitmapValue();
    DataSet data_set = {
            {{bitmap1}, (int64_t) 1},
            {{bitmap2}, (int64_t) 1},
            {{empty_bitmap}, (int64_t) 0},
            {{Null()}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt64, true>(func_name, input_types, data_set);
}

TEST(function_bitmap_test, function_bitmap_to_string_test) {
    std::string func_name = "bitmap_to_string";
    std::vector<std::any> input_types = {TypeIndex::BitMap};

    auto bitmap1 = new BitmapValue(1);
    auto bitmap2 = new BitmapValue(std::vector<uint64_t>({1, 9999999}));
    auto empty_bitmap = new BitmapValue();
    DataSet data_set = {
            {{bitmap1}, std::string("1")},
            {{bitmap2}, std::string("1,9999999")},
            {{empty_bitmap}, std::string("")},
            {{Null()}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
