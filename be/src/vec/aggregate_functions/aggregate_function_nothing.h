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

#pragma once

#include <vec/aggregate_functions/aggregate_function.h>
#include <vec/columns/column.h>
#include <vec/data_types/data_type_nothing.h>
#include <vec/data_types/data_type_nullable.h>

namespace doris::vectorized {

/** Aggregate function that takes arbitrary number of arbitrary arguments and does nothing.
  */
class AggregateFunctionNothing final : public IAggregateFunctionHelper<AggregateFunctionNothing> {
public:
    AggregateFunctionNothing(const DataTypes& arguments, const Array& params)
            : IAggregateFunctionHelper<AggregateFunctionNothing>(arguments, params) {}

    String getName() const override { return "nothing"; }

    DataTypePtr getReturnType() const override {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    }

    void create(AggregateDataPtr) const override {}

    void destroy(AggregateDataPtr) const noexcept override {}

    bool hasTrivialDestructor() const override { return true; }

    size_t sizeOfData() const override { return 0; }

    size_t alignOfData() const override { return 1; }

    void add(AggregateDataPtr, const IColumn**, size_t, Arena*) const override {}

    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena*) const override {}

    // void serialize(ConstAggregateDataPtr, WriteBuffer &) const override
    // {
    // }

    // void deserialize(AggregateDataPtr, ReadBuffer &, Arena *) const override
    // {
    // }

    void insertResultInto(ConstAggregateDataPtr, IColumn& to) const override { to.insertDefault(); }

    const char* getHeaderFilePath() const override { return __FILE__; }
};

} // namespace doris::vectorized
