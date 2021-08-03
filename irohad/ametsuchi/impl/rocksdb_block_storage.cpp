/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ametsuchi/impl/rocksdb_block_storage.hpp"

#include "ametsuchi/impl/rocksdb_common.hpp"
#include "backend/protobuf/block.hpp"
#include "common/byteutils.hpp"
#include "logger/logger.hpp"

using namespace iroha::ametsuchi;

RocksDbBlockStorage::RocksDbBlockStorage(
    std::shared_ptr<RocksDBContext> db_context,
    std::shared_ptr<shared_model::interface::BlockJsonConverter> json_converter,
    logger::LoggerPtr log)
    : db_context_(std::move(db_context)),
      json_converter_(std::move(json_converter)),
      log_(std::move(log)) {}

bool RocksDbBlockStorage::insert(
    std::shared_ptr<const shared_model::interface::Block> block) {
  return json_converter_->serialize(*block).match(
      [&](const auto &block_json) {
        RocksDbCommon common(db_context_);

        if (auto result =
                forBlock<kDbOperation::kCheck, kDbEntry::kMustNotExist>(
                    common, block->height());
            expected::hasError(result)) {
          log_->warn(
              "Error while block {} insertion. Code: {}. Description: {}",
              block->height(),
              result.assumeError().code,
              result.assumeError().description);
          return false;
        }

        common.valueBuffer() = block_json.value;
        if (auto result = forBlock<kDbOperation::kPut>(common, block->height());
            expected::hasError(result)) {
          log_->error("Error while block {} storing. Code: {}. Description: {}",
                      block->height(),
                      result.assumeError().code,
                      result.assumeError().description);
          return false;
        }

        uint64_t count = 0ull;
        if (auto result =
                forBlocksTotalCount<kDbOperation::kGet, kDbEntry::kMustExist>(
                    common);
            expected::hasValue(result))
          count = *result.assumeValue();

        common.encode(count + 1ull);
        if (auto result =
                forBlocksTotalCount<kDbOperation::kPut, kDbEntry::kMustExist>(
                    common);
            expected::hasError(result)) {
          log_->error(
              "Error while block total count storing. Code: {}. Description: "
              "{}",
              result.assumeError().code,
              result.assumeError().description);
          return false;
        }

        return true;
      },
      [this](const auto &error) {
        log_->warn("Error while block serialization: {}", error.error);
        return false;
      });
}

boost::optional<std::unique_ptr<shared_model::interface::Block>>
RocksDbBlockStorage::fetch(
    shared_model::interface::types::HeightType height) const {
  RocksDbCommon common(db_context_);
  if (auto result =
          forBlock<kDbOperation::kGet, kDbEntry::kMustExist>(common, height);
      expected::hasError(result)) {
    log_->error("Error while block {} reading. Code: {}. Description: {}",
                height,
                result.assumeError().code,
                result.assumeError().description);
    return boost::none;
  }

  return json_converter_->deserialize(common.valueBuffer())
      .match(
          [&](auto &&block) {
            return boost::make_optional<
                std::unique_ptr<shared_model::interface::Block>>(
                std::move(block.value));
          },
          [&](const auto &error)
              -> boost::optional<
                  std::unique_ptr<shared_model::interface::Block>> {
            log_->warn("Error while block deserialization: {}", error.error);
            return boost::none;
          });
}

size_t RocksDbBlockStorage::size() const {
  RocksDbCommon common(db_context_);
  if (auto result =
          forBlocksTotalCount<kDbOperation::kGet, kDbEntry::kMustExist>(common);
      expected::hasValue(result))
    return *result.assumeValue();
  return 0ull;
}

void RocksDbBlockStorage::reload() {}

void RocksDbBlockStorage::clear() {
  RocksDbCommon common(db_context_);

  if (auto status = common.filterDelete(fmtstrings::kPathWsv); !status.ok())
    log_->error("Unable to delete WSV. Description: {}", status.ToString());

  if (auto status = common.filterDelete(fmtstrings::kPathStore); !status.ok())
    log_->error("Unable to delete STORE. Description: {}", status.ToString());
}

iroha::expected::Result<void, std::string> RocksDbBlockStorage::forEach(
    iroha::ametsuchi::BlockStorage::FunctionType function) const {
  uint64_t const blocks_count = size();
  for (uint64_t ix = 1; ix <= blocks_count; ++ix) {
    auto maybe_block = fetch(ix);
    if (maybe_block) {
      auto maybe_error = function(std::move(maybe_block).value());
      if (iroha::expected::hasError(maybe_error)) {
        return maybe_error.assumeError();
      }
    } else {
      return fmt::format("Failed to fetch block {}", ix);
    }
  }
  return {};
}
