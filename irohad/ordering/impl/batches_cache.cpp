/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ordering/impl/batches_cache.hpp"

#include "interfaces/iroha_internal/transaction_batch.hpp"
#include "interfaces/transaction.hpp"

namespace iroha::ordering {

  uint64_t BatchesCache::insertBatchToCache(
      std::shared_ptr<shared_model::interface::TransactionBatch> const &batch) {
    std::unique_lock lock(batches_cache_cs_);

    if (used_batches_cache_.getBatchesSet().find(batch)
        == used_batches_cache_.getBatchesSet().end())
      batches_cache_.insert(batch);

    return batches_cache_.getTxsCount();
  }

  void BatchesCache::removeFromBatchesCache(
      const OnDemandOrderingService::HashesSetType &hashes) {
    std::unique_lock lock(batches_cache_cs_);

    batches_cache_.merge(used_batches_cache_);
    assert(used_batches_cache_.getTxsCount() == 0ull);

    batches_cache_.remove([&](auto &batch, bool & /*process_iteration*/) {
      return std::any_of(batch->transactions().begin(),
                         batch->transactions().end(),
                         [&hashes](const auto &tx) {
                           return hashes.find(tx->hash()) != hashes.end();
                         });
    });
  }

  bool BatchesCache::isEmptyBatchesCache() const {
    std::shared_lock lock(batches_cache_cs_);
    return batches_cache_.getBatchesSet().empty();
  }

  uint64_t BatchesCache::txsCount() const {
    std::shared_lock lock(batches_cache_cs_);
    return batches_cache_.getTxsCount() + used_batches_cache_.getTxsCount();
  }

  uint64_t BatchesCache::availableTxsCount() const {
    std::shared_lock lock(batches_cache_cs_);
    return batches_cache_.getTxsCount();
  }

  void BatchesCache::forCachedBatches(
      std::function<void(const BatchesSetType &)> const &f) const {
    std::shared_lock lock(batches_cache_cs_);
    f(batches_cache_.getBatchesSet());
  }

  void BatchesCache::getTransactionsFromBatchesCache(
      size_t requested_tx_amount,
      std::vector<std::shared_ptr<shared_model::interface::Transaction>>
          &collection) {
    collection.clear();
    collection.reserve(requested_tx_amount);

    std::unique_lock lock(batches_cache_cs_);
    uint32_t depth_counter = 0ul;
    batches_cache_.remove([&](auto &batch, bool &process_iteration) {
      auto const txs_count = batch->transactions().size();
      if (collection.size() + txs_count > requested_tx_amount) {
        ++depth_counter;
        process_iteration = (depth_counter < 8ull);
        return false;
      }

      collection.insert(std::end(collection),
                        std::begin(batch->transactions()),
                        std::end(batch->transactions()));

      used_batches_cache_.insert(batch);
      return true;
    });
  }

  void BatchesCache::processReceivedProposal(
      OnDemandOrderingService::CollectionType batches) {
    std::unique_lock lock(batches_cache_cs_);
    for (auto &batch : batches) {
      batches_cache_.removeBatch(batch);
      used_batches_cache_.insert(batch);
    }
  }

}  // namespace iroha::ordering
