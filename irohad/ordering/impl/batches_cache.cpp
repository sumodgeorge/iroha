/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ordering/impl/batches_cache.hpp"

#include "interfaces/iroha_internal/transaction_batch.hpp"
#include "interfaces/transaction.hpp"

namespace iroha::ordering {

  BatchesCache::BatchesCache()
      : available_txs_cache_size_(0ull), held_txs_cache_size_(0ull) {}

  uint64_t BatchesCache::insertBatchToCache(
      std::shared_ptr<shared_model::interface::TransactionBatch> const &batch) {
    std::unique_lock lock(batches_cache_cs_);
    if (used_batches_cache_.find(batch) == used_batches_cache_.end()) {
      batches_cache_.insert(batch);
      available_txs_cache_size_ += batch->transactions().size();
    }

    assert(count(batches_cache_) == available_txs_cache_size_);
    return available_txs_cache_size_;
  }

  void BatchesCache::removeFromBatchesCache(
      const OnDemandOrderingService::HashesSetType &hashes) {
    std::unique_lock lock(batches_cache_cs_);
    batches_cache_.merge(used_batches_cache_);
    moveToAvailable(held_txs_cache_size_);

    assert(used_batches_cache_.empty());
    if (!used_batches_cache_.empty()) {
      auto const remains_count = count(used_batches_cache_);
      moveToHeld(remains_count);
    }

    assert(count(batches_cache_) == available_txs_cache_size_);
    assert(count(used_batches_cache_) == held_txs_cache_size_);

    for (auto it = batches_cache_.begin(); it != batches_cache_.end();)
      if (std::any_of(it->get()->transactions().begin(),
                      it->get()->transactions().end(),
                      [&hashes](const auto &tx) {
                        return hashes.find(tx->hash()) != hashes.end();
                      })) {
        auto const erased_size = (*it)->transactions().size();
        it = batches_cache_.erase(it);
        assert(available_txs_cache_size_ >= erased_size);
        available_txs_cache_size_ -= erased_size;
      } else
        ++it;
  }

  bool BatchesCache::isEmptyBatchesCache() const {
    std::shared_lock lock(batches_cache_cs_);
    return batches_cache_.empty();
  }

  uint64_t BatchesCache::txsCount() const {
    std::shared_lock lock(batches_cache_cs_);
    return available_txs_cache_size_ + held_txs_cache_size_;
  }

  uint64_t BatchesCache::availableTxsCount() const {
    std::shared_lock lock(batches_cache_cs_);
    return available_txs_cache_size_;
  }

  void BatchesCache::forCachedBatches(
      std::function<void(const BatchesSetType &)> const &f) const {
    std::shared_lock lock(batches_cache_cs_);
    f(batches_cache_);
  }

  void BatchesCache::getTransactionsFromBatchesCache(
      size_t requested_tx_amount,
      std::vector<std::shared_ptr<shared_model::interface::Transaction>>
          &collection) {
    collection.clear();
    collection.reserve(requested_tx_amount);

    std::unique_lock lock(batches_cache_cs_);
    auto it = batches_cache_.begin();

    uint32_t depth_counter = 0ul;
    while (it != batches_cache_.end() && depth_counter < 8ul) {
      auto const txs_count = (*it)->transactions().size();
      if (collection.size() + txs_count > requested_tx_amount) {
        ++it;
        ++depth_counter;
        continue;
      }

      collection.insert(std::end(collection),
                        std::begin((*it)->transactions()),
                        std::end((*it)->transactions()));
      used_batches_cache_.insert(*it);
      it = batches_cache_.erase(it);
      moveToHeld(txs_count);
    }
  }

  void BatchesCache::processReceivedProposal(
      OnDemandOrderingService::CollectionType batches) {
    std::unique_lock lock(batches_cache_cs_);
    for (auto &batch : batches) {
      batches_cache_.erase(batch);
      used_batches_cache_.insert(batch);
      moveToHeld(batch->transactions().size());
    }
    assert(count(batches_cache_) == available_txs_cache_size_);
    assert(count(used_batches_cache_) == held_txs_cache_size_);
  }

}  // namespace iroha::ordering
