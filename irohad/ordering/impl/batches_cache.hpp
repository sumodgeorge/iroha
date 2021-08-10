/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef IROHA_BATCHES_CACHE_HPP
#define IROHA_BATCHES_CACHE_HPP

#include "ordering/on_demand_ordering_service.hpp"

#include <memory>
#include <numeric>
#include <shared_mutex>
#include <unordered_set>

#include "consensus/round.hpp"

namespace shared_model::interface {
  class TransactionBatch;
}  // namespace shared_model::interface

namespace iroha::ordering {

  class BatchesCache {
    using BatchesSetType = std::unordered_set<
        std::shared_ptr<shared_model::interface::TransactionBatch>,
        OnDemandOrderingService::BatchPointerHasher,
        shared_model::interface::BatchHashEquality>;

    mutable std::shared_timed_mutex batches_cache_cs_;
    BatchesSetType batches_cache_, used_batches_cache_;

    uint64_t available_txs_cache_size_;
    uint64_t held_txs_cache_size_;

    uint64_t count(BatchesSetType const &src) {
      return std::accumulate(src.begin(),
                             src.end(),
                             0ull,
                             [](unsigned long long sum, auto const &batch) {
                               return sum + boost::size(batch->transactions());
                             });
    }

    void moveToAvailable(uint64_t count) {
      assert(held_txs_cache_size_ >= count);
      available_txs_cache_size_ += count;
      held_txs_cache_size_ -= count;
    }

    void moveToHeld(uint64_t count) {
      assert(available_txs_cache_size_ >= count);
      available_txs_cache_size_ -= count;
      held_txs_cache_size_ += count;
    }

   public:
    BatchesCache(BatchesCache const &) = delete;
    BatchesCache(BatchesCache &&) = default;
    BatchesCache &operator=(BatchesCache const &) = delete;
    BatchesCache &operator=(BatchesCache &&) = default;
    BatchesCache();

    uint64_t insertBatchToCache(
        std::shared_ptr<shared_model::interface::TransactionBatch> const
            &batch);

    void removeFromBatchesCache(
        const OnDemandOrderingService::HashesSetType &hashes);

    bool isEmptyBatchesCache() const;

    uint64_t txsCount() const;
    uint64_t availableTxsCount() const;

    void forCachedBatches(
        std::function<void(const BatchesSetType &)> const &f) const;

    void getTransactionsFromBatchesCache(
        size_t requested_tx_amount,
        std::vector<std::shared_ptr<shared_model::interface::Transaction>>
            &txs);

    void processReceivedProposal(
        OnDemandOrderingService::CollectionType batches);
  };

}  // namespace iroha::ordering

#endif  // IROHA_BATCHES_CACHE_HPP
