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
   public:
    using BatchesSetType = std::unordered_set<
        std::shared_ptr<shared_model::interface::TransactionBatch>,
        OnDemandOrderingService::BatchPointerHasher,
        shared_model::interface::BatchHashEquality>;

   private:
    mutable std::shared_mutex batches_cache_cs_;
    BatchesSetType batches_cache_, used_batches_cache_;

    uint64_t available_txs_cache_size_;
    uint64_t held_txs_cache_size_;

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
