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

  class BatchesContext {
   public:
    using BatchesSetType = std::unordered_set<
        std::shared_ptr<shared_model::interface::TransactionBatch>,
        OnDemandOrderingService::BatchPointerHasher,
        shared_model::interface::BatchHashEquality>;

    BatchesContext(BatchesContext const &) = delete;
    BatchesContext(BatchesContext &&) = default;
    BatchesContext &operator=(BatchesContext const &) = delete;
    BatchesContext &operator=(BatchesContext &&) = default;
    BatchesContext() : tx_count_(0ull) {}

   private:
    uint64_t tx_count_;
    BatchesSetType batches_;

    uint64_t count(BatchesSetType const &src) {
      return std::accumulate(src.begin(),
                             src.end(),
                             0ull,
                             [](unsigned long long sum, auto const &batch) {
                               return sum + batch->transactions().size();
                             });
    }

   public:
    uint64_t getTxsCount() const {
      return tx_count_;
    }

    BatchesSetType const &getBatchesSet() const {
      return batches_;
    }

    bool insert(std::shared_ptr<shared_model::interface::TransactionBatch> const
                    &batch) {
      auto const inserted = batches_.insert(batch).second;
      if (inserted)
        tx_count_ += batch->transactions().size();

      assert(count(batches_) == tx_count_);
      return inserted;
    }

    bool removeBatch(
        std::shared_ptr<shared_model::interface::TransactionBatch> const
            &batch) {
      auto const was = batches_.size();
      batches_.erase(batch);
      if (batches_.size() != was)
        tx_count_ -= batch->transactions().size();

      assert(count(batches_) == tx_count_);
      return (was != batches_.size());
    }

    template <typename _Predic>
    void remove(_Predic &&pred) {
      bool process_iteration = true;
      for (auto it = batches_.begin();
           process_iteration && it != batches_.end();)
        if (std::forward<_Predic>(pred)(*it, process_iteration)) {
          auto const erased_size = (*it)->transactions().size();
          it = batches_.erase(it);

          assert(tx_count_ >= erased_size);
          tx_count_ -= erased_size;
        } else
          ++it;

      assert(count(batches_) == tx_count_);
    }

    inline void merge(BatchesContext &from) {
      auto it = from.batches_.begin();
      while (it != from.batches_.end())
        if (batches_.insert(*it).second) {
          auto const tx_count = (*it)->transactions().size();
          it = from.batches_.erase(it);

          tx_count_ += tx_count;
          from.tx_count_ -= tx_count;
        } else
          ++it;

      assert(count(batches_) == tx_count_);
      assert(count(from.batches_) == from.tx_count_);
    }
  };

  class BatchesCache {
   public:
    using BatchesSetType = BatchesContext::BatchesSetType;

   private:
    mutable std::shared_mutex batches_cache_cs_;
    BatchesContext batches_cache_, used_batches_cache_;

   public:
    BatchesCache(BatchesCache const &) = delete;
    BatchesCache(BatchesCache &&) = default;
    BatchesCache &operator=(BatchesCache const &) = delete;
    BatchesCache &operator=(BatchesCache &&) = default;
    BatchesCache() = default;

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
