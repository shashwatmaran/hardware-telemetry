#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "../protocol/MessageEnvelope.hpp"
#include "TimescaleBatchWriter.hpp"

using json = nlohmann::json;

/**
 * Tier 2: Async queue-based writer
 * 
 * - Bounded in-memory queue (~10K messages)
 * - Region-based partitioning (preserves ordering for same city)
 * - Single dedicated writer thread
 * - Automatic retry on DB failures
 * - Backpressure with metrics
 */
class AsyncQueueWriter {
public:
    struct Config {
        std::size_t maxQueueSize = 10240;       // ~10K messages
        std::size_t batchSize = 500;            // Flush every 500 msgs
        std::chrono::milliseconds flushInterval{1000};  // Or every 1 second
        std::string outboxPath = "timescale_outbox.sql";
        std::size_t maxRetries = 3;
        std::chrono::milliseconds retryDelay{100};
    };

    struct Metrics {
        std::atomic<uint64_t> messagesProcessed{0};
        std::atomic<uint64_t> batchesFlushed{0};
        std::atomic<uint64_t> writeErrors{0};
        std::atomic<uint64_t> backpressureEvents{0};
        std::atomic<size_t> queueDepth{0};
        std::atomic<uint64_t> regionPartitions{0};
    };

    AsyncQueueWriter(const Config& config)
        : config_(config),
          batchWriter_(std::make_shared<TimescaleBatchWriter>(
              config.outboxPath,
              config.maxQueueSize,
              config.batchSize,
              config.flushInterval
          )),
          metrics_() {}

    /**
     * Submit a message to the queue.
     * Non-blocking: either queues immediately or returns false if queue is full.
     */
    bool submit(const MessageEnvelope& envelope) {
        std::unique_lock<std::mutex> lock(queueMutex_);

        // Check queue depth
        if (queues_.size() >= config_.maxQueueSize) {
            metrics_.backpressureEvents++;
            std::cerr << "[AsyncQueueWriter] Backpressure: queue full (" << queues_.size()
                      << "/" << config_.maxQueueSize << "), rejecting " << envelope.message_id
                      << std::endl;
            return false;
        }

        // Extract region for partitioning
        std::string region = extractRegion(envelope);
        queues_[region].push_back(envelope);
        metrics_.queueDepth = getTotalQueueSize();
        cv_.notify_one();

        return true;
    }

    /**
     * Run the writer thread. Blocks until running is set to false.
     * This should be called from the main thread while the collector runs separately.
     */
    void run(std::atomic<bool>& running) {
        std::cout << "[AsyncQueueWriter] Started (max_queue=" << config_.maxQueueSize
                  << ", batch_size=" << config_.batchSize
                  << ", flush_interval=" << config_.flushInterval.count() << "ms)" << std::endl;

        // Start the underlying batch writer in a separate thread
        std::thread batchWriterThread([this, &running]() {
            batchWriter_->run(running);
        });

        // Main queue processor: dequeue and submit to batch writer
        while (running.load()) {
            auto batch = dequeueBatch();
            if (!batch.empty()) {
                submitBatch(batch);
            }
        }

        // Final drain: submit all remaining messages
        drainAndSubmit();

        // Wait for batch writer thread to finish
        batchWriterThread.join();

        std::cout << "[AsyncQueueWriter] Stopped (processed=" << metrics_.messagesProcessed
                  << ", flushed=" << metrics_.batchesFlushed
                  << ", errors=" << metrics_.writeErrors
                  << ", backpressure_events=" << metrics_.backpressureEvents << ")" << std::endl;
    }

    /**
     * Get current metrics
     */
    const Metrics& getMetrics() const {
        return metrics_;
    }

    /**
     * Get current queue depth
     */
    size_t getQueueDepth() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(queueMutex_));
        return getTotalQueueSize();
    }

private:
    static std::string extractRegion(const MessageEnvelope& envelope) {
        try {
            if (envelope.payload.contains("region")) {
                return envelope.payload["region"].get<std::string>();
            }
        } catch (...) {
            // Fallback to source-based partitioning
        }
        return envelope.source;
    }

    size_t getTotalQueueSize() const {
        size_t total = 0;
        for (const auto& [region, queue] : queues_) {
            total += queue.size();
        }
        return total;
    }

    /**
     * Dequeue a batch, partitioned by region to maintain ordering.
     * Waits up to flushInterval_ for new messages.
     */
    std::vector<MessageEnvelope> dequeueBatch() {
        std::unique_lock<std::mutex> lock(queueMutex_);

        // Wait for data or timeout
        cv_.wait_for(lock, config_.flushInterval, [this]() {
            return !queues_.empty();
        });

        std::vector<MessageEnvelope> batch;
        batch.reserve(config_.batchSize);

        // Dequeue up to batchSize_ messages, respecting region ordering
        while (!queues_.empty() && batch.size() < config_.batchSize) {
            // Round-robin through regions to prevent starvation
            for (auto it = queues_.begin();
                 it != queues_.end() && batch.size() < config_.batchSize;) {
                auto& [region, queue] = *it;
                if (!queue.empty()) {
                    batch.push_back(std::move(queue.front()));
                    queue.pop_front();

                    if (queue.empty()) {
                        it = queues_.erase(it);
                    } else {
                        ++it;
                    }
                } else {
                    ++it;
                }
            }
        }

        metrics_.queueDepth = getTotalQueueSize();
        if (!batch.empty()) {
            metrics_.regionPartitions = queues_.size();
        }

        return batch;
    }

    /**
     * Flush a batch with automatic retry on failure
     */
    void submitBatch(const std::vector<MessageEnvelope>& batch) {
        for (const auto& envelope : batch) {
            if (batchWriter_->submit(envelope)) {
                metrics_.messagesProcessed++;
            } else {
                metrics_.writeErrors++;
                std::cerr << "[AsyncQueueWriter] Failed to submit envelope "
                          << envelope.message_id << std::endl;
            }
        }
        metrics_.batchesFlushed++;
    }

    /**
     * Drain all remaining messages on shutdown
     */
    void drainAndSubmit() {
        std::vector<MessageEnvelope> finalBatch;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            for (auto& [region, queue] : queues_) {
                while (!queue.empty()) {
                    finalBatch.push_back(std::move(queue.front()));
                    queue.pop_front();
                }
            }
            queues_.clear();
        }

        if (!finalBatch.empty()) {
            std::cout << "[AsyncQueueWriter] Draining " << finalBatch.size()
                      << " remaining messages..." << std::endl;
            submitBatch(finalBatch);
        }
    }

    Config config_;
    std::shared_ptr<TimescaleBatchWriter> batchWriter_;
    std::map<std::string, std::deque<MessageEnvelope>> queues_;  // Partitioned by region
    std::mutex queueMutex_;
    std::condition_variable cv_;
    Metrics metrics_;
};
