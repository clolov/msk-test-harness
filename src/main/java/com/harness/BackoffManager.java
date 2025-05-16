package com.harness;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class BackoffManager {
    private static final Logger logger = LoggerFactory.getLogger(BackoffManager.class);
    private static final int MAX_BACKOFF_SECONDS = 60; // 1 minute max backoff
    private static final int INITIAL_BACKOFF_SECONDS = 1;

    @Value("${backoff.delete.after.seconds:120}")
    private int deleteAfterSeconds; // Default 2 minutes

    @Autowired
    private KafkaManager kafkaManager;

    private final Map<String, AtomicInteger> backoffAttempts = new ConcurrentHashMap<>();
    private final Map<String, Instant> firstErrorTime = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @PostConstruct
    public void init() {
        // Start periodic check for topics that need deletion
        scheduler.scheduleAtFixedRate(this::checkForTopicDeletion, 30, 30, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void cleanup() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Handle an unretriable error for a topic by implementing exponential backoff
     * @param topic The topic that experienced the error
     * @return The number of seconds to wait before retrying
     */
    public int handleError(String topic) {
        // Record first error time if this is the first error
        firstErrorTime.putIfAbsent(topic, Instant.now());
        
        // Get current attempt count and increment
        AtomicInteger attempts = backoffAttempts.computeIfAbsent(topic, k -> new AtomicInteger(0));
        int currentAttempt = attempts.incrementAndGet();
        
        // Calculate backoff time with exponential increase
        int backoffSeconds = Math.min(
            INITIAL_BACKOFF_SECONDS * (1 << (currentAttempt - 1)), // 1s, 2s, 4s, 8s, etc.
            MAX_BACKOFF_SECONDS
        );
        
        logger.warn("Topic {} experienced error, backing off for {} seconds (attempt {})", 
            topic, backoffSeconds, currentAttempt);
            
        return backoffSeconds;
    }

    /**
     * Reset the backoff state for a topic
     * @param topic The topic to reset
     */
    public void resetBackoff(String topic) {
        backoffAttempts.remove(topic);
        firstErrorTime.remove(topic);
        logger.info("Reset backoff state for topic {}", topic);
    }

    /**
     * Check if topics need to be deleted due to extended error state
     */
    private void checkForTopicDeletion() {
        Instant now = Instant.now();
        
        firstErrorTime.forEach((topic, firstError) -> {
            Duration errorDuration = Duration.between(firstError, now);
            
            if (errorDuration.getSeconds() >= deleteAfterSeconds) {
                logger.warn("Topic {} has been in error state for {} seconds, deleting", 
                    topic, errorDuration.getSeconds());
                    
                // Delete the topic
                kafkaManager.deleteTestTopics(java.util.Collections.singletonList(topic));
                
                // Clean up our tracking
                resetBackoff(topic);
            }
        });
    }

    /**
     * Check if a topic is currently in backoff
     * @param topic The topic to check
     * @return true if the topic is in backoff, false otherwise
     */
    public boolean isInBackoff(String topic) {
        return backoffAttempts.containsKey(topic);
    }
}
