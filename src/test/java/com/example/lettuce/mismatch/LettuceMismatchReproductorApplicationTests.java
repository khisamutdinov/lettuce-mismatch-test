package com.example.lettuce.mismatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.RedisClient;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@SpringBootTest
@Testcontainers
class LettuceMismatchReproductorApplicationTests {

    public static final String VALUE = "value";
    public static final String KEY = "key";
    private final Random random = new Random();

    private final ExecutorService executorService =
        Executors.newFixedThreadPool(20);
    private final ExecutorService redisThreadPool =
        Executors.newWorkStealingPool(
            Runtime.getRuntime().availableProcessors() * 10
        );

    @Container
    private static RedisContainer redis = new RedisContainer(
        RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)
    );

    @Test
    void contextLoads() {
        String redisUrl = redis.getRedisURI();
        try (RedisClient redisClient = RedisClient.create(redisUrl)) {
            var reactiveCommands = redisClient.connect().reactive();
            var mono = reactiveCommands
                .set("key", "zebra")
                .flatMap(respSet -> reactiveCommands.get("key"));
            StepVerifier.create(mono).expectNext("zebra").verifyComplete();
        }
    }

    @Test
    void test_mismatches_occure() throws Exception {
        try (
            RedisClient redisClient = RedisClient.create(redis.getRedisURI())
        ) {
            var reactiveCommands = redisClient.connect().reactive();
            AtomicInteger counter = new AtomicInteger(0);
            int max = 1000;
            AtomicInteger mismatchCounter = new AtomicInteger(0);
            for (int i = 0; i < max; i++) {
                final int finalI = i;
                executorService.submit(() ->
                    reactiveCommands
                        .set(KEY + finalI, VALUE + finalI)
                        .flatMap(respSet -> {
                            print(
                                Thread.currentThread().getName() +
                                ": set " +
                                KEY +
                                finalI +
                                " = value" +
                                finalI +
                                ":" +
                                respSet
                            );
                            return reactiveCommands
                                .get(KEY + finalI)
                                .defaultIfEmpty("")
                                .flatMap(respGet -> {
                                    if (!respGet.equals(VALUE + finalI)) {
                                        mismatchCounter.incrementAndGet();
                                        print(
                                            Thread.currentThread().getName() +
                                            ": MISMATCH 1!!! get key" +
                                            finalI +
                                            " = " +
                                            respGet
                                        );
                                    }
                                    if (
                                        random.nextInt() % 2 == 0 &&
                                        counter.get() < 300
                                    ) {
                                        counter.incrementAndGet();
                                        throw new OutOfMemoryError(
                                            "test out of memory error"
                                        );
                                    }
                                    int randomKey = random.nextInt(finalI + 1);
                                    return reactiveCommands
                                        .get(KEY + randomKey)
                                        .flatMap(respGetRandom -> {
                                            if (
                                                !respGetRandom.equals(
                                                    VALUE + randomKey
                                                )
                                            ) {
                                                mismatchCounter.incrementAndGet();
                                                print(
                                                    Thread.currentThread()
                                                        .getName() +
                                                    ": MISMATCH 2!!! get key" +
                                                    randomKey +
                                                    " = " +
                                                    respGetRandom
                                                );
                                            }
                                            final int finalRandomKey =
                                                random.nextInt(finalI + 1);
                                            return reactiveCommands
                                                .get(KEY + finalRandomKey)
                                                .map(respFinalRand -> {
                                                    if (
                                                        !respFinalRand.equals(
                                                            VALUE +
                                                            finalRandomKey
                                                        )
                                                    ) {
                                                        mismatchCounter.incrementAndGet();
                                                        print(
                                                            Thread.currentThread()
                                                                .getName() +
                                                            ": MISMATCH 3!!! get key" +
                                                            finalRandomKey +
                                                            " = " +
                                                            respFinalRand
                                                        );
                                                    }
                                                    return respFinalRand;
                                                });
                                        });
                                });
                        })
                        .subscribe()
                );
                TimeUnit.MILLISECONDS.sleep(5);
            }
            System.in.read();
            // Mismatches occure and test fails
            assertThat(mismatchCounter.get()).isEqualTo(0);
        }
    }

    @Test
    void test_mismatches_not_occure() throws Exception {
        try (
            RedisClient redisClient = RedisClient.create(redis.getRedisURI())
        ) {
            var reactiveCommands = redisClient.connect().reactive();
            AtomicInteger counter = new AtomicInteger(0);
            int max = 1000;
            AtomicInteger mismatchCounter = new AtomicInteger(0);
            for (int i = 0; i < max; i++) {
                final int finalI = i;
                executorService.submit(() ->
                    reactiveCommands
                        .set(KEY + finalI, VALUE + finalI)
                        .publishOn(Schedulers.fromExecutor(redisThreadPool))
                        .flatMap(respSet -> {
                            print(
                                Thread.currentThread().getName() +
                                ": set " +
                                KEY +
                                finalI +
                                " = value" +
                                finalI +
                                ":" +
                                respSet
                            );
                            return reactiveCommands
                                .get(KEY + finalI)
                                .publishOn(
                                    Schedulers.fromExecutor(redisThreadPool)
                                )
                                .defaultIfEmpty("")
                                .flatMap(respGet -> {
                                    if (!respGet.equals(VALUE + finalI)) {
                                        mismatchCounter.incrementAndGet();
                                        print(
                                            Thread.currentThread().getName() +
                                            ": MISMATCH 1!!! get key" +
                                            finalI +
                                            " = " +
                                            respGet
                                        );
                                    }
                                    if (
                                        random.nextInt() % 2 == 0 &&
                                        counter.get() < 300
                                    ) {
                                        counter.incrementAndGet();
                                        throw new OutOfMemoryError(
                                            "test out of memory error"
                                        );
                                    }
                                    int randomKey = random.nextInt(finalI + 1);
                                    return reactiveCommands
                                        .get(KEY + randomKey)
                                        .publishOn(
                                            Schedulers.fromExecutor(
                                                redisThreadPool
                                            )
                                        )
                                        .flatMap(respGetRandom -> {
                                            if (
                                                !respGetRandom.equals(
                                                    VALUE + randomKey
                                                )
                                            ) {
                                                mismatchCounter.incrementAndGet();
                                                print(
                                                    Thread.currentThread()
                                                        .getName() +
                                                    ": MISMATCH 2!!! get key" +
                                                    randomKey +
                                                    " = " +
                                                    respGetRandom
                                                );
                                            }
                                            final int finalRandomKey =
                                                random.nextInt(finalI + 1);
                                            return reactiveCommands
                                                .get(KEY + finalRandomKey)
                                                .publishOn(
                                                    Schedulers.fromExecutor(
                                                        redisThreadPool
                                                    )
                                                )
                                                .map(respFinalRand -> {
                                                    if (
                                                        !respFinalRand.equals(
                                                            VALUE +
                                                            finalRandomKey
                                                        )
                                                    ) {
                                                        mismatchCounter.incrementAndGet();
                                                        print(
                                                            Thread.currentThread()
                                                                .getName() +
                                                            ": MISMATCH 3!!! get key" +
                                                            finalRandomKey +
                                                            " = " +
                                                            respFinalRand
                                                        );
                                                    }
                                                    return respFinalRand;
                                                });
                                        });
                                });
                        })
                        .subscribe()
                );
                TimeUnit.MILLISECONDS.sleep(5);
            }
            System.in.read();
            // Mismatches should not occur
            assertThat(mismatchCounter.get()).isEqualTo(0);
        }
    }

    private void print(String message) {
        System.out.println(message);
    }
}
