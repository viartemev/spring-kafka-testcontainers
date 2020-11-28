package com.example.demo

import org.apache.kafka.clients.admin.NewTopic
import org.assertj.core.api.Assertions
import org.awaitility.Awaitility
import org.junit.Test
import org.junit.jupiter.api.BeforeAll
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.util.concurrent.ListenableFutureCallback
import java.util.concurrent.TimeUnit


@RunWith(SpringRunner::class)
@SpringBootTest
@DirtiesContext
@Import(KafkaTestContainerTest.KafkaTestContainersConfiguration::class, TestListener::class)
@ContextConfiguration(initializers = [KafkaInitializer::class], classes = [KafkaAutoConfiguration::class, KafkaConfig::class])
class KafkaTestContainerTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    private lateinit var kafkaAdmin: KafkaAdmin

    @Autowired
    private lateinit var kafkaConfig: KafkaConfig

    @Autowired
    private lateinit var container: KafkaMessageListenerContainer<Int, String>

    @BeforeAll
    fun before() {
        ContainerTestUtils.waitForAssignment(container, 1)
    }

    @Test
    fun sendAndReceiveTest() {
        Assertions.assertThat(kafkaTemplate).isNotNull
        kafkaTemplate.send("test-topic-1", "flight of a dragon")
                .addCallback(object : ListenableFutureCallback<Any> {
                    override fun onSuccess(result: Any?) {
                        println("Success: $result")
                    }

                    override fun onFailure(ex: Throwable) {
                        println("Failure: $ex")
                    }
                })
        // Wait
        Awaitility.await()
                .atMost(30, TimeUnit.MINUTES)
                .until { kafkaConfig.invocations.size > 0 }
        // Assert
        Assertions.assertThat(kafkaConfig.invocations).hasSize(1)
                .contains("flight of a dragon")
    }

    @TestConfiguration
    internal class KafkaTestContainersConfiguration {
        @Bean
        fun newTopic(): NewTopic = NewTopic("test-topic-1", 1, 1)
    }

}