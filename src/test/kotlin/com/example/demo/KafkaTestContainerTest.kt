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
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Import
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
@ComponentScan(basePackageClasses = [Listener::class])
@Import(KafkaTestContainerTest.KafkaTestContainersConfiguration::class, TestListenerContainer::class)
@ContextConfiguration(initializers = [KafkaInitializer::class], classes = [KafkaAutoConfiguration::class, KafkaConfig::class])
class KafkaTestContainerTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    private lateinit var container: KafkaMessageListenerContainer<Int, String>

    @Autowired
    private lateinit var listener: Listener
        
    @Test
    fun sendAndReceiveTest() {
        ContainerTestUtils.waitForAssignment(container, 1)

        Assertions.assertThat(kafkaTemplate).isNotNull
        kafkaTemplate.send("test-topic-1", "boom")
                .addCallback(object : ListenableFutureCallback<Any> {
                    override fun onSuccess(result: Any?) {
                        println("Success: $result")
                    }

                    override fun onFailure(ex: Throwable) {
                        println("Failure: $ex")
                    }
                })
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until { listener.invocations.size > 0 }
        Assertions.assertThat(listener.invocations).hasSize(1)
                .contains("boom")
    }

    @TestConfiguration
    internal class KafkaTestContainersConfiguration {
        @Bean
        fun newTopic(): NewTopic = NewTopic("test-topic-1", 1, 1)
    }

}