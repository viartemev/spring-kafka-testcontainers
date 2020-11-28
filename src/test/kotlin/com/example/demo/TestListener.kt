package com.example.demo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.MessageListener
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingDeque


@TestConfiguration
class TestListener {

    @Bean
    fun consumerFactory(@Autowired kafkaProperties: KafkaProperties): ConsumerFactory<Int, String> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "test-hub"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = true
        props[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = "100"
        props[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = "15000"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = IntegerDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun container(@Autowired cf: ConsumerFactory<Int, String>): KafkaMessageListenerContainer<Int, String> {
        val containerProps = ContainerProperties("test-topic-1")
        val messageHolder = ConcurrentHashMap<String, BlockingQueue<ConsumerRecord<*, *>>>()
        containerProps.messageListener = KafkaTestConsumer<Int, String>(messageHolder)
        return KafkaMessageListenerContainer(cf, containerProps)
    }

    internal class KafkaTestConsumer<K, V>(private val messageHolder: ConcurrentHashMap<String, BlockingQueue<ConsumerRecord<*, *>>>) : MessageListener<K, V> {
        private val log = LoggerFactory.getLogger(KafkaTestConsumer::class.java)
        override fun onMessage(data: ConsumerRecord<K, V>) {
            log.info("receive message: {}", data)
            messageHolder.computeIfAbsent(data.topic()) { LinkedBlockingDeque() }
            messageHolder[data.topic()]?.add(data)
        }
    }


}