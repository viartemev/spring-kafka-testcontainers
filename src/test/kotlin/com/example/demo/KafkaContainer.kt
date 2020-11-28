package com.example.demo

import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName


internal class KafkaInitializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
    override fun initialize(configurableApplicationContext: ConfigurableApplicationContext) {
        val container = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
        container.start()
        println("Container started...")
        val bootstrapServers: String = container.bootstrapServers.replace("PLAINTEXT://", "")
        println("bootstrap servers: $bootstrapServers ...")
        val values = TestPropertyValues.of(
                "spring.kafka.bootstrap-servers=${bootstrapServers}",
        )
        values.applyTo(configurableApplicationContext)
    }
}
