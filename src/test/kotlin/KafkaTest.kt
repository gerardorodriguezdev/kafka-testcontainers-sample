import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

@Testcontainers
class KafkaTest {

    @Test
    fun test() {
        val producerProps = mapOf<String, String>(
            "bootstrap.servers" to kafka.bootstrapServers,
            "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
            "value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
            "security.protocol" to "PLAINTEXT"
        )
        val consumerProps =
            mapOf(
                "bootstrap.servers" to kafka.bootstrapServers,
                "auto.offset.reset" to "earliest",
                "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                "group.id" to "someGroup",
                "security.protocol" to "PLAINTEXT"
            )

        val producer = KafkaProducer<String, ByteArray>(producerProps)
        runBlocking {
            val job = launch {
                KafkaConsumer<String, ByteArray>(consumerProps).use { consumer ->
                    consumer.subscribe(listOf("topic"))
                    val message = repeatUntilSome {
                        consumer.poll(400.milliseconds.toJavaDuration())
                            .map { record -> String(record.value()) }
                            .firstOrNull()
                    }
                    println(">>>" + message)
                }
            }

            producer.use { producer ->
                producer.sendAsync(ProducerRecord("topic", "Hi".encodeToByteArray()))
            }

            job.join()
        }
    }

    private suspend fun KafkaProducer<String, ByteArray>.sendAsync(record: ProducerRecord<String, ByteArray>) {
        suspendCoroutine { continuation ->
            send(record) { metadata, exception ->
                exception?.let { continuation.resumeWithException(it) } ?: continuation.resume(metadata)
            }
        }
    }

    tailrec fun <T> repeatUntilSome(block: () -> T?): T = block() ?: repeatUntilSome(block)

    companion object {
        @JvmStatic
        @Container
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
    }
}