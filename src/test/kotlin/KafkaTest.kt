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
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

@Testcontainers
class KafkaTest {
    private val producerProps = mapOf<String, String>(
        "bootstrap.servers" to kafka.bootstrapServers,
        "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
        "security.protocol" to "PLAINTEXT"
    )
    private val consumerProps =
        mapOf(
            "bootstrap.servers" to kafka.bootstrapServers,
            "auto.offset.reset" to "earliest",
            "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "group.id" to "someGroup",
            "security.protocol" to "PLAINTEXT"
        )

    @Test
    fun consumerProducerTest() = runBlocking {
        val consumer = KafkaConsumer<String, ByteArray>(consumerProps)
        val consumerJob = launch {
            consumer.use { consumer ->
                consumer.subscribe(listOf(TOPIC_NAME))
                val message = consumer.receive()
                assertEquals(expected = MESSAGE, actual = message)
            }
        }

        val producer = KafkaProducer<String, ByteArray>(producerProps)
        producer.use { producer ->
            producer.sendAsync(ProducerRecord(TOPIC_NAME, MESSAGE.encodeToByteArray()))
        }

        consumerJob.join()
    }

    private companion object {
        const val TOPIC_NAME = "my-topic"
        const val MESSAGE = "MyMessage"
        const val IMAGE_NAME = "confluentinc/cp-kafka:6.2.1"
        val pollTimeout = 400.milliseconds.toJavaDuration()

        @JvmStatic
        @Container
        val kafka = KafkaContainer(DockerImageName.parse(IMAGE_NAME))

        suspend fun KafkaProducer<String, ByteArray>.sendAsync(record: ProducerRecord<String, ByteArray>) {
            suspendCoroutine { continuation ->
                send(record) { metadata, exception ->
                    if (exception != null) {
                        continuation.resumeWithException(exception)
                    } else {
                        continuation.resume(metadata)
                    }
                }
            }
        }

        tailrec fun KafkaConsumer<String, ByteArray>.receive(): String {
            val message = poll(pollTimeout).map { record -> String(record.value()) }.firstOrNull()
            return message ?: receive()
        }
    }
}