using Confluent.Kafka;

var config = new ProducerConfig
{
	BootstrapServers = "localhost:9092",
	ClientId = "producer-1"
};

using (var producer = new ProducerBuilder<string, string>(config).Build())
{
	var message = new Message<string, string>
	{
		Key = null, // Set the key if you want to partition the messages
		Value = "Hello, Kafka!"
	};

	try
	{
		var deliveryResult = await producer.ProduceAsync("input-topic", message);
		Console.WriteLine($"Message delivered to {deliveryResult.TopicPartitionOffset}");
	}
	catch (ProduceException<string, string> ex)
	{
		Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
	}
}