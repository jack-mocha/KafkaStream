using Confluent.Kafka;
using System;
using System.Threading;

const string Topic = "input-topic";

// Configure the consumer
var config = new ConsumerConfig
{
	BootstrapServers = "localhost:9092",
	GroupId = "your-consumer-group",
	AutoOffsetReset = AutoOffsetReset.Earliest,
	EnableAutoCommit = false // Disable auto commit to have more control over offsets
};

using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
{
	// Subscribe to the topic
	consumer.Subscribe(Topic);

	Console.WriteLine($"Consuming messages from topic: {Topic}");

	// Start consuming messages in a background thread
	var cancellationTokenSource = new CancellationTokenSource();
	var cancellationToken = cancellationTokenSource.Token;

	var consumerThread = new Thread(() =>
	{
		try
		{
			while (true)
			{
				try
				{
					var consumeResult = consumer.Consume(cancellationToken);
					Console.WriteLine($"Received message: {consumeResult.Message.Value} from partition {consumeResult.Partition} offset {consumeResult.Offset}");

					// Process the consumed message here

					// Manually commit the offset to mark the message as consumed
					consumer.Commit(consumeResult);
				}
				catch (ConsumeException ex)
				{
					Console.WriteLine($"Error occurred: {ex.Error.Reason}");
				}
			}
		}
		catch (OperationCanceledException)
		{
			// This exception will be thrown when cancellation is requested.
			Console.WriteLine("Cancellation requested");
		}
		finally
		{
			consumer.Close();
		}
	});

	// Start the consumer thread
	consumerThread.Start();

	// Wait for a key press to exit
	Console.WriteLine("Press any key to exit");
	Console.ReadKey();

	// Request cancellation and wait for the consumer thread to stop
	cancellationTokenSource.Cancel();
	consumerThread.Join();

	Console.WriteLine("End Consumer");
}
