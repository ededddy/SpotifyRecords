using System.Text.Json;
using Confluent.Kafka;
using Consumer.Contracts;
using Consumer.Repositories;
using Microsoft.Extensions.Configuration;
using SpotifyAPI.Web;

namespace Consumer;

public class Program
{
	private static IConfiguration _config = new ConfigurationBuilder()
		.SetBasePath(Directory.GetCurrentDirectory())
		.AddJsonFile("config.json", optional: false).Build();

	public static async Task<int> Main()
	{
		ICurrentPlayingRepository currentPlayingRepository =
			new CurrentPlayinRepository(_config["MongoDB:connectionString"]);

		ConsumerConfig config = new ConsumerConfig
		{
			BootstrapServers = _config["Kafka:BootStrapServers"],
			GroupId = "1",
			AutoOffsetReset = AutoOffsetReset.Latest,
			EnableAutoCommit = false,
		};

		ConsumerBuilder<string, string> builder = new ConsumerBuilder<string, string>(config);
		builder.SetKeyDeserializer(Deserializers.Utf8);
		builder.SetValueDeserializer(Deserializers.Utf8);
		IEnumerable<string> topics = new List<string>() { "spotify-records" };
		CancellationTokenSource cts = new CancellationTokenSource();
		Console.CancelKeyPress += (_, e) =>
		{
			e.Cancel = true;
			cts.Cancel();
		};
		using (var consumer = builder
			       // boilerplate code from sample for manuall commit
			       // https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Consumer/Program.cs
			       .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
			       .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
			       .SetPartitionsAssignedHandler((c, partitions) =>
			       {
				       // Since a cooperative assignor (CooperativeSticky) has been configured, the
				       // partition assignment is incremental (adds partitions to any existing assignment).
				       Console.WriteLine(
					       "Partitions incrementally assigned: [" +
					       string.Join(',', partitions.Select(p => p.Partition.Value)) +
					       "], all: [" +
					       string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
					       "]");

				       // Possibly manually specify start offsets by returning a list of topic/partition/offsets
				       // to assign to, e.g.:
				       // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
			       })
			       .SetPartitionsRevokedHandler((c, partitions) =>
			       {
				       // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
				       // assignment is incremental (may remove only some partitions of the current assignment).
				       var remaining = c.Assignment.Where(atp =>
					       partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
				       Console.WriteLine(
					       "Partitions incrementally revoked: [" +
					       string.Join(',', partitions.Select(p => p.Partition.Value)) +
					       "], remaining: [" +
					       string.Join(',', remaining.Select(p => p.Partition.Value)) +
					       "]");
			       })
			       .SetPartitionsLostHandler((_, partitions) =>
			       {
				       // The lost partitions handler is called when the consumer detects that it has lost ownership
				       // of its assignment (fallen out of the group).
				       Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
			       })
			       .Build())
		{
			consumer.Subscribe(topics);
			try
			{
				Console.WriteLine("Polling...");
				while (true)
				{
					try
					{
						var consumeResult = consumer.Consume(cts.Token);
						FullTrack? track = JsonSerializer.Deserialize<FullTrack>(consumeResult.Message.Value);
						Console.WriteLine("Message Received");
						if (track == null)
							continue;
						await currentPlayingRepository.InsertDocument(track);
						try
						{
							consumer.Commit(consumeResult);
						}
						catch (KafkaException e)
						{
							Console.WriteLine($"Commit Error : {e.Error.Reason}");
						}
					}
					catch (ConsumeException e)
					{
						Console.WriteLine($"ERROR: {e.Error.Reason}");
					}
				}
			}
			catch (OperationCanceledException)
			{
				consumer.Close();
			}
		}


		return 0;
	}
}