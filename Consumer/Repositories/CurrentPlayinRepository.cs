using MongoDB.Driver;
using SpotifyAPI.Web;
using Consumer.Contracts;

namespace Consumer.Repositories;

public class CurrentPlayinRepository : ICurrentPlayingRepository
{
	private MongoClient _client;
	private IMongoDatabase _db;
	private IMongoCollection<FullTrack> _collection;

	public CurrentPlayinRepository(string connectionString)
	{
		try
		{
			_client = new MongoClient(connectionString);
			_db = _client.GetDatabase("SpotifyRecords");
			_collection = _db.GetCollection<FullTrack>("CurrentlyPlayingRecords");
		}
		catch (Exception e)
		{
			Console.WriteLine($"ERROR : {e.Message}");
		}
	}

	public async Task InsertDocument(FullTrack track)
	{
		try
		{
			await _collection.InsertOneAsync(track);
			Console.WriteLine("Successfully Inserted 1 Document");
		}
		catch (Exception e)
		{
			Console.WriteLine($"ERROR: {e.Message}");
		}
	}

	public async Task InsertBuffer(List<FullTrack> track)
	{
		try
		{
			await _collection.InsertManyAsync(track);
			Console.WriteLine($"Sucessfully Inserted {track.Count()} documents");
		}
		catch (Exception e)
		{
			Console.WriteLine($"ERROR: {e.Message}");
		}
	}
}