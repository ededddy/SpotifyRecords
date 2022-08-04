using Swan;
using Confluent.Kafka;
using Newtonsoft.Json;
using SpotifyAPI.Web;
using SpotifyAPI.Web.Auth;
using static SpotifyAPI.Web.Scopes;
using Microsoft.Extensions.Configuration;

namespace Producer;

public class Program
{
	private static readonly EmbedIOAuthServer _server =
		new EmbedIOAuthServer(new Uri("http://localhost:5000/callback"), 5000);

	private const string _credientialPath = "creds.json";

	// Get config.json content
	private static IConfiguration _config = new ConfigurationBuilder()
		.SetBasePath(Directory.GetCurrentDirectory())
		.AddJsonFile("config.json", optional: false).Build();

	private static void Exiting() => Console.CursorVisible = true;

	private static string _clientID
	{
		get => _config["Spotify:CLIENT_ID"];
	}


	public static async Task<int> Main()
	{
		if (File.Exists(_credientialPath))
			await Start();
		else
			await StartAuthentication();

		Console.ReadKey();
		return 0;
	}

	private static async Task<SpotifyClient> InitSpotifClient()
	{
		var json = await File.ReadAllTextAsync(_credientialPath);
		var token = JsonConvert.DeserializeObject<PKCETokenResponse>(json);

		var authenticator = new PKCEAuthenticator(_clientID, token!);
		authenticator.TokenRefreshed += (_, nToken) =>
			File.WriteAllText(_credientialPath, JsonConvert.SerializeObject(nToken));

		var config = SpotifyClientConfig.CreateDefault().WithAuthenticator(authenticator);

		var spotify = new SpotifyClient(config);
		return spotify;
	}

	private static ProducerBuilder<TKey, TValue> CreateProducer<TKey, TValue>()
	{
		ProducerConfig cfg = new ProducerConfig
		{
			BootstrapServers = _config["Kafka:BootStrapServers"],
			Partitioner = Partitioner.Murmur2Random,
			Acks = Acks.Leader,
			LingerMs = 300,
		};

		var builder = new ProducerBuilder<TKey, TValue>(cfg);
		return builder;
	}

	private static async Task Start()
	{
		AppDomain.CurrentDomain.ProcessExit += (_, _) => Exiting();
		var spotify = await InitSpotifClient();

		var user = await spotify.UserProfile.Current();
		Console.WriteLine($"Auditing user : {user.DisplayName} (id: {user.Id})");

		var builder = CreateProducer<string, string>();
		// additional setup for serialziation
		builder.SetKeySerializer(Serializers.Utf8);
		builder.SetValueSerializer(Serializers.Utf8);
		using (var producer = builder.Build())
		{
			var playingRequest = new PlayerCurrentlyPlayingRequest();
			while (true)
			{
				try
				{
					var currentlyPlaying = await spotify.Player.GetCurrentlyPlaying(playingRequest);
					FullTrack? item = currentlyPlaying.Item as FullTrack;
					Console.WriteLine("Currently playing received");
					try
					{
						DeliveryResult<string, string> produceResult = await producer.ProduceAsync("spotify-records",
							new Message<string, string>()
							{
								Key = item!.Id,
								Value = item.ToJson()
							});
						Console.WriteLine("Message Delivered to \n" +
						                  $"\t{produceResult.Topic} at {produceResult.Partition}");
						Console.WriteLine($"\tOffSet : {produceResult.Offset}");
						Console.WriteLine($"Value : {produceResult.Message.Value}");
					}
					catch (ProduceException<string, string> e)
					{
						Console.WriteLine($"Error : {e.Message}");
					}

					var sleepTime = item!.DurationMs - currentlyPlaying.ProgressMs;
					Thread.Sleep((int)sleepTime!);
				}
				catch
				{
					Console.WriteLine(
						"Cannot retrieve Currently Playing. Perhaps it has been a while since user played a track.");
					return;
				}
			}
		}
	}

	private static async Task StartAuthentication()
	{
		var (verifier, challenge) = PKCEUtil.GenerateCodes();

		await _server.Start();
		_server.AuthorizationCodeReceived += async (_, response) =>
		{
			PKCETokenResponse token = await new OAuthClient().RequestToken(
				new PKCETokenRequest(_clientID, response.Code, _server.BaseUri, verifier)
			);

			await File.WriteAllTextAsync(_credientialPath, JsonConvert.SerializeObject(token));

			await _server.Stop();
			await Start();
		};

		var request = new LoginRequest(_server.BaseUri, _clientID, LoginRequest.ResponseType.Code)
		{
			CodeChallenge = challenge,
			CodeChallengeMethod = "S256",
			Scope = new List<string>
			{
				UserReadEmail,
				UserReadPrivate,
				UserReadCurrentlyPlaying,
			}
		};

		Uri uri = request.ToUri();
		try
		{
			BrowserUtil.Open(uri);
		}
		catch (Exception)
		{
			Console.WriteLine($"Unable to open URL, open it yourself : {uri}");
		}
	}
}