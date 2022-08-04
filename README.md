# .NET 6 Console Application

## Requirements

- NET 6
- Rider / Visual Studio / dotnet-cli
- Kafka Instance
- MongoDB

## To Run

1. Setup your Kafka instance
2. Setup MongoDB (Create SpotifyRecord database and CurrentlyPlayingRecords collection)
3. Setup 2 config.json
4. Build and Run

NOTE: You may need to start playing on spotify in order to get
record back.

## Sample Configs

1. Consumer config

```json
{
  "MongoDB": {
    "connectionString": "mongodb://localhost:27017"
  },
  "Kafka": {
    "BootStrapServers": "PLAINTEXT://172.28.63.150:9092,PLAINTEXT://172.28.63.150:9093,PLAINTEXT://172.28.63.150:9094"
  }
}
```

2. Producer config

```json
{
  "Spotify": {
    "CLIENT_ID": "<Spotify_APP_Client_ID>"
  },
  "Kafka": {
    "BootStrapServers": "PLAINTEXT://172.28.63.150:9092,PLAINTEXT://172.28.63.150:9093,PLAINTEXT://172.28.63.150:9094"
  }
}
```
