using SpotifyAPI.Web;

namespace Consumer.Contracts;

public interface ICurrentPlayingRepository
{
	public Task InsertDocument(FullTrack track);
	public Task InsertBuffer(List<FullTrack> track);
}