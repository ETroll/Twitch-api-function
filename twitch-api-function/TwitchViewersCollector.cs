using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace twitch_api_function
{
    public static class TwitchViewersCollector
    {
        [FunctionName("TwitchViewersCollector")]
        public static async Task Run(
            [TimerTrigger("0 0 */1 * * *", RunOnStartup = true)]TimerInfo myTimer, 
            [Table("ScitechViewers")] IAsyncCollector<ViewersTableEntity> viewersTable, 
            ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            string clientId = Environment.GetEnvironmentVariable("TWITCH_CLIENT_ID");
            string gameName = Environment.GetEnvironmentVariable("TWITCH_GAME_NAME");

            if(!string.IsNullOrWhiteSpace(clientId) && !string.IsNullOrWhiteSpace(gameName))
            {
                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Add("Client-ID", clientId);
                    client.BaseAddress = new Uri("https://api.twitch.tv");
                    HttpResponseMessage gamesHttpResult = await client.GetAsync($"helix/games?name={Uri.EscapeDataString(gameName)}");

                    if(!gamesHttpResult.IsSuccessStatusCode)
                    {
                        log.LogError($@"Twitch API call (Get Games) was not 
                            successfull. HTTP CODE: {gamesHttpResult.StatusCode}");
                        return;
                    }
                    try
                    {
                        string gameContent = await gamesHttpResult.Content.ReadAsStringAsync();
                        TwitchResult<TwitchGame> gameResult = JsonConvert.DeserializeObject<TwitchResult<TwitchGame>>(gameContent);

                        if(gameResult.Data.Count > 0)
                        {
                            string gameId = gameResult.Data[0].Id;

                            int sum = 0;
                            int breaker = 0;
                            string cursor = string.Empty;

                            while(breaker < 20) //There should not be more than 20 pages.. I think..
                            {
                                string afterString = string.Empty;
                                if(!string.IsNullOrWhiteSpace(cursor))
                                {
                                    afterString = $"&after={cursor}";
                                }

                                HttpResponseMessage streamsResult = await client.GetAsync($"helix/streams?first=100&game_id={gameId}{afterString}");
                                if (!streamsResult.IsSuccessStatusCode)
                                {
                                    log.LogError($"Twitch API call (Get Streams) was not successfull. HTTP CODE: {streamsResult.StatusCode}");
                                    return;
                                }

                                string streamResultContent = await streamsResult.Content.ReadAsStringAsync();
                                TwitchResult<TwitchStream> streamResult = JsonConvert.DeserializeObject<TwitchResult<TwitchStream>>(streamResultContent);
                                if (streamResult?.Data.Count > 0)
                                {
                                    foreach(var stream in streamResult.Data)
                                    {
                                        sum += stream.Viewer_count;
                                    }
                                }

                                if(streamResult?.Pagination.Cursor == null)
                                {
                                    break;
                                }
                                else
                                {
                                    cursor = streamResult.Pagination.Cursor;
                                }
                                breaker++;
                                
                            }

                            DateTime timestamp = DateTime.UtcNow;
                            await viewersTable.AddAsync(new ViewersTableEntity
                            {
                                PartitionKey = timestamp.ToString("yyyy-MM-dd"),
                                RowKey = timestamp.Hour.ToString(),
                                Viewers = sum
                            });
                        }
                        else
                        {
                            log.LogWarning($"No gamed with the name {gameName} found");
                        }
                    }
                    catch(Exception ex)
                    {
                        log.LogError(ex, "Could not parse results from Twitch");
                    }
                }
            }
            else
            {
                log.LogError("ClientId or Game Name was not found. Exiting.");
            }




        }
    }

    public class ViewersTableEntity : TableEntity
    {
        public int Viewers { get; set; }
    }

    public class TwitchResult<T>
    {
        public List<T> Data { get; set; }
        public TwitchPagination Pagination { get; set; }
    }

    public class TwitchPagination
    {
        public string Cursor { get; set; }
    }

    public class TwitchGame
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Box_art_url { get; set; }
    }
    public class TwitchStream
    {
        public string Id { get; set; }
        public string User_id { get; set; }
        public string User_name { get; set; }
        public string Game_id { get; set; }
        public string Type { get; set; }
        public string Title { get; set; }
        public int Viewer_count { get; set; }
        public string Started_at { get; set; }
        public string Language { get; set; }
        public string Thumbnail_url { get; set; }
        public List<string> Tag_ids { get; set; }
    }
}
