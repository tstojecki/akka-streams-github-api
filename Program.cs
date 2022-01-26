using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;

using var sys = ActorSystem.Create("system");
using var mat = sys.Materializer();

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();

var apiToken = Environment.GetEnvironmentVariable("github_api_token");

app.MapPost("/collect", RunJob);

app.UseSwaggerUI();

app.Run();


IResult RunJob(List<string> usernames)
{
    var source = Source.From(usernames)
        .Select(user => $"https://api.github.com/users/{user}/events?per_page=10&page=1");

    var worker = Flow.Create<string>()
        .Via(Flows.Fetcher<Activity>(apiToken))
        .SelectAsync(1, async x =>
        {
            await Task.Delay(1000);
            return x;
        });

    var sink = Sink.ForEach<Page<Activity>>(x => 
    {
        Console.WriteLine(x.Url);
        Console.WriteLine(string.Join(" | ", x.GroupBy(x => x.Type).Select(x => $"{x.Key}: {x.Count()}")));
        Console.WriteLine();
    });

    var maxLevelOfParallelism = 2;
    var levelOfParallelism = usernames.Count() > maxLevelOfParallelism ? maxLevelOfParallelism : usernames.Count();
    source.Via(Flows.Balancer(worker, levelOfParallelism))
        .RunWith(sink, mat);

    return Results.Ok();
}

public static class Flows
{
    public static Flow<string, Page<T>, NotUsed> Fetcher<T>(string? apiToken)
    {
        return Flow.Create<string>()            
            .ConcatMany(firstUrl =>
            {
                var httpClient = new HttpClient();

                if (apiToken != null)
                {
                    httpClient.DefaultRequestHeaders.Add("authorization", $"token {apiToken}");
                }
                httpClient.DefaultRequestHeaders.UserAgent.Add(new System.Net.Http.Headers.ProductInfoHeaderValue(new System.Net.Http.Headers.ProductHeaderValue("github-useractivity-client")));
                return Source.UnfoldAsync(firstUrl, url =>
                {
                    var pageTask = httpClient.GetAsync(url);

                    var next = pageTask.ContinueWith(task =>
                    {
                        var responseMessage = task.Result;

                        var content = responseMessage.Content.ReadFromJsonAsync<IReadOnlyList<T>>().Result ?? new List<T>();
                        var nextLink = responseMessage.GetNextLink();

                        if (nextLink == null) return Option<(string, Page<T>)>.None;
                        else return new Option<(string, Page<T>)>((nextLink, new(content, url)));
                    });

                    return next;
                });
            });
    }

    public static Flow<TIn, TOut, NotUsed> Balancer<TIn, TOut>(Flow<TIn, TOut, NotUsed> worker, int workerCount)
    {

        return Flow.FromGraph(GraphDsl.Create(b =>
        {
            var balancer = b.Add(new Balance<TIn>(workerCount, waitForAllDownstreams: true));
            var merge = b.Add(new Merge<TOut>(workerCount));

            for (var i = 0; i < workerCount; i++)
                b.From(balancer).Via(worker.Async()).To(merge);

            return new FlowShape<TIn, TOut>(balancer.In, merge.Out);
        }));
    }
}

public class Activity
{
    public string? Id { get; set; }
    public string? Type { get; set; }
}

public class Page<T> : List<T>
{
    public Page(IReadOnlyList<T> list, string url)
        : base(list)
    {
        Url = url;
    }

    public string Url { get; }    
}