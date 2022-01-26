using System.Text.RegularExpressions;

public static class GithubExtensions
{
    // octokit.net sdk doesn't support pageable results
    // https://github.com/octokit/octokit.net/issues/1955#issuecomment-867303619

    static readonly Regex _linkRelRegex = new("rel=\"(next|prev|first|last)\"", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    static readonly Regex _linkUriRegex = new("<(.+)>", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    public static string? GetNextLink(this HttpResponseMessage responseMessage)
    {
        var links = responseMessage.GetLinks();
        if (links.ContainsKey("next"))
        {
            return links["next"].ToString();
        }

        return null;
    }

    public static IDictionary<string, Uri> GetLinks(this HttpResponseMessage responseMessage)
    {
        Dictionary<string, Uri> results = new();

        var headers = responseMessage.Headers;        
        if (headers.Contains("Link"))
        {
            var links = headers.GetValues("Link").First().Split(',', StringSplitOptions.TrimEntries);

            foreach (var link in links)
            {
                var relMatch = _linkRelRegex.Match(link);
                if (!relMatch.Success || relMatch.Groups.Count != 2) break;

                var uriMatch = _linkUriRegex.Match(link);
                if (!uriMatch.Success || uriMatch.Groups.Count != 2) break;

                results.Add(relMatch.Groups[1].Value, new Uri(uriMatch.Groups[1].Value));
            }
        }

        return results;
    }
}