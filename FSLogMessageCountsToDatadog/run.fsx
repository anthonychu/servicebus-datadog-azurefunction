open System
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open System.Text.RegularExpressions
open System.Net.Http

type DatadogMetric = {
    metric: string
    points: int64[][]
    ``type``: string
    tags: string[]
}

type DatadogPayload = {
    series: DatadogMetric[]
}

let Run(input: string, log: TraceWriter) =
    let httpClient = new HttpClient()

    let Env name = 
        System.Environment.GetEnvironmentVariable (name, EnvironmentVariableTarget.Process)

    let url = "https://app.datadoghq.com/api/v1/series?api_key=" + (Env "DatadogApiKey")

    let Escape input = Regex.Replace (input, @"[^A-Za-z0-9]+", "_")

    let namespaceManager = 
        Env "ServiceBusConnectionString"
        |> NamespaceManager.CreateFromConnectionString

    let subscriptions =
        namespaceManager.GetTopics ()
        |> Seq.collect (fun t -> namespaceManager.GetSubscriptions t.Path)
        |> Seq.map (fun s -> ((sprintf "%s.%s" (Escape s.TopicPath) (Escape s.Name)), s.MessageCountDetails))

    let queues =
        namespaceManager.GetQueues ()
        |> Seq.map (fun q -> (Escape q.Path, q.MessageCountDetails))

    let createMetric entityName metricName messageCount = 
        {
            metric = sprintf "ServiceBus.%s.%s" entityName metricName;
            points = [| [| DateTimeOffset.UtcNow.ToUnixTimeSeconds(); messageCount |] |];
            ``type`` = "gauge";
            tags = [| sprintf "environment:%s" (Env "EnvironmentName") |]
        }

    let logMessageCounts (entityName: string) (details: MessageCountDetails) =
        let payload = {
            series = [|
                createMetric entityName "active_message_count" details.ActiveMessageCount;
                createMetric entityName "dead_letter_message_count" details.DeadLetterMessageCount
            |]
        }
        log.Info (sprintf "Posting: %s %i %i" entityName details.ActiveMessageCount details.DeadLetterMessageCount)
        let result = (httpClient.PostAsJsonAsync (url, payload)).Result
        log.Info (result.StatusCode.ToString())

    Seq.concat [ subscriptions; queues ]
        |> Seq.iter (fun s -> 
            logMessageCounts (fst s) (snd s) |> ignore
            )
