#r "Microsoft.ServiceBus"

using System;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Text.RegularExpressions;
using System.Net.Http;
using static System.Environment;

static HttpClient httpClient = new HttpClient();
static string url = "https://app.datadoghq.com/api/v1/series?api_key=" + Env("DatadogApiKey");

public static async Task Run(TimerInfo myTimer, TraceWriter log)
{  
    var namespaceManager = NamespaceManager.CreateFromConnectionString(Env("ServiceBusConnectionString"));
    foreach(var topic in await namespaceManager.GetTopicsAsync())
    {
        foreach(var subscription in await namespaceManager.GetSubscriptionsAsync(topic.Path))
        {
            await LogMessageCountsAsync($"{Escape(topic.Path)}.{Escape(subscription.Name)}", 
                subscription.MessageCountDetails, log);
        }
    }
    foreach(var queue in await namespaceManager.GetQueuesAsync())
    {
        await LogMessageCountsAsync(Escape(queue.Path), queue.MessageCountDetails, log);
    }
}

private static async Task LogMessageCountsAsync(string entityName, MessageCountDetails details, TraceWriter log)
{
    var payload = new 
    {
        series = new []
        {
            CreateMetric(entityName, "active_message_count", details.ActiveMessageCount),
            CreateMetric(entityName, "dead_letter_message_count", details.DeadLetterMessageCount)
        }
    };
    log.Info($"Posting: {entityName} {details.ActiveMessageCount} {details.DeadLetterMessageCount}");
    var result = await httpClient.PostAsJsonAsync(url, payload);
    log.Info(result.StatusCode.ToString());
}

private static object CreateMetric(string entityName, string metricName, long messageCount) => new
    {
        metric = $"servicebus.{entityName}.{metricName}",
        points = new [] { new [] { DateTimeOffset.UtcNow.ToUnixTimeSeconds(), messageCount } },
        type = "gauge",
        tags = new [] { "environment:" + Env("EnvironmentName") }
    };

private static string Escape(string input) => Regex.Replace(input, @"[^A-Za-z0-9]+", "_");

private static string Env(string name) => GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
