using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;

public class QueueDrainer
{
    private const int PrefectchCount = 1000;
    private readonly ILogger<QueueDrainer> _logger;
    private ServiceBusAdministrationClient _adminClient = null!;
    private ServiceBusClient _client = null!;
    private bool _deadLetter;
    private TimeSpan _maxWaitTime = TimeSpan.FromSeconds(2);
    private string _queueName = null!;
    private bool? _sessionSupported;

    public QueueDrainer(ILogger<QueueDrainer> logger)
    {
        _logger = logger;
    }

    public async Task DrainAsync(string connectionString, string queueName, bool deadLetter = false)
    {
        _queueName = queueName;
        _deadLetter = deadLetter;
        _client = CreateClient(connectionString);
        _adminClient = new ServiceBusAdministrationClient(connectionString);
        var count = await GetCount();
        while (count > 0)
        {
            try
            {
                await using var session = await CreateSession(CancellationToken.None);
                await ReadAllFromSession(session, CancellationToken.None);
                count = await GetCount();
            }
            catch (ServiceBusException e)
            {
                if (e.Reason == ServiceBusFailureReason.ServiceTimeout)
                {
                    _logger.LogInformation("Timeout creating session, queue is probably empty.");
                    break;
                }
                throw;
            }
        }
    }

    private ServiceBusClient CreateClient(string connectionString)
    {
        var clientOptions = new ServiceBusClientOptions
        {
            Identifier = "DrainQ",
            TransportType = ServiceBusTransportType.AmqpWebSockets,
            RetryOptions = new ServiceBusRetryOptions
            {
                Delay = TimeSpan.FromMilliseconds(100),
                MaxDelay = TimeSpan.FromSeconds(5),
                MaxRetries = 1,
                TryTimeout = TimeSpan.FromSeconds(30),
            },
        };
        return new ServiceBusClient(connectionString, clientOptions);
    }

    private async Task<ServiceBusReceiver> CreateSession(CancellationToken cancellationToken)
    {
        var rxOptions = new ServiceBusSessionReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = PrefectchCount,
        };

        if (!_deadLetter)
        {
            try
            {
                if (_sessionSupported.GetValueOrDefault(true))
                {
                    _logger.LogInformation("Getting Session");
                    var result1 = await _client.AcceptNextSessionAsync(_queueName, rxOptions, cancellationToken);
                    _logger.LogInformation("Session {SessionId} accepted", result1.SessionId);
                    _sessionSupported = true;
                    return result1;
                }
            }
            catch (InvalidOperationException)
            {
                if (_sessionSupported.HasValue)
                    throw;
            }
        }
        var options = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = PrefectchCount,
        };
        if (_deadLetter)
        {
            options.SubQueue = SubQueue.DeadLetter;
        }
        var result = _client.CreateReceiver(_queueName, options);
        _logger.LogInformation("Got sessionless queue");
        _sessionSupported = false;
        return result;
    }

    private async Task<long> GetCount()
    {
        var props = (await _adminClient.GetQueueRuntimePropertiesAsync(_queueName)).Value;
        var count = _deadLetter ? props.DeadLetterMessageCount : props.ActiveMessageCount;
        _logger.LogInformation("Queue has {Count} messages", count);
        return count;
    }

    private async Task<bool> ReadAllFromSession(ServiceBusReceiver receiver, CancellationToken cancellationToken)
    {
        bool readAny = false;
        while (true)
        {
            var messages = await receiver.ReceiveMessagesAsync(PrefectchCount, _maxWaitTime, cancellationToken: cancellationToken);
            if (messages.Count == 0)
            {
                break;
            }
            readAny = true;
            _logger.LogInformation("Received {Count} messages", messages.Count);

            var tasks = new List<Task>();
            foreach (var message in messages)
            {
                tasks.Add(receiver.CompleteMessageAsync(message, cancellationToken));
            }
            await Task.WhenAll(tasks);
            _logger.LogInformation("Completed {Count} messages", tasks.Count);
        }
        return readAny;
    }
}
