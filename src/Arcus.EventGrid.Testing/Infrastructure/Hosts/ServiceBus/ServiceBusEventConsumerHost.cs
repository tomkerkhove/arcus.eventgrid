using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GuardNet;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;

namespace Arcus.EventGrid.Testing.Infrastructure.Hosts.ServiceBus
{
    /// <summary>
    ///     Event consumer host for receiving Azure Event Grid events via Azure Logic Apps & Service Bus Topics
    /// </summary>
    public class ServiceBusEventConsumerHost : EventConsumerHost
    {
        private readonly ManagementClient _managementClient;
        private readonly bool _deleteSubscriptionOnStop;
        private readonly string _connectionString;
        private SubscriptionClient _subscriptionClient;
        public string Id { get; } = Guid.NewGuid().ToString();

        /// <summary>
        ///     Constructor
        /// </summary>
        /// <param name="consumerHostOptions">
        ///     Configuration options that indicate what Service Bus entities to use and how they should behave
        /// </param>
        /// <param name="logger">Logger to use for writing event information during the hybrid connection</param>
        public ServiceBusEventConsumerHost(ServiceBusEventConsumerHostOptions consumerHostOptions, ILogger logger)
            : base(logger)
        {
            Guard.NotNull(consumerHostOptions, nameof(consumerHostOptions));
            Guard.NotNull(logger, nameof(logger));

            TopicPath = consumerHostOptions.TopicPath;

            _connectionString = consumerHostOptions.ConnectionString;
            _managementClient = new ManagementClient(consumerHostOptions.ConnectionString);
            _deleteSubscriptionOnStop = consumerHostOptions.DeleteSubscriptionOnStop;
        }

        /// <summary>
        ///     Path of the topic relative to the namespace base address.
        /// </summary>
        public string TopicPath { get; }

        /// <summary>
        ///     Name of the subscription that was created
        /// </summary>
        public string SubscriptionName { get; private set; }

        /// <summary>
        ///     Endpoint of the Service Bus Namespace
        /// </summary>
        public string ServiceBusEndpoint { get; private set; }

        /// <summary>
        ///     Start receiving traffic
        /// </summary>
        public async Task Start()
        {
            if (string.IsNullOrWhiteSpace(SubscriptionName) == false)
            {
                throw new InvalidOperationException("Host is already started");
            }

            _logger.LogInformation("Starting Service Bus event consumer host");

            SubscriptionName = $"Test-{Guid.NewGuid().ToString()}";
            await CreateSubscriptionAsync(TopicPath, _managementClient, SubscriptionName).ConfigureAwait(continueOnCapturedContext: false);
            _logger.LogInformation("Created subscription '{subscription}' on topic '{topic}'", SubscriptionName, TopicPath);

            _subscriptionClient = new SubscriptionClient(_connectionString, TopicPath, SubscriptionName);
            ServiceBusEndpoint = _subscriptionClient.ServiceBusConnection?.Endpoint?.AbsoluteUri;
            StartMessagePump();
            _logger.LogInformation("Message pump started on '{SubscriptionName}' (topic '{TopicPath}' for endpoint '{ServiceBusEndpoint}')", SubscriptionName, TopicPath, ServiceBusEndpoint);
        }

        /// <summary>
        ///     Stop receiving traffic
        /// </summary>
        public override async Task Stop()
        {
            _logger.LogInformation("Stopping host");

            if (_deleteSubscriptionOnStop)
            {
                await _managementClient.DeleteSubscriptionAsync(TopicPath, SubscriptionName).ConfigureAwait(continueOnCapturedContext: false);
                _logger.LogInformation("Subscription '{SubscriptionName}' deleted on topic '{TopicPath}'", SubscriptionName, TopicPath);
            }

            await _subscriptionClient.CloseAsync().ConfigureAwait(continueOnCapturedContext: false);

            await base.Stop();
        }

        private void StartMessagePump()
        {
            var messageHandlerOptions = new MessageHandlerOptions(async exceptionReceivedEventArgs => await HandleException(exceptionReceivedEventArgs, _logger))
            {
                AutoComplete = false,
                MaxConcurrentCalls = 10
            };

            _subscriptionClient.RegisterMessageHandler(async (receivedMessage, cancellationToken) => await HandleNewMessage(receivedMessage, _subscriptionClient, cancellationToken, _logger), messageHandlerOptions);
        }

        private static async Task HandleNewMessage(Message receivedMessage, SubscriptionClient subscriptionClient, CancellationToken cancellationToken, ILogger logger)
        {
            if (receivedMessage == null)
            {
                return;
            }

            logger.LogInformation("Message '{messageId}' was received", receivedMessage.MessageId);

            string rawReceivedEvents = string.Empty;
            try
            {
                rawReceivedEvents = Encoding.UTF8.GetString(receivedMessage.Body);
                EventsReceived(rawReceivedEvents);

                await subscriptionClient.CompleteAsync(receivedMessage.SystemProperties.LockToken).ConfigureAwait(continueOnCapturedContext: false);

                logger.LogInformation("Message '{messageId}' was successfully handled", receivedMessage.MessageId);
            }
            catch (Exception ex)
            {
                logger.LogError("Failed to persist raw events with exception '{exceptionMessage}'. Payload: {rawEventsPayload}", ex.Message, rawReceivedEvents);
            }
        }

        private static Task HandleException(ExceptionReceivedEventArgs exceptionReceivedEventArgs, ILogger logger)
        {
            logger.LogCritical(exceptionReceivedEventArgs.Exception.Message);
            return Task.CompletedTask;
        }

        private async Task CreateSubscriptionAsync(string topicPath, ManagementClient managementClient, string subscriptionName)
        {
            var subscriptionDescription = new SubscriptionDescription(topicPath, subscriptionName)
            {
                AutoDeleteOnIdle = TimeSpan.FromHours(1),
                MaxDeliveryCount = 3,
                UserMetadata = "Subscription created by Arcus in order to run integration tests"
            };

            var ruleDescription = new RuleDescription("Accept All", new TrueFilter());

            await managementClient.CreateSubscriptionAsync(subscriptionDescription, ruleDescription).ConfigureAwait(continueOnCapturedContext: false);
        }
    }
}