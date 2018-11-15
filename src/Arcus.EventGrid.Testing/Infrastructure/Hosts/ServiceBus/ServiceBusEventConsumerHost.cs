﻿using System;
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
        private readonly SubscriptionClient _subscriptionClient;
        private readonly ManagementClient _managementClient;

        private static bool isHostShuttingDown;
        public string Id { get; } = Guid.NewGuid().ToString();
        private ServiceBusEventConsumerHost(string topicPath, string subscriptionName, SubscriptionClient subscriptionClient, ManagementClient managementClient, ILogger logger)
            : base(logger)
        {
            Guard.NotNullOrWhitespace(topicPath, nameof(topicPath));
            Guard.NotNullOrWhitespace(subscriptionName, nameof(subscriptionName));
            Guard.NotNull(subscriptionClient, nameof(subscriptionClient));
            Guard.NotNull(managementClient, nameof(managementClient));

            TopicPath = topicPath;
            SubscriptionName = subscriptionName;
            _subscriptionClient = subscriptionClient;
            _managementClient = managementClient;
        }

        /// <summary>
        ///     Path of the topic relative to the namespace base address.
        /// </summary>
        public string TopicPath { get; }

        /// <summary>
        ///     Name of the subscription that was created
        /// </summary>
        public string SubscriptionName { get; }

        /// <summary>
        ///     Start receiving traffic
        /// </summary>
        /// <param name="topicPath">Path of the topic relative to the namespace base address</param>
        /// <param name="serviceBusConnectionString">
        ///     Connection string of the Azure Service Bus namespace to use to consume
        ///     messages
        /// </param>
        /// <param name="logger">Logger to use for writing event information during the hybrid connection</param>
        public static async Task<ServiceBusEventConsumerHost> Start(string topicPath, string serviceBusConnectionString, ILogger logger)
        {
            Guard.NotNullOrWhitespace(topicPath, nameof(topicPath));
            Guard.NotNullOrWhitespace(serviceBusConnectionString, nameof(serviceBusConnectionString));
            Guard.NotNull(logger, nameof(logger));

            logger.LogInformation("Starting Service Bus event consumer host");

            var managementClient = new ManagementClient(serviceBusConnectionString);

            var subscriptionName = $"Test-{Guid.NewGuid().ToString()}";
            await CreateSubscriptionAsync(topicPath, managementClient, subscriptionName).ConfigureAwait(continueOnCapturedContext: false);
            logger.LogInformation("Created subscription '{subscription}' on topic '{topic}'", subscriptionName, topicPath);

            var subscriptionClient = new SubscriptionClient(serviceBusConnectionString, topicPath, subscriptionName);
            StartMessagePump(subscriptionClient, logger);
            logger.LogInformation("Message pump started on '{SubscriptionName}' (topic '{TopicPath}' for endpoint '{ServiceBusEndpoint}')", subscriptionName, topicPath, subscriptionClient.ServiceBusConnection?.Endpoint?.AbsoluteUri);

            return new ServiceBusEventConsumerHost(topicPath, subscriptionName, subscriptionClient, managementClient, logger);
        }

        /// <summary>
        ///     Stop receiving traffic
        /// </summary>
        public override async Task Stop()
        {
            _logger.LogInformation("Stopping host");
            isHostShuttingDown = true;

            await _managementClient.DeleteSubscriptionAsync(TopicPath, SubscriptionName).ConfigureAwait(continueOnCapturedContext: false);
            _logger.LogInformation("Subscription '{SubscriptionName}' deleted on topic '{TopicPath}'", SubscriptionName, TopicPath);

            await _subscriptionClient.CloseAsync().ConfigureAwait(continueOnCapturedContext: false);

            await base.Stop();
        }

        private static void StartMessagePump(SubscriptionClient subscriptionClient, ILogger logger)
        {
            var messageHandlerOptions = new MessageHandlerOptions(async exceptionReceivedEventArgs => await HandleException(exceptionReceivedEventArgs, logger))
            {
                AutoComplete = false,
                MaxConcurrentCalls = 10
            };

            subscriptionClient.RegisterMessageHandler(async (receivedMessage, cancellationToken) => await HandleNewMessage(receivedMessage, subscriptionClient, cancellationToken, logger), messageHandlerOptions);
        }

        private static async Task HandleNewMessage(Message receivedMessage, SubscriptionClient subscriptionClient, CancellationToken cancellationToken, ILogger logger)
        {
            if (receivedMessage == null || isHostShuttingDown)
            {
                return;
            }

            var rawReceivedEvents = Encoding.UTF8.GetString(receivedMessage.Body);

            try
            {
                EventsReceived(rawReceivedEvents);

                await subscriptionClient.CompleteAsync(receivedMessage.SystemProperties.LockToken).ConfigureAwait(continueOnCapturedContext: false);
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

        private static async Task CreateSubscriptionAsync(string topicPath, ManagementClient managementClient, string subscriptionName)
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