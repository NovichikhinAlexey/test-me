using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ME.Contracts.Api.IncomingMessages;
using ME.Contracts.OutgoingMessages;
using Microsoft.Extensions.Logging;
using MyJetWallet.MatchingEngine.EventReader;
using MyJetWallet.MatchingEngine.EventReader.BaseReader;
using MyJetWallet.MatchingEngine.Grpc;
using Newtonsoft.Json;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            var settings1 = new MatchingEngineEventReaderSettings("amqp://rabbit:Dhgr49TdY6befc4l@192.168.10.80:5672", "my-queue-name-1")
            {
                TopicName = "spot.me.events",
                IsQueueAutoDelete = true
            };

            var settings2 = new MatchingEngineEventReaderSettings("amqp://rabbit:Dhgr49TdY6befc4l@192.168.10.80:5672", "my-queue-name-2")
            {
                TopicName = "spot.me.events",
                IsQueueAutoDelete = true
            };

            var settings3 = new MatchingEngineEventReaderSettings("amqp://rabbit:Dhgr49TdY6befc4l@192.168.10.80:5672", "my-queue-name-4")
            {
                TopicName = "spot.me.events",
                IsQueueAutoDelete = true
            };

            var handler = new CashInEventHandler();

            using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());

            Console.WriteLine("MatchingEngineCashInEventReader");
            var reader1 = new MatchingEngineCashInEventReader(settings1, new[] { handler }, loggerFactory.CreateLogger<MatchingEngineCashInEventReader>());
            reader1.Start();

            Console.WriteLine("MatchingEngineCashOutEventReader");
            var reader2 = new MatchingEngineCashOutEventReader(settings2, new[] { new CashOutEventEventHandler() }, loggerFactory.CreateLogger<MatchingEngineCashOutEventReader>());
            reader2.Start();

            Console.WriteLine("MatchingEngineExecutionEventReader");
            var reader3 = new MatchingEngineExecutionEventReader(settings3, new[] { new ExecutionEventHandler() }, loggerFactory.CreateLogger<MatchingEngineExecutionEventReader>());
            reader3.Start();


            Console.WriteLine("Pres enter to api call...");
            Console.ReadLine();

            ApiCall();

            Console.WriteLine("Pres enter to exit");
            Console.ReadLine();

            reader1.Stop();
            reader2.Stop();
            reader3.Stop();
        }

        private static void ApiCall()
        {
            var factory = new MatchingEngineClientFactory(
                "http://192.168.10.80:5001",
                "http://192.168.10.80:5002",
                "http://192.168.10.80:5003");

            var cashClient = factory.GetCashService();

            var cashInResp = cashClient.CashInOut(new CashInOutOperation()
            {
                Id = Guid.NewGuid().ToString("N"),
                MessageId = Guid.NewGuid().ToString("N"),

                AccountId = "manual-test-003",
                WalletId = "manual-test-w-003",
                BrokerId = "jetwallet",

                AssetId = "BTC",
                Volume = "0.001",
                Description = "test deposit"

            });

            Console.WriteLine("CASH IN");
            Console.WriteLine(JsonConvert.SerializeObject(cashInResp, Formatting.Indented));

            var tradingClient = factory.GetTradingService();

            var limitOrder = new LimitOrder()
            {
                Id = Guid.NewGuid().ToString("N"),
                MessageId = Guid.NewGuid().ToString("N"),

                AccountId = "manual-test-003",
                WalletId = "manual-test-w-003",
                BrokerId = "jetwallet",

                AssetPairId = "BTCUSD",
                Price = "80000",
                Volume = "-0.001",
                Type = LimitOrder.Types.LimitOrderType.Limit,
                WalletVersion = -1
            };
            var limitOrderResp = tradingClient.LimitOrder(limitOrder);

            Console.WriteLine();
            Console.WriteLine("LIMIT ORDER");
            Console.WriteLine(JsonConvert.SerializeObject(limitOrderResp, Formatting.Indented));

            var cancelResp = tradingClient.CancelLimitOrder(new LimitOrderCancel()
            {
                AccountId = "manual-test-003",
                WalletId = "manual-test-w-003",
                BrokerId = "jetwallet",

                Id = Guid.NewGuid().ToString("N"),
                MessageId = Guid.NewGuid().ToString("N"),

                LimitOrderId = {limitOrder.Id}
            });

            Console.WriteLine();
            Console.WriteLine("CANCEL ORDER");
            Console.WriteLine(JsonConvert.SerializeObject(cancelResp, Formatting.Indented));
        }
    }

    public class CashInEventHandler : IMatchingEngineSubscriber<CashInEvent>
    {
        public Task Process(IList<CustomQueueItem<CashInEvent>> batch)
        {
            Console.WriteLine("CashInEvent:");
            Console.WriteLine(JsonConvert.SerializeObject(batch, Formatting.Indented));

            return Task.CompletedTask;
        }
    }

    public class CashOutEventEventHandler : IMatchingEngineSubscriber<CashOutEvent>
    {
        public Task Process(IList<CustomQueueItem<CashOutEvent>> batch)
        {
            Console.WriteLine("CashOutEvent:");
            Console.WriteLine(JsonConvert.SerializeObject(batch, Formatting.Indented));

            return Task.CompletedTask;
        }
    }

    public class ExecutionEventHandler : IMatchingEngineSubscriber<ExecutionEvent>
    {
        public Task Process(IList<CustomQueueItem<ExecutionEvent>> batch)
        {
            Console.WriteLine("ExecutionEvent:");
            Console.WriteLine(JsonConvert.SerializeObject(batch, Formatting.Indented));

            return Task.CompletedTask;
        }
    }

}
