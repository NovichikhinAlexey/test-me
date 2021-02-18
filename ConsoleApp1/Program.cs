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
                IsQueueAutoDelete = true,
                MessageTypes = new List<Header.Types.MessageType>()
                {
                    Header.Types.MessageType.CashIn,
                    Header.Types.MessageType.CashOut,
                    Header.Types.MessageType.CashTransfer,
                    Header.Types.MessageType.Order
                }
            };


            var handler = new EventHandler();

            using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());

            Console.WriteLine("MatchingEngineCashInEventReader");
            var reader1 = new MatchingEngineGlobalEventReader(settings1, new[] { handler }, loggerFactory.CreateLogger<MatchingEngineGlobalEventReader>());
            reader1.Start();


            Console.WriteLine("Pres enter to api call...");
            Console.ReadLine();

            ApiCall();

            Console.WriteLine("Pres enter to exit");
            Console.ReadLine();

            reader1.Stop();
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

    public class EventHandler : IMatchingEngineSubscriber<object>
    {
        public Task Process(IList<CustomQueueItem<object>> batch)
        {
            foreach (var item in batch)
            {
                if (item.Value is CashInEvent cashIn)
                {
                    Console.WriteLine("CashInEvent:");
                    Console.WriteLine(JsonConvert.SerializeObject(cashIn, Formatting.Indented));
                }

                if (item.Value is CashOutEvent cashOut)
                {
                    Console.WriteLine("CashOutEvent:");
                    Console.WriteLine(JsonConvert.SerializeObject(cashOut, Formatting.Indented));
                }

                if (item.Value is CashTransferEvent cashTransfer)
                {
                    Console.WriteLine("CashTransferEvent:");
                    Console.WriteLine(JsonConvert.SerializeObject(cashTransfer, Formatting.Indented));
                }

                if (item.Value is ExecutionEvent execution)
                {
                    Console.WriteLine("ExecutionEvent:");
                    Console.WriteLine(JsonConvert.SerializeObject(execution, Formatting.Indented));
                }
            }
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
