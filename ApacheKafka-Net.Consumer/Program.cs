using Confluent.Kafka;
using System.Diagnostics;
using System.Text.Json;

namespace ApacheKafka_Net.Consumer
{
    public class Program
    {

        protected static void Main(string[] args)
        {
            string topic = "bola";
            string groupId = "bola_group";
            string bootstrapServers = "127.0.0.1:9092";

            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            try
            {
                using var consumerBuilder = new ConsumerBuilder<Ignore, string>(config).Build();
                {
                    consumerBuilder.Subscribe(topic);

                    var cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (_, e) => {
                        e.Cancel = true;
                        cts.Cancel();
                    };


                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var cr = consumerBuilder.Consume(cts.Token);
                                Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                                
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Error occured: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException ee)
            {
                
            }
        }
    }
}