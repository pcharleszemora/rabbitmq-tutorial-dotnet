// See https://aka.ms/new-console-template for more information

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

const string _hostName = "localHost";
const string _queue = "hello";

// 1. Create + Open Connection to Channel
var factory = new ConnectionFactory { HostName = _hostName };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// 2. Declare Queue in Consumer
// - We do this b/c we might start the consumer (Receive.cs) _before_ the publisher (Send.cs)
// - We want to make sure the queue exists _before_ we try to consume messages from it
// - You can run the Producer (Send.cs) or Consumer (Receive.cs) in any order b/c both declare the queue.
await channel.QueueDeclareAsync(
    queue: _queue,
    durable: false,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

// 3. Declare Consumer + Provide It With a Callback to Process Messages From Queue
Console.WriteLine(" [*] Waiting for messages.");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) => 
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");
    return Task.CompletedTask;
};

// Note from Module 02: Work Queues
// autoAck: true turns off Manual message acknowledgement (default)
// We typically always want autoAck : false
// The reason why is b/c RabbitMQ will 're-queue' messages that are not processed (i.e. messages that a consumer does not send a delivery message acknowledgement)
// This is helpful in scenarios where there are network failures and/or Consumer failures (e.g. A Worker fails or is taken off line)
// RabbitMQ keeps track of the messages it sends for this purpose.
// We'll implement an actual message acknowledgement in Module 2
await channel.BasicConsumeAsync(
    queue: _queue, 
    autoAck: true, 
    consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
