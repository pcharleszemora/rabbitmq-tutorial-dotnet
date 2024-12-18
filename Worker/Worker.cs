// See https://aka.ms/new-console-template for more information

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

const string _hostName = "localHost";
const string _queue = "task_queue";

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
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

// 2a. Fair Dispatch
// This `BasicQosAsync()` call implements 'Fair Dispatch': `BasicQos method with the prefetchCount = 1 setting`
// The impl tells RabbitMQ not to give more than one message to a worker at a time. 
// Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one. Instead, it will dispatch it to the next worker that is not still busy.
// This helps the scenario with two workers when all odd messages are heavy and even messages are light, one worker will be constantly busy and the other one will do hardly any work.
// RabbitMQ doesn't know anything about how much work a given worker is doing.
// Important:
// If all the workers are busy, your queue can fill up. You will want to keep an eye on that, and maybe add more workers, or have some other strategy.
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

// 3. Declare Consumer + Provide It With a Callback to Process Messages From Queue
Console.WriteLine(" [*] Waiting for messages.");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (model, ea) => 
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    Console.WriteLine($" [x] Received {message}");

    // Simulate Execution Time
    // Each dot represents 1 second
    int dots = message.Split('.').Length - 1;
    await Task.Delay(dots * 1000);
    Console.WriteLine($" [x] Done");

    // Here, `channel` could also be accessed as `((AsyncEventBasicConsumer)sender)`
    // Acknowledgement must be sent on the same channel that received the delivery
    // Attempts to acknowledge using a different channel will result in a channel-level protocol exception. See the doc guide on confirmations to learn more: https://www.rabbitmq.com/docs/confirms
    // Important:
    // It is a common mistask to miss implementing the acknowledgement: `channel.BasicAckAsync()`
    // However, consequences are serious
    // The reason why is b/c Messages will be redelivered when your client quits (which may look like random redelivery), but RabbitMQ will eat more and more memory as it won't be able to release any unacked messages.
    // This is how you debug the problem - use `rabbitmqctl` to print the `messages_unacknowledged` field.
    // Mac:
    // sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged
    // Windows (no sudo)
    // rabbitmqctl.bat list_queues name messages_ready messages_unacknowledged
    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
};
// Note from Module 02: Work Queues
// `autoAck: true` turns off Manual message acknowledgement (default)
// We typically always want `autoAck: false`
// The reason why is b/c RabbitMQ will 're-queue' messages that are not processed (i.e. messages that a consumer does not send a delivery message acknowledgement)
// This is helpful in scenarios where there are network failures and/or Consumer failures (e.g. A Worker fails or is taken off line)
// RabbitMQ keeps track of the messages it sends for this purpose.
// You can even make the queues and messages durable if the RabbitMQ server fails
// You do this above in the QueueDeclareAsync() method with arg: `durable: true`
await channel.BasicConsumeAsync(
    queue: _queue, 
    autoAck: false, 
    consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
