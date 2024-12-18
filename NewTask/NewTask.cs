// See https://aka.ms/new-console-template for more information

using RabbitMQ.Client;
using System.Text;
using System.Threading.Tasks;

const string _hostName = "localhost";
const string _queue = "task_queue";
const string _routingKey = "task_queue";

// Create a connection to RabbitMQ server
var factory = new ConnectionFactory { HostName = _hostName };
using var connection = await factory.CreateConnectionAsync();

// 2. Create a Channel within RabbitMQ servrer
using var channel = await connection.CreateChannelAsync();

// 3. Declare Queue Within Channel
// - This is going to be a "Work Queue"
// Work Queue
// = The assumption behind a work queue is that each task is delivered to exactly one worker.
await channel.QueueDeclareAsync(
    queue: _queue, 
    durable: true, 
    exclusive: false, 
    autoDelete: false, 
    arguments: null);

var message = GetMessage(args);
var body = Encoding.UTF8.GetBytes(message);

// Marking messages as persistent doesn't fully guarantee that a message won't be lost. Although it tells RabbitMQ to save the message to disk, there is still a short time window when RabbitMQ has accepted a message and hasn't saved it yet. Also, RabbitMQ doesn't do fsync(2) for every message -- it may be just saved to cache and not really written to the disk. The persistence guarantees aren't strong, but it's more than enough for our simple task queue. If you need a stronger guarantee then you can use publisher confirms: https://www.rabbitmq.com/docs/confirms
var properties = new BasicProperties
{
    Persistent = true
};

// 4. Publish Message
// Module 02: Work Queues
// By default, RabbitMQ will send each message to the next consumer, in sequence. On average every consumer will get the same number of messages. This way of distributing messages is called round-robin. Try this out with three or more workers.
// Pass the properties to the `BasicPublishAsync()`
// Notice the `queue` in `QueueDeclareAsync()` and the routingKey here in `BasicPublishAsync()` have the same value: `task_queue`
await channel.BasicPublishAsync(
    exchange: string.Empty, 
    routingKey: _routingKey,
    mandatory: true,
    basicProperties: properties,
    body: body);

Console.WriteLine($" [x] Sent {message}");
System.Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();

static string GetMessage(string[] args)
{
    return ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
}