using RabbitMQ.Client;
using System.Text;

const string _hostName = "localhost";
const string _exchange = "logs";

// 1. Create = Establish Connection
var factory = new ConnectionFactory { HostName = _hostName };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// 2. Declare Exchange
// Exchange
// = on one side, an exchange receives messages from producers; on the other side, the exchange pushes those messages to queues
// The exchange must know exactly what to do with a messages it receives
// e.g. append the message to a particular queue? Append the message to many queues? Discard the message?
// In previous tutorials we covered these concepts:
// a. producer => is a user application that sends messages.
// b. queue => is a buffer that stores messages.
// c. consumer => is a user application that receives messages.
// - An exchange sits between producer and queue(s)
// - "The core idea in the messaging model in RabbitMQ is that the producer never sends any messages directly to a queue. Actually, quite often the producer doesn't even know if a message will be delivered to any queue at all."
// `ExchangeType.Fanout`
// = broadcasts all the messages it receives to all the queues it knows.
// When `channel.ExchangeDeclareAsync(exchnage: string.Empty, ...)`, the 'default' or nameless exchange is used to declare it.
// You can view the default exchange in the RabbitMQ mgmt UI on the Exchanges page: "(AMQP default)"
await channel.ExchangeDeclareAsync(exchange: _exchange, type: ExchangeType.Fanout);

var message = GetMessage(args);
var body = Encoding.UTF8.GetBytes(message);

// 3. Publish Message to Exchange
// When await channel.BasicPublishAsync(exchange: <name_of_exchange>, routingKey: <name_of_queue>, ...), messages are 'published to the name_of_exchange, and routed to the name_of_queue if it exists.
// Important Detail1 : We need to supply a routingKey when sending, but its value is ignored for fanout exchanges.
// - We declare a `fanout` exchange above: await channel.ExchangeDeclareAsync(exchange: _exchange, type: ExchangeType.Fanout);
// Important Detail 2: The messages will be lost if no queue is bound to the exchange yet, but that's okay for us; if no consumer is listening yet we can safely discard the message.
// - We bind the server-named queue to the "logs" exchange in `ReceiveLogs.cs`
await channel.BasicPublishAsync(
    exchange: _exchange, 
    routingKey: string.Empty,
    body: body);

Console.WriteLine($" [x] Sent {message}");

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();

static string GetMessage(string[] args)
{
    return ((args.Length > 0) ? string.Join(" ", args) : "info: Hello World!");
}