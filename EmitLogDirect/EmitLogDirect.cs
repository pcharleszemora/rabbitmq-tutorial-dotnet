using RabbitMQ.Client;
using System.Text;

const string _hostName = "localhost";
const string _exchange = "direct_logs";

// 1. Create + Establish Connection
var factory = new ConnectionFactory { HostName = _hostName };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// 2. Declare Exchange
// This will emit logs to a 'Direct Exchange'
// 'Direct' Exchange
// = a message goes to the queues whose binding key exactly matches the routingKey or the message
// It is legal to bind multiple queues to the same binding key
// This is what we'll do in this example
// We'll be able to run:
// a. one instance of ReceiveLogsDirect to print the logs to console
// b. one instance of ReceiveLogsDirect to write the logs to file
// Sidebar:
// Bindings can take an extra routingKey parameter in the QueueBindAsync(routingKey: <value_for_binding_key>) 
// To avoid the confusion with a BasicPublishAsync() parameter we're going to call it a binding key
// i.e. 
// BasicPublishAsync(routingKey: ...) = we'll say takes a => routingKey
// QueueBindAsync(routingKey: ...) = we'll say takes a => binding key
await channel.ExchangeDeclareAsync(exchange: _exchange, type: ExchangeType.Direct);

// This will assume severity will be one of `info`, `warning`, or `error`
var severity = (args.Length > 0) ? args[0] : "info";
var message = GetMessage(args);
var body = Encoding.UTF8.GetBytes(message);

// 3. Publish Message to Exchange
await channel.BasicPublishAsync(
    exchange: _exchange, 
    routingKey: severity,
    body: body);

Console.WriteLine($" [x] Sent {message}");

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();

static string GetMessage(string[] args)
{
    return (args.Length > 1) ? string.Join(" ", args.Skip(1).ToArray()) : "Hello World!";
}