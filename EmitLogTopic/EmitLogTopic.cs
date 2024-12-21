using RabbitMQ.Client;
using System.Text;

const string _hostName = "localhost";
const string _exchange = "topic_logs";

// 1. Create + Establish Connection
var factory = new ConnectionFactory { HostName = _hostName };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// 2. Declare Exchange
// This is going to be a "Topic Exchange" Type
// Topic Exchange (From RabbitMQ Tutorial. This is a complex concept)
// Reference: https://www.rabbitmq.com/tutorials/tutorial-five-dotnet#topic-exchange
// Messages sent to a topic exchange can't have an arbitrary routing_key - it must be a list of words, delimited by dots. The words can be anything, but usually they specify some features connected to the message. A few valid routing key examples: stock.usd.nyse, nyse.vmw, quick.orange.rabbit. There can be as many words in the routing key as you like, up to the limit of 255 bytes.
// The binding key must also be in the same form. The logic behind the topic exchange is similar to a direct one - a message sent with a particular routing key will be delivered to all the queues that are bound with a matching binding key. However there are two important special cases for binding keys:
//    * (star) can substitute for exactly one word.
//    # (hash) can substitute for zero or more words.
// Visual Example of Topic Exchange: https://www.rabbitmq.com/tutorials/tutorial-five-dotnet#topic-exchange
// Summary:
// Topic exchange is powerful and can behave like other exchanges.
// When a queue is bound with # (hash) binding key - it will receive all the messages, regardless of the routing key - like in fanout exchange.
// When special characters * (star) and # (hash) aren't used in bindings, the topic exchange will behave just like a direct one.
// The code in our example is going to start off with a working assumption that the routing keys of logs will have two words: <facility>.<severity>
await channel.ExchangeDeclareAsync(exchange: _exchange, type: ExchangeType.Topic);

var routingKey = (args.Length > 0) ? args[0] : "anonymous.info";
var message = GetMessage(args);
var body = Encoding.UTF8.GetBytes(message);

// 3. Publish Message to Exchange
await channel.BasicPublishAsync(
    exchange: _exchange, 
    routingKey: routingKey,
    body: body);

Console.WriteLine($" [x] Sent {message}");

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();

static string GetMessage(string[] args)
{
    return (args.Length > 1) ? string.Join(" ", args.Skip(1).ToArray()) : "Hello World!";
}