// See https://aka.ms/new-console-template for more information

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

const string _hostName = "localHost";
const string _exchange = "logs";

// 1. Create + Open Connection to Channel
var factory = new ConnectionFactory { HostName = _hostName };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// 2. Declare Exchange
// Important Detail 2: The messages will be lost if no queue is bound to the exchange yet, but that's okay for us; if no consumer is listening yet we can safely discard the message.
// - We bind the server-named queue to the "logs" exchange below
await channel.ExchangeDeclareAsync(exchange: _exchange, type: ExchangeType.Fanout);

// 3. Declare a Server-Named Queue
// In the .NET client, when we supply no parameters to QueueDeclareAsync() we create a non-durable, exclusive, autodelete queue with a generated name (e.g. amq.gen-JzTY20BRgKO-HjmUJj0wLg)
QueueDeclareOk queueDeclareResult = await channel.QueueDeclareAsync();
string queueName = queueDeclareResult.QueueName;

// 4. Create "Binding"
// Binding is the relationship between an exchange and queue
// e.g.
// Exchange => logs
// is bound to
// Queue => amq.gen-JzTY20BRgKO-HjmUJj0wLg
// The logs exchange will append messages to the amq.gen-JzTY20BRgKO-HjmUJj0wLg queue
// You can view Bindings in the RabbitMQ Mgmt UI or Console.
// This is similar to viewing the Exchanges
// RabbitMQ Mgmt UI
// http://localhost:15672 -> Exchanges -> Bindings
// RabbitMQ Console
// 1. docker exec -it rabbitmq bash (Assuming running RabbitMQ Server in a Docker Container)
// 2. rabbitmqctl list_bindings
await channel.QueueBindAsync(queue: queueName, exchange: _exchange, routingKey: string.Empty);


Console.WriteLine(" [*] Waiting for logs.");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) => 
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] {message}");
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
    queue: queueName, 
    autoAck: true, 
    consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
