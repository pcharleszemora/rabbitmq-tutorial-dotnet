// See https://aka.ms/new-console-template for more information

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

const string _hostName = "localHost";
const string _exchange = "direct_logs";

if (args.Length < 1)
{
    Console.Error.WriteLine($"Usage: {Environment.GetCommandLineArgs()[0]} [info] [warning] [error]");
    Console.WriteLine(" Press [enter] to exit");
    Console.ReadLine();
    Environment.ExitCode = 1;
    return;    
}

// 1. Create + Open Connection to Channel
var factory = new ConnectionFactory { HostName = _hostName };

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// 2. Declare Exchange
await channel.ExchangeDeclareAsync(exchange: _exchange, type: ExchangeType.Direct);

// 3. Declare a Server-Named Queue
var queueDeclareResult = await channel.QueueDeclareAsync();
string queueName = queueDeclareResult.QueueName;

// 4. Create "Binding"
// This will create bindings based on the args provided in the console before running the program
// e.g. 
// dotnet run warning error > logs_from_rabbit.log
// ^ This will only bind 'warning' and 'error' and write them to the log file
// dotnet run info warning error
// ^ This will print all logs to console
foreach (string? severity in args)
{
    await channel.QueueBindAsync(queue: queueName, exchange: _exchange, routingKey: severity);
}

Console.WriteLine(" [*] Waiting for messages.");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) => 
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    var routingKey = ea.RoutingKey;
    Console.WriteLine($" [x] Received '{routingKey}': '{message}'");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(
    queue: queueName, 
    autoAck: true, 
    consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
