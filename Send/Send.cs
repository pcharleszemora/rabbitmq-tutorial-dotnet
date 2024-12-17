// See https://aka.ms/new-console-template for more information

using RabbitMQ.Client;
using System.Text;
using System.Threading.Tasks;

const string _hostName = "localhost";
const string _queue = "hello";
const string _routingKey = "hello";

// Create a connection to RabbitMQ server
var factory = new ConnectionFactory { HostName = _hostName };
using var connection = await factory.CreateConnectionAsync();

// 2. Create a Channel within RabbitMQ servrer
using var channel = await connection.CreateChannelAsync();

// 3. Declare Queue Within Channel
await channel.QueueDeclareAsync(
    queue: _queue, 
    durable: false, 
    exclusive: false, 
    autoDelete: false, 
    arguments: null);

const string message = "Hello World!";
var body = Encoding.UTF8.GetBytes(message);

// 4. Publish Message
await channel.BasicPublishAsync(
    exchange: string.Empty, 
    routingKey: _routingKey,
    body: body);

Console.WriteLine($" [x] Sent {message}");
System.Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();