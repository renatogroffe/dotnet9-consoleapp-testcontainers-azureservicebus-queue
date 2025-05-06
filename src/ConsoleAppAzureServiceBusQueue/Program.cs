using Azure.Messaging.ServiceBus;
using Bogus.DataSets;
using ConsoleAppAzureServiceBusQueue.Utils;
using Serilog;
using System.Text;
using Testcontainers.ServiceBus;

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("testcontainers-azureservicebus-queue.tmp")
    .CreateLogger();
logger.Information("***** Iniciando testes com Testcontainers + Azure Service Bus Queue *****");

CommandLineHelper.Execute("docker container ls",
    "Containers antes da execucao do Testcontainers...");

var serviceBusContainer = new ServiceBusBuilder()
  .WithImage("mcr.microsoft.com/azure-messaging/servicebus-emulator:1.1.2")
  .WithAcceptLicenseAgreement(true)
  .Build();
await serviceBusContainer.StartAsync();

CommandLineHelper.Execute("docker container ls",
    "Containers apos execucao do Testcontainers...");

var connectionAzureServiceBus = serviceBusContainer.GetConnectionString();
const string queue = "queue.1";
logger.Information($"Connection String = {connectionAzureServiceBus}");
logger.Information($"Queue a ser utilizada nos testes = {queue}");

var client = new ServiceBusClient(connectionAzureServiceBus);
var sender = client.CreateSender(queue);
const int maxMessages = 10;
var lorem = new Lorem("pt_BR");
for (int i = 1; i <= maxMessages; i++)
{
    var sentence = lorem.Sentence();
    logger.Information($"Enviando mensagem {i}/{maxMessages}: {sentence}");
    await sender.SendMessageAsync(new ServiceBusMessage(sentence));
}
logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();


var receiverOptions = new ServiceBusReceiverOptions
{
    ReceiveMode = ServiceBusReceiveMode.PeekLock,
};
var receiver = client.CreateReceiver(queue, receiverOptions);
int k = 0;
while (true)
{
    k++;
    var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
    if (message != null)
    {
        logger.Information($"Mensagem recebida {k}/{maxMessages}: {Encoding.UTF8.GetString(message.Body)}");
        await receiver.CompleteMessageAsync(message);
        logger.Information("Pressione ENTER para continuar...");
        Console.ReadLine();
    }
    else
    {
        logger.Information("Nao foram recebidas novas mensagens.");
        break;
    }
}

logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();

Console.WriteLine("Testes concluidos com sucesso!");