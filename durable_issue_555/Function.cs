using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace durable_issue_555
{
    public static class Function
    {
        /// <summary>
        /// Client
        /// </summary>
        /// <param name="req"></param>
        /// <param name="starter"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("Run")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get","post")]HttpRequestMessage req,
            [OrchestrationClient] DurableOrchestrationClient starter, TraceWriter log)
        {
            log.Info($"Run - Start.");

            // Function input comes from the request content.
            var instanceId = await starter.StartNewAsync("Orc_Messaging", null);

            log.Info($"Run - End. (InstanceId: = '{instanceId}')");
            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        /// <summary>
        /// Orchestrator
        /// </summary>
        /// <param name="context"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("Orc_Messaging")]
        public static async Task<List<string>> OrcMessaging(
            [OrchestrationTrigger] DurableOrchestrationContext context, TraceWriter log)
        {
            if (!context.IsReplaying) log.Info($"Orc_Messaging - Start. {{InstanceId={context.InstanceId}}}");

            var outputs = new List<string>();

            // for test
            var TEST_COUNT = 2;
            for (var i = 0; i < TEST_COUNT; i++)
            {
                var messageStatus = new MessageStatus();
                messageStatus.MessageId = i.ToString();

                var sendMessageTasks = new List<Task<string>>
                {
                    context.CallSubOrchestratorAsync<string>("SubOrc_SendLineMessage", (context.InstanceId, messageStatus)),
                    context.CallSubOrchestratorAsync<string>("SubOrc_SendFacebookMessage", (context.InstanceId, messageStatus))
                };

                // wait tasks
                await Task.WhenAll(sendMessageTasks);
            }

            log.Info($"Orc_Messaging - End. {{InstanceId={context.InstanceId}}}");
            return outputs;
        }

        /// <summary>
        /// Sub Orchestration
        /// </summary>
        /// <param name="context"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("SubOrc_SendLineMessage")]
        public static async Task<string> SubOrcSendLineMessage(
            [OrchestrationTrigger] DurableOrchestrationContext context, TraceWriter log)
        {
            // parameter:parentInstanceId, MessageStatusÅj
            (string parentInstanceId, MessageStatus messageStatus) = context.GetInput<(string, MessageStatus)>();

            if (!context.IsReplaying) log.Info($"SubOrc_SendLineMessage - Start. {{InstanceId={context.InstanceId}, MessageId={messageStatus.MessageId}}}");
            log.Info($"SubOrc_SendLineMessage - End. {{InstanceId={context.InstanceId}}}");
            return "";
        }

        /// <summary>
        /// Sub Orchestration
        /// </summary>
        /// <param name="context"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("SubOrc_SendFacebookMessage")]
        public static async Task<string> SubOrcSendFacebookMessage(
            [OrchestrationTrigger] DurableOrchestrationContext context, TraceWriter log)
        {
            // parameter:parentInstanceId, MessageStatusÅj
            (string parentInstanceId, MessageStatus messageStatus) = context.GetInput<(string, MessageStatus)>();

            if (!context.IsReplaying) log.Info($"SubOrcSendFacebookMessage - Start. {{InstanceId={context.InstanceId}, MessageId={messageStatus.MessageId}}}");
            log.Info($"SubOrcSendFacebookMessage - End. {{InstanceId={context.InstanceId}}}");
            return "";
        }

        /// <summary>
        /// GetAllStatus
        /// </summary>
        /// <param name="req"></param>
        /// <param name="client"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("GetAllStatus")]
        public static async Task<string> GetAllStatus(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient] DurableOrchestrationClient starter, TraceWriter log)
        {
            log.Info($"GetAllStatus - Start.");

            var sb = new StringBuilder();
            var instances = await starter.GetStatusAsync();
            foreach (var instance in instances)
            {
                if (instance.RuntimeStatus != OrchestrationRuntimeStatus.Completed)
                {
                    sb.Append(JsonConvert.SerializeObject(instance));
                    sb.Append("\r\n");
                    log.Info(JsonConvert.SerializeObject(instance));
                }
            };

            log.Info($"GetAllStatus - End.");
            return sb.ToString();
        }

        /// <summary>
        /// TerminateInstance
        /// </summary>
        /// <param name="starter"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("TerminateInstance")]
        public static Task TerminateInstance(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient] DurableOrchestrationClient starter, TraceWriter log)
        {
            log.Info($"TerminateInstance - Start.");

            var queryStrings = req.GetQueryNameValuePairs();
            if (queryStrings == null) {
                return null;
            }

            var instanceId = queryStrings.FirstOrDefault(kv => string.Compare(kv.Key, "instanceId", true) == 0);
            if (string.IsNullOrEmpty(instanceId.Value))
            {
                return null;
            }

            var reason = "It was time To be done.";

            log.Info($"TerminateInstance - End.");
            return starter.TerminateAsync(instanceId.Value, reason);
        }


        /// <summary>
        /// MessageStatus Entity
        /// </summary>
        public class MessageStatus
        {
            public MessageStatus()
            {
            }
            public string MessageId { get; set; }
            public string InstanceId { get; set; }
            public string Status { get; set; }
            public string Message { get; set; }
        }
    }
}
