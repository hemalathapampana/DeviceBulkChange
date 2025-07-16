using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Repositories.Device;
using Altaworx.AWS.Core.Repositories.JasperDevice;
using Altaworx.AWS.Core.Repositories.JasperRatePlan;
using Altaworx.AWS.Core.Services;
using AltaworxDeviceBulkChange.Constants;
using Altaworx.ThingSpace.Core;
using Altaworx.ThingSpace.Core.Models;
using Altaworx.ThingSpace.Core.Models.Api;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Logger;
using Amop.Core.Models;
using Amop.Core.Models.DeviceBulkChange;
using Amop.Core.Models.eBonding;
using Amop.Core.Models.Jasper;
using Amop.Core.Models.Revio;
using Amop.Core.Models.Telegence;
using Amop.Core.Models.Telegence.Api;
using Amop.Core.Repositories;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Revio;
using Amop.Core.Repositories.Telegence;
using Amop.Core.Resilience;
using Amop.Core.Services.Base64Service;
using Amop.Core.Services.eBonding;
using Amop.Core.Services.Http;
using Amop.Core.Services.Jasper;
using Amop.Core.Services.Revio;
using Amop.Core.Services.Telegence;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using Polly;
using PayloadModel = Altaworx.AWS.Core.Models.PayloadModel;
using TelegenceSubscriberUpdateRequest = Altaworx.AWS.Core.TelegenceSubscriberUpdateRequest;
using ThingSpaceServiceAmop = Altaworx.ThingSpace.Core;
using Amop.Core.Models.ThingSpace;
using AltaworxDeviceBulkChange.Repositories;
using Amop.Core.Constants;
using Altaworx.AWS.Core.Helpers.Constants;
using Amop.Core.Enumerations;
using Altaworx.AWS.Core.Repositories.Teal;
using Amop.Core.Services.Teal;
using Amop.Core.Models.Teal;
using Amop.Core.Helpers.Teal;
using AltaworxDeviceBulkChange.Models;
using Amop.Core.Repositories.Pond;
using Amop.Core.Services.Pond;
using Amop.Core.Helpers.Pond;
using Amop.Core.Models.Pond;
using AltaworxDeviceBulkChange.Services;
using Amazon.SimpleEmail.Model;
using System.Threading.Channels;
using Amop.Core.Services.Internal.Webhook;
using Amop.Core.Models.Internal.Webhook;
//Update 
//using AltaworxDeviceBulkChange.Repositories;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxDeviceBulkChange
{
    public partial class Function : AwsFunctionBase
    {
        /// <summary>
        /// Corresponds to the lookup value in the DeviceStatus table
        /// </summary>
        private const int THINGSPACE_DEVICESTATUS_PENDINGACTIVATION = 15;
        private const int THINGSPACE_DEVICESTATUSID_ACTIVE = 8;

        private const int MAX_DEVICE_CHANGES = Int32.MaxValue;
        private const int PageSize = 100;
        private const int PortalTypeM2M = 0;
        private const int PortalTypeMobility = 2;
        private const int PortalTypeLNP = 1;
        private const int RemainingTimeCutoff = 60;
        private const int SQL_TRANSIENT_RETRY_MAX_COUNT = 3;
        private const int HTTP_RETRY_MAX_COUNT = 3;
        private const int NEW_SERVICE_ACTIVATION_MAX_COUNT = 6;
        private const int MAX_TELEGENCE_SERVICES_PER_REQUEST = 200;
        private readonly string MAX_PARALLEL_REQUEST = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.MAX_PARALLEL_REQUESTS) ?? "10";

        private const int SQS_SHORT_DELAY_SECONDS = 300;
        private const int MAXIMUM_NUMBER_OF_RETRIES = 3;
        private const string TELEGENCE_NEWACTIVATION_STAGING_TABLE = "TelegenceNewServiceActivation_Staging";
        private const string CustomerRatePlanDeviceQueueTable = "Device_CustomerRatePlanOrRatePool_Queue";

        private string DeviceBulkChangeQueueUrl = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.DEVICE_BULK_CHANGE_QUEUE_URL);
        private string eBondingDeviceStatusChangeQueueUrl = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.EBONDING_DEVICE_STATUS_CHANGE_QUEUE_URL);
        private string DeviceStatusUpdatePath = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.JASPER_DEVICE_RATE_PLAN_UPDATE_PATH);
        private string TelegenceDeviceStatusUpdateURL = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.TELEGENCE_DEVICE_STATUS_UPDATE_URL);
        private string TelegenceSubscriberUpdateURL = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.TELEGENCE_SUBSCRIBER_UPDATE_URL);
        private string TelegenceIPAddressesURL = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.TELEGENCE_IP_ADDRESSES_URL);
        private string TelegenceIPProvisioningURL = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.TELEGENCE_IP_PROVISIONING_URL);
        private string TelegenceIPProvisionRequestStatusURL = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.TELEGENCE_IP_PROVISION_REQUEST_STATUS_URL);
        private static string ThingSpaceGetStatusRequestURL = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.THINGSPACE_GET_STATUS_REQUEST_URL);
        public virtual string ProxyUrl { get; set; } = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.PROXY_URL);
        private string TelegenceActivationErrorStatusList = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.TELEGENCE_ACTIVATION_ERROR_STATUS_LIST);
        private string ThingSpaceUpdateDeviceStatusRetryNumber = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.THINGSPACE_UPDATE_DEVICE_STATUS_RETRY_NUMBER);
        private string JasperDeviceUsernameUpdatePath = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.JASPER_DEVICE_USERNAME_UPDATE_PATH);
        private string JasperDeviceAuditTrailPath = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.JASPER_DEVICE_AUDIT_TRAIL_PATH);
        private string TealAssignRatePlanPath = Environment.GetEnvironmentVariable(TealHelper.CommonString.TEAL_ASSIGN_RATE_PLAN_URL);
        private string ThingSpaceChangeIdentifierPath = Environment.GetEnvironmentVariable(EnvironmentVariableKeyConstants.THINGSPACE_CHANGE_IDENTIFIER_URL);
        private HttpRequestFactory _httpRequestFactory = new HttpRequestFactory();
        private TelegenceAPIClient _telegenceApiGetClient;
        private TelegenceAPIClient _telegenceApiPatchClient;
        private TelegenceAPIClient _telegenceApiPostClient;
        private BulkChangeRepository bulkChangeRepository;
        private SqsValues sqsValues;
        private ThingSpaceRepository thingSpaceRepository;
        private DeviceRepository deviceRepository;
        private AdmWebhookService admWebhookService;



        /// <summary>
        /// Updates status of one or more devices
        /// </summary>
        /// <param name="sqsEvent"></param>
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            this.bulkChangeRepository = new BulkChangeRepository();
            long bulkChangeId = 0;
            try
            {
                keysysContext = BaseFunctionHandler(context);
                if (string.IsNullOrEmpty(TelegenceSubscriberUpdateURL))
                {
                    DeviceBulkChangeQueueUrl = context.ClientContext.Environment[EnvironmentVariableKeyConstants.DEVICE_BULK_CHANGE_QUEUE_URL];
                    eBondingDeviceStatusChangeQueueUrl = context.ClientContext.Environment[EnvironmentVariableKeyConstants.EBONDING_DEVICE_STATUS_CHANGE_QUEUE_URL];
                    DeviceStatusUpdatePath = context.ClientContext.Environment[EnvironmentVariableKeyConstants.JASPER_DEVICE_RATE_PLAN_UPDATE_PATH];
                    TelegenceDeviceStatusUpdateURL =
                        context.ClientContext.Environment[EnvironmentVariableKeyConstants.TELEGENCE_DEVICE_STATUS_UPDATE_URL];
                    TelegenceSubscriberUpdateURL = context.ClientContext.Environment[EnvironmentVariableKeyConstants.TELEGENCE_SUBSCRIBER_UPDATE_URL];
                    ProxyUrl = context.ClientContext.Environment[EnvironmentVariableKeyConstants.PROXY_URL];
                    TelegenceActivationErrorStatusList =
                        context.ClientContext.Environment[EnvironmentVariableKeyConstants.TELEGENCE_ACTIVATION_ERROR_STATUS_LIST];
                    ThingSpaceGetStatusRequestURL =
                        context.ClientContext.Environment[EnvironmentVariableKeyConstants.THINGSPACE_GET_STATUS_REQUEST_URL];
                    ThingSpaceUpdateDeviceStatusRetryNumber =
                        context.ClientContext.Environment[EnvironmentVariableKeyConstants.THINGSPACE_UPDATE_DEVICE_STATUS_RETRY_NUMBER];
                    JasperDeviceUsernameUpdatePath = context.ClientContext.Environment[EnvironmentVariableKeyConstants.JASPER_DEVICE_USERNAME_UPDATE_PATH];
                    JasperDeviceAuditTrailPath = context.ClientContext.Environment[EnvironmentVariableKeyConstants.JASPER_DEVICE_AUDIT_TRAIL_PATH];
                    TealAssignRatePlanPath = context.ClientContext.Environment[TealHelper.CommonString.TEAL_ASSIGN_RATE_PLAN_URL];
                    ThingSpaceChangeIdentifierPath = context.ClientContext.Environment[EnvironmentVariableKeyConstants.THINGSPACE_CHANGE_IDENTIFIER_URL];
                }

                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

                thingSpaceRepository = new ThingSpaceRepository(keysysContext.logger, keysysContext.CentralDbConnectionString);
                deviceRepository = new DeviceRepository(keysysContext.logger, new EnvironmentRepository(), keysysContext.Context);
                bulkChangeId = await ProcessEventAsync(keysysContext, sqsEvent);
            }
            catch (Exception ex)
            {
                LogInfo(keysysContext, LogTypeConstant.Exception, $"{ex.Message}\n{ex.GetBaseException().StackTrace}");
            }

            await NotifyStatusUpdate(bulkChangeId, keysysContext);
            CleanUp(keysysContext);
        }

        private async Task NotifyStatusUpdate(long bulkChangeId, KeySysLambdaContext keysysContext)
        {
            try
            {
                if (bulkChangeId == 0)
                {
                    return;
                }
                var bulkChange = GetBulkChange(keysysContext, bulkChangeId);
                AwsFunctionBase.LogInfo(keysysContext, LogTypeConstant.Sub, "NotifyStatusUpdate");
                AwsFunctionBase.LogInfo(keysysContext, LogTypeConstant.Info, $"BulkChangeId = {bulkChange.Id}, ChangeRequestTypeId = {bulkChange.ChangeRequestTypeId}, Status = {bulkChange.Status}");
                if (bulkChange != null && bulkChange?.ChangeRequestTypeId == (int)DeviceChangeType.StatusUpdate)
                {
                    // TODO: Due to memory issue, we need to get changes in batches, currently we are getting PageSize=100 changes at a time
                    // We need apply batch size to get changes from Receiver end to query changes from DB instead of getting all changes at once here
                    var changes = GetDeviceChanges(keysysContext, bulkChange.Id, bulkChange.PortalTypeId, PageSize, false);
                    var request = changes.Select(c => BulkChangeDetailRecord.MapToBulkChangeDetailRequest(c)).ToList();
                    if (admWebhookService == null)
                    {
                        admWebhookService = new AdmWebhookService();
                    }
                    await admWebhookService.NotifyStatusUpdateDone(request, ParameterizedLog(keysysContext));
                }
            }
            catch (Exception e)
            {
                AwsFunctionBase.LogInfo(keysysContext, LogTypeConstant.Exception, $"Exception when notifying status update done: {e.Message} - {e.StackTrace}");
            }
        }

        private async Task<long> ProcessEventAsync(KeySysLambdaContext context, SQSEvent sqsEvent)
        {
            LogInfo(context, LogTypeConstant.Sub, "ProcessEventAsync");
            if (sqsEvent.Records.Count > 0)
            {
                if (sqsEvent.Records.Count == 1)
                {
                    return await ProcessEventRecordAsync(context, sqsEvent.Records[0]);
                }
                else
                {
                    LogInfo(context, LogTypeConstant.Exception, $"Expected a single message, received {sqsEvent.Records.Count}");
                }
            }
            return 0;
        }

        private async Task<long> ProcessEventRecordAsync(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            LogInfo(context, LogTypeConstant.Sub, "Start ProcessEventRecordAsync");

            sqsValues = new SqsValues(context, message);
            var bulkChangeId = sqsValues.BulkChangeId;

            if (bulkChangeId == 0)
            {
                LogInfo(context, LogTypeConstant.Exception, "No valid bulk change id provided in message");
                return bulkChangeId;
            }
            var bulkChange = GetBulkChange(context, bulkChangeId);
            long additionBulkChangeId = 0;
            if (message.MessageAttributes.ContainsKey("AdditionBulkChangeId") && !string.IsNullOrWhiteSpace(message.MessageAttributes["AdditionBulkChangeId"].StringValue))
            {
                additionBulkChangeId = long.Parse(message.MessageAttributes["AdditionBulkChangeId"].StringValue);
            }

            var isPreviousBulkChangeProcessing = bulkChangeRepository.IsPreviousBulkChangeProcessing(context, bulkChangeId);
            if (isPreviousBulkChangeProcessing)
            {
                await EnqueueDeviceBulkChangesAsync(context, bulkChangeId, DeviceBulkChangeQueueUrl, CommonConstants.DELAY_IN_SECONDS_BULK_CHANGE_PROCESSING, 0);
                return bulkChangeId;
            }

            bool newServiceActivations = false;
            bool isRetryNewActivateThingSpaceDevice = false;
            var serviceProviderId = 0;
            var carrierDataGroup = string.Empty;
            var carrierRatePool = string.Empty;
            int newServiceActivationIteration = int.MaxValue;

            if (message.MessageAttributes.ContainsKey("NewServiceActivations"))
            {
                bool.TryParse(message.MessageAttributes["NewServiceActivations"].StringValue,
                    out newServiceActivations);
                int.TryParse(message.MessageAttributes["ServiceProviderId"].StringValue,
                    out serviceProviderId);
                carrierRatePool = message.MessageAttributes.ContainsKey("CarrierRatePool") ? message.MessageAttributes["CarrierRatePool"].StringValue : string.Empty;
                carrierDataGroup = message.MessageAttributes.ContainsKey("CarrierDataGroup") ? message.MessageAttributes["CarrierDataGroup"].StringValue : string.Empty;
                int.TryParse(message.MessageAttributes["NewServiceActivationIteration"].StringValue,
                                    out newServiceActivationIteration);
            }

            if (message.MessageAttributes.ContainsKey("IsRetryNewActivateThingSpaceDevice"))
            {
                bool.TryParse(message.MessageAttributes["IsRetryNewActivateThingSpaceDevice"].StringValue,
                    out isRetryNewActivateThingSpaceDevice);
            }

            try
            {
                // Process device statuses until done/errors reaches NEW_SERVICE_ACTIVATION_MAX_COUNT times.
                if (newServiceActivations)
                {
                    if (newServiceActivationIteration <= NEW_SERVICE_ACTIVATION_MAX_COUNT)
                    {
                        await ProcessNewServiceActivationStatusAsync(context, bulkChangeId, serviceProviderId, carrierRatePool, carrierDataGroup, newServiceActivationIteration, additionBulkChangeId, sqsValues.RetryNumber);
                    }
                    else
                    {
                        LogInfo(context, LogTypeConstant.Error, "Max iteration limit reached. Stopping the process get information after activating the ThingSpace device.");
                    }
                    await bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChangeId, BulkChangeStatus.PROCESSED);
                }
                else if (isRetryNewActivateThingSpaceDevice)
                {
                    bulkChange = GetBulkChange(context, bulkChangeId);
                    if (bulkChange == null)
                    {
                        LogInfo(context, LogTypeConstant.Exception, $"No bulk change found for id {bulkChangeId}");
                    }
                    else
                    {
                        var processUpdateStatus = new FunctionProcessUpdateStatus();
                        var processReUpdate = await processUpdateStatus.ProcessUpdateDeviceAfterActivateThingSpaceDevice(context, sqsValues, bulkChange);
                        if (!processReUpdate)
                        {
                            if (sqsValues.RetryNumber >= int.Parse(ThingSpaceUpdateDeviceStatusRetryNumber))
                            {
                                LogInfo(context, LogTypeConstant.Info, $"Have retried {ThingSpaceUpdateDeviceStatusRetryNumber} times. End process activate ThingSpace Device!");
                                await bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChangeId, BulkChangeStatus.PROCESSED);
                                return bulkChangeId;
                            }
                            // need to wait at least 15 minutes when checking Thing Space device successfully activated
                            await SendMessageToCheckThingSpaceDeviceNewActivate(context, sqsValues.RetryNumber + 1, bulkChangeId, CommonConstants.DELAY_IN_SECONDS_FIFTEEN_MINUTES);
                        }
                        else
                        {
                            await bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChangeId, BulkChangeStatus.PROCESSED);
                        }
                    }
                }
                else if (sqsValues.IsRetryUpdateIdentifier)
                {
                    if (sqsValues.RetryNumber >= int.Parse(ThingSpaceUpdateDeviceStatusRetryNumber))
                    {
                        LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.END_PROCESS_CHANGE_IDENTIFIER, ThingSpaceUpdateDeviceStatusRetryNumber));
                        await MarkProcessedForM2MDeviceChangeAsync(context, sqsValues.M2MDeviceChangeId, false, string.Format(LogCommonStrings.END_PROCESS_CHANGE_IDENTIFIER, ThingSpaceUpdateDeviceStatusRetryNumber));
                        return bulkChangeId;
                    }
                    else
                    {
                        await RetryUpdateIdentifierProcess(context, bulkChangeId, sqsValues);
                    }
                }
                else
                {
                    var shouldContinue = await ProcessBulkChangeAsync(context, bulkChangeId, message, additionBulkChangeId, sqsValues.RetryNumber);

                    if (shouldContinue && QueueHasMoreItems(context, bulkChangeId))
                    {
                        // wait 5 seconds then continue to process other remaining items
                        await EnqueueDeviceBulkChangesAsync(context, bulkChangeId, DeviceBulkChangeQueueUrl, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS, sqsValues.RetryNumber);
                    }
                    else
                    {
                        // update bulk change status PROCESSED
                        await bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChangeId, BulkChangeStatus.PROCESSED);
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"EXCEPTION: {ex.Message} - {ex.StackTrace}");
                LogInfo(context, LogTypeConstant.Info, $"BulkChange Id: {bulkChangeId}");

                if (sqsValues.RetryNumber <= MAXIMUM_NUMBER_OF_RETRIES && QueueHasMoreItems(context, bulkChangeId))
                {
                    sqsValues.RetryNumber++;
                    await EnqueueDeviceBulkChangesAsync(context, bulkChangeId, DeviceBulkChangeQueueUrl, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS, sqsValues.RetryNumber, additionBulkChangeId: additionBulkChangeId);
                }
                else
                {
                    // update bulk change status ERROR
                    await bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChangeId, BulkChangeStatus.ERROR);
                    if (additionBulkChangeId > 0)
                    {
                        await bulkChangeRepository.MarkBulkChangeStatusAsync(context, additionBulkChangeId, BulkChangeStatus.ERROR);
                    }
                }
            }
            return bulkChangeId;
        }

        private async Task EnqueueDeviceBulkChangesAsync(KeySysLambdaContext context, long bulkChangeId,
            string queueUrl, int delaySeconds, int retryNumber, bool newServiceActivations = false, int serviceProviderId = 0,
            string carrierRatePool = "", string carrierDataGroup = "", int newServiceActivationIteration = int.MaxValue, long additionBulkChangeId = 0, bool isRetryUpdateIdentifier = false, long m2mDeviceChangeId = 0, string requestId = "")
        {
            LogInfo(context, CommonConstants.SUB, $"{bulkChangeId},'{queueUrl}',{delaySeconds},{retryNumber})");
            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var messageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    {
                        SQSMessageKeyConstant.BULK_CHANGE_ID,
                        new MessageAttributeValue {DataType = nameof(String), StringValue = bulkChangeId.ToString()}
                    },
                    {
                        SQSMessageKeyConstant.NEW_SERVICE_ACTIVATIONS,
                        new MessageAttributeValue {DataType = nameof(String), StringValue = newServiceActivations.ToString()}
                    },
                    {
                        SQSMessageKeyConstant.SERVICE_PROVIDER_ID,
                            new MessageAttributeValue {DataType = nameof(String), StringValue = serviceProviderId.ToString()}
                    },
                    {
                        SQSMessageKeyConstant.NEW_SERVICE_ACTIVATION_ITERATION,
                            new MessageAttributeValue {DataType = nameof(String), StringValue = newServiceActivationIteration.ToString()}
                    },
                    {
                        SQSMessageKeyConstant.ADDITION_BULK_CHANGE_ID,
                            new MessageAttributeValue {DataType = nameof(String), StringValue = additionBulkChangeId.ToString()}
                    },
                    {
                        SQSMessageKeyConstant.IS_FROM_AUTOMATED_UPDATE_DEVICE_STATUS_LAMBDA,
                            new MessageAttributeValue {DataType = nameof(String), StringValue = sqsValues.IsFromAutomatedUpdateDeviceStatusLambda.ToString()}
                    },
                    {
                       SQSMessageKeyConstant.RETRY_NUMBER,
                       new MessageAttributeValue { DataType = nameof(String), StringValue = retryNumber.ToString()}
                    },
                    {
                       SQSMessageKeyConstant.IS_RETRY_UPDATE_IDENTIFIER,
                       new MessageAttributeValue { DataType = nameof(String), StringValue = isRetryUpdateIdentifier.ToString()}
                    }
                };

                if (m2mDeviceChangeId > 0)
                {
                    messageAttributes.Add(SQSMessageKeyConstant.M2M_DEVICE_CHANGE_ID,
                        new MessageAttributeValue { DataType = nameof(String), StringValue = m2mDeviceChangeId.ToString() });
                }

                if (!string.IsNullOrWhiteSpace(requestId))
                {
                    messageAttributes.Add(SQSMessageKeyConstant.REQUEST_ID,
                        new MessageAttributeValue { DataType = nameof(String), StringValue = requestId });
                }

                if (!string.IsNullOrWhiteSpace(carrierRatePool))
                {
                    messageAttributes.Add(SQSMessageKeyConstant.CARRIER_RATE_POOL,
                        new MessageAttributeValue { DataType = nameof(String), StringValue = carrierRatePool });
                }

                if (!string.IsNullOrWhiteSpace(carrierDataGroup))
                {
                    messageAttributes.Add(SQSMessageKeyConstant.CARRIER_DATA_GROUP,
                        new MessageAttributeValue { DataType = nameof(String), StringValue = carrierDataGroup });
                }

                var request = new SendMessageRequest
                {
                    DelaySeconds = delaySeconds,
                    MessageAttributes = messageAttributes,
                    MessageBody = "Not used",
                    QueueUrl = queueUrl
                };

                var response = await client.SendMessageAsync(request);
                if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
                {
                    LogInfo(context, LogTypeConstant.Exception, $"Error enqueuing retrieval attempt for bulk change id {bulkChangeId}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                }
            }
        }

        private async Task<bool> ProcessBulkChangeAsync(KeySysLambdaContext context, long bulkChangeId, SQSEvent.SQSMessage message, long additionBulkChangeId, int retryNumber)
        {
            //Update
            var centralConnectionString = EnvironmentRepo.GetEnvironmentVariable(context.Context, "CentralDbConnectionString");
            var bulkRepo = new BulkChangeRepository();

            var bulkChange = GetBulkChange(context, bulkChangeId);
            if (bulkChange == null)
            {
                LogInfo(context, LogTypeConstant.Exception, $"No bulk change found for id {bulkChangeId}");
                return false;
            }

            var sqlRetryPolicy = GetSqlTransientRetryPolicy(context);
            var logRepo = new DeviceBulkChangeLogRepository(context.CentralDbConnectionString, sqlRetryPolicy);

            switch (bulkChange.ChangeRequestType.ToLowerInvariant())
            {
                case ChangeRequestType.StatusUpdate:
                    var changes = GetDeviceChanges(context, bulkChange.Id, bulkChange.PortalTypeId, PageSize).ToList();
                    return await ProcessStatusUpdateAsync(context, logRepo, bulkChange, changes, retryNumber);
                case ChangeRequestType.ActivateNewService:
                    return await ProcessNewServiceActivationAsync(context, logRepo, bulkChange, message, additionBulkChangeId, retryNumber);
                case ChangeRequestType.Archival:
                    var archivalChanges = GetDeviceChanges(context, bulkChangeId, bulkChange.PortalTypeId, PageSize).ToList();
                    await ProcessArchivalAsync(context, logRepo, bulkChange.Id, archivalChanges);
                    return true;
                case ChangeRequestType.CustomerRatePlanChange:
                    await ProcessCustomerRatePlanChangeAsync(context, logRepo, bulkChange, sqlRetryPolicy);
                    return false;
                case ChangeRequestType.CustomerAssignment:
                    var changeRequest = GetBulkChangeRequest(context, bulkChangeId, bulkChange.PortalTypeId);
                    var request = JsonConvert.DeserializeObject<BulkChangeAssociateCustomer>(changeRequest);
                    var pageSize = PageSize;
                    if (request?.CreateRevService == false)
                    {
                        pageSize = CommonConstants.PAGE_SIZE_WHEN_NOT_CREATE_SERVICE;
                    }
                    var associateCustomerChanges = GetDeviceChanges(context, bulkChange.Id, bulkChange.PortalTypeId, pageSize).ToList();
                    if (string.IsNullOrEmpty(request?.RevCustomerId))
                    {
                        await bulkChangeRepository.UpdateAMOPCustomer(context, logRepo, associateCustomerChanges, bulkChange);
                    }
                    else
                    {
                        await ProcessAssociateCustomerAsync(context, logRepo, bulkChange, associateCustomerChanges);
                    }
                    return true;
                case ChangeRequestType.CarrierRatePlanChange:
                    return await ProcessCarrierRatePlanChangeAsync(context, logRepo, bulkChange, sqlRetryPolicy);
                case ChangeRequestType.CreateRevService:
                    await ProcessCreateRevServiceAsync(context, logRepo, bulkChange);
                    return false;
                case ChangeRequestType.ChangeICCIDAndIMEI:
                    await ProcessChangeEquipmentAsync(context, logRepo, bulkChange);
                    return true;
                case ChangeRequestType.EditUsernameCostCenter:
                    //Update -Here bulkRepo parameter added
                    return await ProcessEditUsernameAsync(context, logRepo, bulkRepo, bulkChange);
                default:
                    LogInfo(context, LogTypeConstant.Exception, $"Unhandled bulk change request type: {bulkChange.ChangeRequestType}");
                    return false;
            }
        }

        private async Task ProcessCreateRevServiceAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange)
        {
            var changes = GetDeviceChanges(context, bulkChange.Id, bulkChange.PortalTypeId, Int32.MaxValue);
            if (changes == null || changes.Count == 0)
            {
                context.logger.LogInfo("WARN", $"No unprocessed changes found for create rev service {bulkChange.Id}");
                return;
            }

            //Http Retry Policy
            var httpRetryPolicy = GetHttpRetryPolicy(context);

            //Sql Retry Policy
            var sqlRetryPolicy = GetSqlTransientAsyncRetryPolicy(context);

            var revIOAuthenticationRepository = new RevioAuthenticationRepository(context.CentralDbConnectionString, new Base64Service());
            var integrationAuthenticationId = JsonConvert
                .DeserializeObject<BulkChangeAssociateCustomer>(changes.FirstOrDefault()?.ChangeRequest)
                .IntegrationAuthenticationId;

            var revIOAuthentication = revIOAuthenticationRepository.GetRevioApiAuthentication(integrationAuthenticationId);
            var revApiClient = new RevioApiClient(new SingletonHttpClientFactory(), _httpRequestFactory, revIOAuthentication,
                context.IsProduction);

            foreach (var change in changes)
            {
                var changeRequest = JsonConvert.DeserializeObject<BulkChangeAssociateCustomer>(change.ChangeRequest);

                context.logger.LogInfo("INFO", $"Creating Rev Service for {changeRequest.ICCID} or {changeRequest.Number}");
                var createRevServiceResult = await CreateRevServiceAsync(context, logRepo, sqlRetryPolicy,
                    bulkChange, change, revApiClient, changeRequest, integrationAuthenticationId);

                var apiResult = createRevServiceResult.ResponseObject;

                if (apiResult == null || !apiResult.Ok)
                {
                    // Not logging to DeviceBulkChangeLogRepository b/c it is logged in CreateRevServiceAsync
                    context.logger.LogInfo("ERROR", $"Error creating Rev Service for device {changeRequest.ICCID} or {changeRequest.Number}");
                }

                // add customer rate plan
                await BulkchangeUpdateCustomerRatePlan(context, logRepo, bulkChange, change);

                await sqlRetryPolicy.ExecuteAsync(async () =>
                {
                    await MarkProcessedForRevService(context, change.Id, apiResult?.Ok != null && (bool)apiResult?.Ok,
                        JsonConvert.SerializeObject(apiResult), bulkChange.PortalTypeId);
                });
            }

            await sqlRetryPolicy.ExecuteAsync(async () =>
            {
                await UpdateDeviceRevServiceLinks(context);
            });
        }

        private async Task BulkchangeUpdateCustomerRatePlan(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, BulkChangeDetailRecord change)
        {
            context.logger.LogInfo("SUB", $"BulkchangeUpdateCustomerRatePlan()");
            context.logger.LogInfo("INFO", $"Portal type: {bulkChange.PortalTypeId}");
            var changeRequest = JsonConvert.DeserializeObject<BulkChangeAssociateCustomer>(change.ChangeRequest);
            context.logger.LogInfo("INFO", $"Device: {changeRequest.ICCID}");

            var apiResult = new CreateResponse();
            apiResult.Ok = true;

            if (changeRequest.AddCustomerRatePlan)
            {
                int? customerRatePlanIdToSubmit = null;
                int customerRatePlanId = 0;
                if (int.TryParse(changeRequest.CustomerRatePlan, out customerRatePlanId))
                {
                    customerRatePlanIdToSubmit = customerRatePlanId;
                }

                int? customerRatePoolIdToSubmit = null;
                int customerRatePoolId = 0;
                if (int.TryParse(changeRequest.CustomerRatePool, out customerRatePoolId))
                {
                    customerRatePoolIdToSubmit = customerRatePoolId;
                }

                var sqlRetryPolicyCustomerRatePlan = GetSqlTransientRetryPolicy(context);
                context.logger.LogInfo("INFO", $"Processing Customer Rate Plan update {changeRequest.ICCID}");
                var ratePlanChangeResult = await ProcessCustomerRatePlanChangeAsync(bulkChange.Id,
                    customerRatePlanIdToSubmit, changeRequest.EffectiveDate, null, customerRatePoolIdToSubmit,
                    context.CentralDbConnectionString, context.logger, sqlRetryPolicyCustomerRatePlan, false);

                if (bulkChange.PortalTypeId == PortalTypeM2M)
                {
                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = ratePlanChangeResult.HasErrors ? ratePlanChangeResult.ResponseObject : null,
                        HasErrors = ratePlanChangeResult.HasErrors,
                        LogEntryDescription = "Associate Customer: Update AMOP Customer Rate Plan",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = ratePlanChangeResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = ratePlanChangeResult.ActionText + Environment.NewLine + ratePlanChangeResult.RequestObject,
                        ResponseText = ratePlanChangeResult.ResponseObject
                    });
                }
                else
                {
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = ratePlanChangeResult.HasErrors ? ratePlanChangeResult.ResponseObject : null,
                        HasErrors = ratePlanChangeResult.HasErrors,
                        LogEntryDescription = "Associate Customer: Update AMOP Customer Rate Plan",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = ratePlanChangeResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = ratePlanChangeResult.ActionText + Environment.NewLine + ratePlanChangeResult.RequestObject,
                        ResponseText = ratePlanChangeResult.ResponseObject
                    });
                }
            }
        }

        private async Task<bool> ProcessCarrierRatePlanChangeAsync(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, ISyncPolicy syncPolicy)
        {
            var serviceProviderId = bulkChange.ServiceProviderId;
            var changes = GetDeviceChanges(context, bulkChange.Id, bulkChange.PortalTypeId, PageSize);
            if (changes == null || changes.Count == 0)
            {
                context.logger.LogInfo("WARN", $"No unprocessed changes found for carrier rate plan change {bulkChange.Id}");
                return false;
            }

            switch (bulkChange.IntegrationId)
            {
                case (int)IntegrationType.Telegence:
                    return await ProcessTelegenceCarrierRatePlanChange(context, logRepo, bulkChange, serviceProviderId, changes, syncPolicy);
                case (int)IntegrationType.eBonding:
                    return await ProcessEBondingCarrierRatePlanChange(context, logRepo, bulkChange, changes);
                case (int)IntegrationType.Jasper:
                case (int)IntegrationType.TMobileJasper:
                case (int)IntegrationType.POD19:
                case (int)IntegrationType.Rogers:
                    return await ProcessJasperCarrierRatePlanChange(context, logRepo, bulkChange, serviceProviderId, changes);
                case (int)IntegrationType.ThingSpace:
                    return await ProcessThingSpaceCarrierRatePlanChange(context, logRepo, bulkChange, changes);
                case (int)IntegrationType.Teal:
                    return await ProcessTealCarrierRatePlanChange(context, logRepo, bulkChange, serviceProviderId, changes);
                case (int)IntegrationType.Pond:
                    return await ProcessPondCarrierRatePlanChange(context, logRepo, bulkChange, serviceProviderId, changes);
            }
            return true;
        }
        private async Task<bool> ProcessThingSpaceCarrierRatePlanChange(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes)
        {
            LogInfo(context, "SUB", "ProcessThingSpaceCarrierRatePlanChange()");
            LogInfo(context, "INFO", $"BulkChangeID: {bulkChange.Id}");

            var environmentRepo = new EnvironmentRepository();
            var httpClientFactory = new SingletonHttpClientFactory();
            var httpRequestFactory = new HttpRequestFactory();
            var thingSpaceAuthenticationCommon = ThingSpaceCommon.GetThingspaceAuthenticationInformation(context.CentralDbConnectionString, bulkChange.ServiceProviderId);

            //map to thingSpaceAuthentication in ThingSpace.core
            var thingSpaceAuthentication = new ThingSpaceServiceAmop.Models.ThingSpaceAuthentication
            {
                ThingSpaceAuthenticationId = thingSpaceAuthenticationCommon.ThingSpaceAuthenticationId,
                BaseUrl = thingSpaceAuthenticationCommon.BaseUrl,
                ClientId = thingSpaceAuthenticationCommon.ClientId,
                ClientSecret = thingSpaceAuthenticationCommon.ClientSecret,
                AuthTokenUrl = thingSpaceAuthenticationCommon.AuthTokenUrl,
                Username = thingSpaceAuthenticationCommon.Username,
                Password = thingSpaceAuthenticationCommon.Password,
                AuthUrl = thingSpaceAuthenticationCommon.AuthUrl,
                AccountNumber = thingSpaceAuthenticationCommon.AccountNumber,
                WriteIsEnabled = thingSpaceAuthenticationCommon.WriteIsEnabled
            };

            if (thingSpaceAuthentication == null)
            {
                var errorMessage = "Failed to get Thing Space Authentication Information.";
                LogInfo(context, "ERROR", errorMessage);

                var firstChange = changes.First();
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "M2M Carrier Rate Plan Change: Pre-flight Check",
                    M2MDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });

                return false;
            }

            if (!thingSpaceAuthentication.WriteIsEnabled)
            {
                var errorMessage = "Writes disabled for this service provider.";
                LogInfo(context, "WARN", errorMessage);

                var firstChange = changes.First();
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Carrier Rate Plan Change: Pre-flight Check",
                    M2MDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });
                return false;
            }

            var httpRetryPolicy = GetHttpRetryPolicy(context);

            var thingSpaceDeviceDetailService = new ThingSpaceServiceAmop.ThingSpaceDeviceDetailService(thingSpaceAuthentication,
                new Base64Service(), httpClientFactory, httpRetryPolicy, context.logger, httpRequestFactory);

            var deviceRepository = new DeviceRepository(context.logger, environmentRepo, context.Context);

            foreach (var change in changes)
            {
                var carrierRatePlan =
                    JsonConvert.DeserializeObject<BulkChangeCarrierRatePlanUpdate>(change.ChangeRequest);
                var thingSpaceDeviceDetail = new ThingSpaceDeviceDetail
                {
                    ICCID = new List<string>() { change.DeviceIdentifier },
                    CarrierRatePlan = carrierRatePlan.CarrierRatePlanUpdate.CarrierRatePlan,
                };
                var result = await thingSpaceDeviceDetailService.UpdateThingSpaceDeviceDetailsAsync(thingSpaceDeviceDetail, environmentRepo,
                    context.Context, context.logger);
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = result.HasErrors ? JsonConvert.SerializeObject(result.ResponseObject) : null,
                    HasErrors = result.HasErrors,
                    LogEntryDescription = "Update ThingSpace Rate Plan: ThingSpace API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = result.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = result.ActionText + Environment.NewLine + result.RequestObject,
                    ResponseText = JsonConvert.SerializeObject(result.ResponseObject)
                });

                if (result.HasErrors)
                {
                    var errorMessage = "Failed to update Carrier Rate Plan: ThingSpace API.";
                    LogInfo(context, "WARN", errorMessage);
                    await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, false,
                        errorMessage);
                    continue;
                }

                var dbResult = await deviceRepository.UpdateRatePlanAsync(thingSpaceDeviceDetail.ICCID[0],
                    thingSpaceDeviceDetail.CarrierRatePlan, null, change.TenantId);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = dbResult.HasErrors ? JsonConvert.SerializeObject(dbResult.ResponseObject) : null,
                    HasErrors = dbResult.HasErrors,
                    LogEntryDescription = "Update ThingSpace Rate Plan: AMOP Update",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = dbResult.ActionText + Environment.NewLine + dbResult.RequestObject,
                    ResponseText = JsonConvert.SerializeObject(dbResult.ResponseObject)
                });

                await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, !dbResult.HasErrors,
                    !dbResult.HasErrors
                        ? "Successfully update Carrier Rate Plan; AMOP Update"
                        : "Failed to update Carrier Rate Plan: AMOP Update");
            }

            return true;
        }

        private async Task<bool> ProcessJasperCarrierRatePlanChange(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, int serviceProviderId,
            ICollection<BulkChangeDetailRecord> changes)
        {
            LogInfo(context, "SUB", "ProcessJasperCarrierRatePlanChange()");
            var environmentRepo = new EnvironmentRepository();
            var httpClientFactory = new KeysysHttpClientFactory();
            var jasperAuthentication = JasperCommon.GetJasperAuthenticationInformation(context.CentralDbConnectionString,
                bulkChange.ServiceProviderId);

            if (jasperAuthentication == null)
            {
                var errorMessage = "Failed to get Jasper Authentication Information.";
                LogInfo(context, "ERROR", errorMessage);

                var firstChange = changes.First();
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "M2M Carrier Rate Plan Change: Pre-flight Check",
                    M2MDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });

                return false;
            }

            if (!jasperAuthentication.WriteIsEnabled)
            {
                var errorMessage = "Writes disabled for this service provider.";
                LogInfo(context, "WARN", errorMessage);

                var firstChange = changes.First();
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Carrier Rate Plan Change: Pre-flight Check",
                    M2MDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });
                return false;
            }

            var httpRetryPolicy = GetHttpRetryPolicy(context);

            var jasperRatePlanRepository =
                new JasperRatePlanRepository(context.logger, environmentRepo, context.Context);

            var jasperDeviceDetailService = new JasperDeviceDetailService(jasperAuthentication,
                new Base64Service(), httpClientFactory, httpRetryPolicy);

            var deviceRepository = new DeviceRepository(context.logger, environmentRepo, context.Context);

            foreach (var change in changes)
            {
                var carrierRatePlan =
                    JsonConvert.DeserializeObject<BulkChangeCarrierRatePlanUpdate>(change.ChangeRequest);
                var jasperDeviceDetail = new JasperDeviceDetail
                {
                    ICCID = change.DeviceIdentifier,
                    CarrierRatePlan = carrierRatePlan.CarrierRatePlanUpdate.CarrierRatePlan,
                    CommunicationPlan = carrierRatePlan.CarrierRatePlanUpdate.CommPlan,
                };
                var result = await jasperDeviceDetailService.UpdateJasperDeviceDetailsAsync(jasperDeviceDetail, environmentRepo,
                    context.Context, context.logger);
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = result.HasErrors ? JsonConvert.SerializeObject(result.ResponseObject) : null,
                    HasErrors = result.HasErrors,
                    LogEntryDescription = "Update Jasper Rate Plan: Jasper API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = result.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = result.ActionText + Environment.NewLine + result.RequestObject,
                    ResponseText = JsonConvert.SerializeObject(result.ResponseObject)
                });

                if (result.HasErrors)
                {
                    var errorMessage = "Failed to update Carrier Rate Plan: Jasper API.";
                    LogInfo(context, "WARN", errorMessage);
                    await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, false,
                        errorMessage);
                    continue;
                }

                var dbResult = await deviceRepository.UpdateRatePlanAsync(jasperDeviceDetail.ICCID,
                    jasperDeviceDetail.CarrierRatePlan, jasperDeviceDetail.CommunicationPlan, change.TenantId);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = dbResult.HasErrors ? JsonConvert.SerializeObject(dbResult.ResponseObject) : null,
                    HasErrors = dbResult.HasErrors,
                    LogEntryDescription = "Update Jasper Rate Plan: AMOP Update",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = dbResult.ActionText + Environment.NewLine + dbResult.RequestObject,
                    ResponseText = JsonConvert.SerializeObject(dbResult.ResponseObject)
                });

                await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, !dbResult.HasErrors,
                    !dbResult.HasErrors
                        ? "Successfully update Carrier Rate Plan; AMOP Update"
                        : "Failed to update Carrier Rate Plan: AMOP Update");
            }

            return true;
        }

        private async Task<bool> ProcessTealCarrierRatePlanChange(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, int serviceProviderId, ICollection<BulkChangeDetailRecord> changes)
        {
            LogInfo(context, CommonConstants.SUB, "");
            var environmentRepo = new EnvironmentRepository();
            var tealRepository = new TealRepository(context);
            var tealAuthentication = tealRepository.GetTealAuthenticationInformation(serviceProviderId);
            var processedBy = context.Context.FunctionName;
            var tealAuthenticationErrorMessage = string.Empty;
            if (tealAuthentication == null)
            {
                tealAuthenticationErrorMessage = string.Format(LogCommonStrings.FAILED_GET_AUTHENTICATION_INFORMATION, CommonConstants.TEAL_CARRIER_NAME);
            }
            else if (!tealAuthentication.WriteIsEnabled)
            {
                tealAuthenticationErrorMessage = LogCommonStrings.SERVICE_PROVIDER_IS_DISABLED;
            }
            if (!string.IsNullOrWhiteSpace(tealAuthenticationErrorMessage))
            {
                LogInfo(context, CommonConstants.WARNING, tealAuthenticationErrorMessage);
                var firstChange = changes.FirstOrDefault();
                var logEntry = string.Format(LogCommonStrings.FAILED_TO_UPDATE, TealHelper.CommonString.TEAL_CARRIER_RATE_PLAN);
                if (firstChange != null)
                {
                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(bulkChange.Id, tealAuthenticationErrorMessage, firstChange.Id, processedBy, BulkChangeStatus.ERROR, firstChange.ChangeRequest, true, logEntry));
                }
                return false;
            }

            var base64Service = new Base64Service();
            var deviceRepository = new DeviceRepository(context.logger, environmentRepo, context.Context);
            var tealService = new TealAPIService(tealAuthentication, base64Service, new SingletonHttpClientFactory(), new HttpRequestFactory());
            foreach (var change in changes)
            {
                if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                {
                    break;
                }
                var carrierRatePlan = JsonConvert.DeserializeObject<BulkChangeCarrierRatePlanUpdate>(change.ChangeRequest);
                var tealRatePlan = new TealUpdateRatePlan()
                {
                    Eid = change.Eid,
                    PlanUuid = carrierRatePlan.CarrierRatePlanUpdate.PlanUuid
                };
                var tealRequest = new TealAPIRequest<TealUpdateRatePlan>() { Entries = new List<TealUpdateRatePlan>() { tealRatePlan } };
                var tealRequestJson = JsonConvert.SerializeObject(tealRequest);
                var tealAPIResult = await tealService.ProcessTealUpdateAsync(TealAssignRatePlanPath, TealHelper.CommonString.TEAL_ASSIGN_RATE_PLAN, tealRequestJson, context.logger);

                var tealAPIResultLogEntry = $"{TealHelper.CommonString.TEAL_CARRIER_RATE_PLAN}: {LogCommonStrings.API}.";
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(tealAPIResult, bulkChange.Id, change.Id, tealAPIResultLogEntry));

                var tealResponseRootObject = JsonConvert.DeserializeObject<TealResponseRootObject<TealUpdateResultResponse>>(tealAPIResult.ResponseObject);
                var tealUpdateResultResponse = tealResponseRootObject?.Entries.FirstOrDefault();
                if (tealAPIResult.HasErrors || tealResponseRootObject == null || tealUpdateResultResponse == null || !tealUpdateResultResponse.Success)
                {
                    var errorMessage = $"{string.Format(LogCommonStrings.FAILED_TO_UPDATE, TealHelper.CommonString.TEAL_CARRIER_RATE_PLAN)}";
                    if (!tealUpdateResultResponse.Success)
                    {
                        errorMessage += $": {tealUpdateResultResponse.ErrorMessage}";
                    }
                    LogInfo(context, CommonConstants.WARNING, errorMessage);
                    await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, false, errorMessage);
                    continue;
                }

                var dbUpdateResult = await deviceRepository.UpdateRatePlanAsync(change.ICCID, carrierRatePlan.CarrierRatePlanUpdate.CarrierRatePlan, null, change.TenantId);
                var databaseResultLog = $"{string.Format(LogCommonStrings.SUCCESSULLY_UPDATE, TealHelper.CommonString.TEAL_CARRIER_RATE_PLAN)}: {LogCommonStrings.DATABASE}.";
                if (dbUpdateResult.HasErrors)
                {
                    databaseResultLog = $"{string.Format(LogCommonStrings.FAILED_TO_UPDATE, TealHelper.CommonString.TEAL_CARRIER_RATE_PLAN)}: {LogCommonStrings.DATABASE}.";
                }

                var tealDbResultLogEntry = $"{TealHelper.CommonString.TEAL_CARRIER_RATE_PLAN}: {LogCommonStrings.DATABASE}.";
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(dbUpdateResult, bulkChange.Id, change.Id, tealDbResultLogEntry));
                await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, !dbUpdateResult.HasErrors, databaseResultLog);
            }

            return true;
        }

        public async Task<bool> ProcessPondCarrierRatePlanChange(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, int serviceProviderId, ICollection<BulkChangeDetailRecord> changes)
        {
            LogInfo(context, CommonConstants.SUB, "");

            var pondRepository = new PondRepository(context.CentralDbConnectionString);
            var pondAuthentication = pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, serviceProviderId);
            var processedBy = context.Context.FunctionName;
            var authenticationErrorMessage = string.Empty;
            if (pondAuthentication == null)
            {
                authenticationErrorMessage = string.Format(LogCommonStrings.FAILED_GET_AUTHENTICATION_INFORMATION, CommonConstants.POND_CARRIER_NAME);
            }
            else if (!pondAuthentication.WriteIsEnabled)
            {
                authenticationErrorMessage = LogCommonStrings.SERVICE_PROVIDER_IS_DISABLED;
            }

            if (!string.IsNullOrWhiteSpace(authenticationErrorMessage))
            {
                LogInfo(context, CommonConstants.WARNING, authenticationErrorMessage);
                var firstChange = changes.FirstOrDefault();
                var logEntry = string.Format(LogCommonStrings.FAILED_TO_UPDATE, PondHelper.CommonString.POND_CARRIER_RATE_PLAN);
                if (firstChange != null)
                {
                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(bulkChange.Id, authenticationErrorMessage, firstChange.Id, processedBy, BulkChangeStatus.ERROR, firstChange.ChangeRequest, true, logEntry));
                }
                return false;
            }

            var pondApiService = new PondApiService(pondAuthentication, new HttpRequestFactory(), context.IsProduction);
            var baseUri = pondAuthentication.SandboxURL;
            if (context.IsProduction)
            {
                baseUri = pondAuthentication.ProductionURL;
            }
            await UpdatePondCarrierRatePlanForDevices(context, logRepo, bulkChange, serviceProviderId, changes, pondRepository, pondAuthentication, processedBy, pondApiService, baseUri);

            return true;
        }

        public async Task UpdatePondCarrierRatePlanForDevices(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, int serviceProviderId, ICollection<BulkChangeDetailRecord> changes, PondRepository pondRepository, PondAuthentication pondAuthentication, string processedBy, PondApiService pondApiService, string baseUri)
        {
            foreach (var change in changes)
            {
                if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                {
                    break;
                }
                try
                {
                    var existingPackageIds = pondRepository.GetExistingPackages(ParameterizedLog(context), change.ICCID, serviceProviderId, PondHelper.PackageStatus.ACTIVE);

                    // First add the new package based on Package Template Id
                    var carrierRatePlan = JsonConvert.DeserializeObject<BulkChangeCarrierRatePlanUpdate>(change.ChangeRequest);
                    var pondAddPackageResponse = await AddNewPondPackage(logRepo, bulkChange, pondAuthentication, pondApiService, baseUri, change, carrierRatePlan);
                    // Check if the new package is added successfully
                    if (pondAddPackageResponse.HasErrors)
                    {
                        await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, false, pondAddPackageResponse.ResponseObject);
                        continue;
                    }
                    var pondPackage = JsonConvert.DeserializeObject<PondDeviceCarrierRatePlanResponse>(pondAddPackageResponse.ResponseObject);
                    if (pondPackage == null)
                    {
                        var errorMessage = string.Format(LogCommonStrings.FAILED_TO_UPDATE, PondHelper.CommonString.POND_CARRIER_RATE_PLAN);
                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(pondAddPackageResponse, bulkChange.Id, change.Id, LogCommonStrings.ERROR_WHILE_CALLING_POND_API));
                        await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, false, errorMessage);
                        continue;
                    }
                    var shouldUpdateDeviceRatePlan = false;
                    // Then update the new package status to Active
                    var updateStatusResult = await UpdateStatusForNewPondPackageOnApi(logRepo, bulkChange, pondAuthentication, pondApiService, baseUri, change, pondAddPackageResponse, pondPackage.PackageId, PondHelper.PackageStatus.ACTIVE);
                    if (!updateStatusResult.HasErrors)
                    {
                        pondPackage.Status = PondHelper.PackageStatus.ACTIVE;

                        if (existingPackageIds != null && existingPackageIds.Count > 0)
                        {
                            updateStatusResult = await TerminateExistingPackages(context, existingPackageIds, pondApiService, pondAuthentication, baseUri, processedBy, pondRepository, bulkChange.Id, change.Id, logRepo);
                            if (!updateStatusResult.HasErrors)
                            {
                                shouldUpdateDeviceRatePlan = true;
                            }
                            else
                            {
                                updateStatusResult = await UpdateStatusForNewPondPackageOnApi(logRepo, bulkChange, pondAuthentication, pondApiService, baseUri, change, pondAddPackageResponse, pondPackage.PackageId, PondHelper.PackageStatus.TERMINATED);
                                if (!updateStatusResult.HasErrors)
                                {
                                    pondPackage.Status = PondHelper.PackageStatus.TERMINATED;
                                }
                            }
                        }
                    }

                    // Add the new package record to database
                    SaveNewPondDeviceCarrierRatePlanToDatabase(context, logRepo, bulkChange, serviceProviderId, pondRepository, processedBy, change, pondPackage);
                    if (shouldUpdateDeviceRatePlan)
                    {
                        // Last step is to update the carrier rate plan to device
                        updateStatusResult = await UpdateM2MDeviceRatePlan(logRepo, bulkChange, change, carrierRatePlan.CarrierRatePlanUpdate.CarrierRatePlan, updateStatusResult);
                    }
                    await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, !updateStatusResult.HasErrors, updateStatusResult.ResponseObject);
                }
                catch (Exception ex)
                {
                    LogInfo(context, CommonConstants.EXCEPTION, $"{PondHelper.CommonString.POND_CARRIER_RATE_PLAN}: {ex.Message} - {ex.StackTrace}");
                    await MarkProcessedForM2MDeviceChangeAsync(context, change.Id, false,
                        $"{string.Format(LogCommonStrings.FAILED_TO_UPDATE, PondHelper.CommonString.POND_CARRIER_RATE_PLAN)}: {LogCommonStrings.DATABASE}.");
                }
            }
        }

        protected virtual async Task<DeviceChangeResult<string, string>> UpdateM2MDeviceRatePlan(DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, BulkChangeDetailRecord change, string carrierRatePlanName, DeviceChangeResult<string, string> updateStatusResult)
        {
            updateStatusResult = await deviceRepository.UpdateRatePlanAsync(change.ICCID, carrierRatePlanName, null, change.TenantId);
            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(updateStatusResult, bulkChange.Id, change.Id, $"{LogCommonStrings.DEVICE_UPDATE_CARRIER_RATE_PLAN}: {LogCommonStrings.DATABASE}."));
            return updateStatusResult;
        }

        private async Task<DeviceChangeResult<string, string>> AddNewPondPackage(DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, PondAuthentication pondAuthentication, PondApiService pondApiService, string baseUri, BulkChangeDetailRecord change, BulkChangeCarrierRatePlanUpdate carrierRatePlan)
        {
            var pondAddPackage = new PondAddPackageRequest()
            {
                PackageTypeId = carrierRatePlan.CarrierRatePlanUpdate.RatePlanId
            };
            var pondAddPackageRequestJson = JsonConvert.SerializeObject(pondAddPackage);
            var pondAddPackageApiUrl = $"{baseUri.TrimEnd('/')}/{pondAuthentication.DistributorId}/{string.Format(URLConstants.POND_ADD_PACKAGE_END_POINT, change.ICCID)}";
            // Add new package to device
            var pondAddPackageResponse = await pondApiService.ProcessPondUpdateAsync(HttpClientSingleton.Instance, pondAddPackageRequestJson, pondAddPackageApiUrl, null);
            var pondAddPackageLogEntry = $"{LogCommonStrings.POND_ADD_CARRIER_RATE_PLAN}: {LogCommonStrings.API}.";
            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(pondAddPackageResponse, bulkChange.Id, change.Id, pondAddPackageLogEntry));
            return pondAddPackageResponse;
        }

        public void SaveNewPondDeviceCarrierRatePlanToDatabase(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, int serviceProviderId, PondRepository pondRepository, string processedBy, BulkChangeDetailRecord change, PondDeviceCarrierRatePlanResponse pondPackage)
        {
            var dbAddDeviceCarrierRatePlanResult = pondRepository.AddDeviceCarrierRatePlan(ParameterizedLog(context), pondPackage, serviceProviderId, processedBy);
            var dbAddDeviceCarrierRatePlanLogEntry = $"{LogCommonStrings.POND_ADD_CARRIER_RATE_PLAN}: {LogCommonStrings.DATABASE}.";
            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(dbAddDeviceCarrierRatePlanResult, bulkChange.Id, change.Id, dbAddDeviceCarrierRatePlanLogEntry));
        }

        private async Task<DeviceChangeResult<string, string>> TerminateExistingPackages(KeySysLambdaContext context, List<string> existingPackageIds, PondApiService pondApiService, PondAuthentication pondAuthentication, string baseUri, string processedBy, PondRepository pondRepository, long bulkChangeId, long changeId, DeviceBulkChangeLogRepository logRepo)
        {
            DeviceChangeResult<string, string> updateStatusResult;
            foreach (var packageId in existingPackageIds)
            {
                updateStatusResult = await pondApiService.UpdatePackageStatus(pondAuthentication, baseUri, packageId, PondHelper.PackageStatus.TERMINATED);
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(updateStatusResult, bulkChangeId, changeId, $"{LogCommonStrings.POND_UPDATE_CARRIER_RATE_PLAN_STATUS}: {LogCommonStrings.API}."));
                if (updateStatusResult.HasErrors)
                {
                    LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.ERROR_WHEN_TERMINATING_PACKAGE, packageId, updateStatusResult));
                    return updateStatusResult;
                }
            }
            // All packages terminated successfully => Update the status in AMOP database
            updateStatusResult = pondRepository.UpdateDeviceCarrierRatePlanStatus(ParameterizedLog(context), string.Join(",", existingPackageIds.ToArray()), PondHelper.PackageStatus.TERMINATED, processedBy);
            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(updateStatusResult, bulkChangeId, changeId, $"{LogCommonStrings.POND_UPDATE_CARRIER_RATE_PLAN_STATUS}: {LogCommonStrings.DATABASE}."));
            return updateStatusResult;
        }

        protected async Task<DeviceChangeResult<string, string>> UpdateStatusForNewPondPackageOnApi(DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, PondAuthentication pondAuthentication, PondApiService pondApiService, string baseUri, BulkChangeDetailRecord change, DeviceChangeResult<string, string> pondAddPackageResponse, string packageId, string newPackageStatus)
        {
            var pondUpdatePackageResponse = await pondApiService.UpdatePackageStatus(pondAuthentication, baseUri, packageId, newPackageStatus);
            var pondUpdatePackageLogEntry = $"{LogCommonStrings.POND_UPDATE_CARRIER_RATE_PLAN_STATUS}: {LogCommonStrings.API}.";
            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(pondAddPackageResponse, bulkChange.Id, change.Id, pondUpdatePackageLogEntry));
            return pondUpdatePackageResponse;
        }


        private async Task<bool> ProcessEBondingCarrierRatePlanChange(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes)
        {
            LogInfo(context, "SUB", "ProcessEBondingCarrierRatePlanChange()");
            var eBondingAuthentication =
                GetEBondingAuthentication(context.CentralDbConnectionString, bulkChange.ServiceProviderId);

            if (eBondingAuthentication == null)
            {
                var errorMessage = "Failed to get eBonding Authentication Information.";
                LogInfo(context, "ERROR", errorMessage);

                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Carrier Rate Plan Change: Pre-flight Check",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });

                return false;
            }

            if (!eBondingAuthentication.WriteIsEnabled)
            {
                var errorMessage = "Writes disabled for this service provider.";
                LogInfo(context, "WARN", errorMessage);

                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Carrier Rate Plan Change: Pre-flight Check",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });
                return false;
            }

            var eBondingRatePlanService = GetEBondingRatePlanService(eBondingAuthentication, context.IsProduction);
            if (eBondingRatePlanService == null)
            {
                var errorMessage = "eBonding Rate Plan Service error. Unable to get Rate Plan Service.";
                LogInfo(context, "ERROR", errorMessage);

                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Carrier Rate Plan Change: Pre-flight Check",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });
                return false;
            }

            foreach (var change in changes)
            {
                var changeRequest = JsonConvert.DeserializeObject<CarrierRatePlanChange>(change.ChangeRequest);
                var res = await eBondingRatePlanService.ChangeRatePlan(Guid.NewGuid().ToString(), changeRequest);
                var isSuccessful = !res.HasErrors;
                var statusDetails = isSuccessful
                    ? $"eBonding Carrier Rate Plan update successful for {changeRequest.PhoneNumber}."
                    : $"eBonding Carrier Rate Plan update failed for {changeRequest.PhoneNumber}. {res.ResponseObject.CareOrderResponse1.StatusDescription}.";

                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = res.ResponseObject.CareOrderResponse1.StatusCode,
                    HasErrors = !isSuccessful,
                    LogEntryDescription = "Carrier Rate Plan Change: eBonding API",
                    MobilityDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = isSuccessful ? BulkChangeStatus.PROCESSED : BulkChangeStatus.ERROR,
                    RequestText = res.ActionText + Environment.NewLine + JsonConvert.SerializeObject(res.RequestObject),
                    ResponseText = res.ResponseObject != null ? JsonConvert.SerializeObject(res.ResponseObject) : statusDetails
                });

                if (isSuccessful)
                {
                    var dbResult = await UpdateEBondingDeviceCarrierRatePlan(context, changeRequest);
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                        HasErrors = !isSuccessful,
                        LogEntryDescription = "Carrier Rate Plan Change: AMOP Update",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = dbResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(dbResult.RequestObject),
                        ResponseText = dbResult.ResponseObject
                    });
                }

                await MarkProcessedForMobilityDeviceChangeAsync(context, change.Id, isSuccessful, statusDetails);
            }
            return true;
        }

        private async Task<bool> ProcessTelegenceCarrierRatePlanChange(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, int serviceProviderId, ICollection<BulkChangeDetailRecord> changes, ISyncPolicy syncPolicy)
        {
            LogInfo(context, "SUB", "ProcessTelegenceCarrierRatePlanChange()");
            var telegenceApiAuthentication =
                GetTelegenceApiAuthentication(context.CentralDbConnectionString, serviceProviderId);

            if (telegenceApiAuthentication == null)
            {
                var errorMessage = "Failed to get Telegence Authentication Information.";
                LogInfo(context, "ERROR", errorMessage);

                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Carrier Rate Plan Change: Pre-flight Check",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });

                return false;
            }

            if (!telegenceApiAuthentication.WriteIsEnabled)
            {
                var errorMessage = "Writes disabled for this service provider.";
                LogInfo(context, "WARN", errorMessage);

                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Carrier Rate Plan Change: Pre-flight Check",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });
                return false;
            }

            if (_telegenceApiPatchClient == null)
                _telegenceApiPatchClient = new TelegenceAPIClient(new SingletonHttpClientFactory(), new HttpRequestFactory(), telegenceApiAuthentication,
                    context.IsProduction, $"{ProxyUrl}/api/Proxy/Patch", context.logger);

            var httpRetryPolicy = GetHttpRetryPolicy(context);
            int.TryParse(MAX_PARALLEL_REQUEST, out var maxParallelRequests);
            if (maxParallelRequests <= 0)
            {
                maxParallelRequests = 1;
            }

            await Parallel.ForEachAsync(
                changes,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = maxParallelRequests,
                },
                async (change, _) =>
                {
                    var changeRequest = JsonConvert.DeserializeObject<CarrierRatePlanChange>(change.ChangeRequest);
                    var result = await ProcessTelegenceCarrierRatePlanUpdateAsync(changeRequest,
                        _telegenceApiPatchClient, httpRetryPolicy);

                    if (result == null)
                    {
                        LogInfo(context, "ERROR", "Failed to process Telegence Carrier Rate Plan update.");
                        await MarkProcessedForMobilityDeviceChangeAsync(context, change.Id,
                            false, "Failed to process Telegence Carrier Rate Plan update.");
                        //return false;
                    }

                    bool isSuccessful = !result.HasErrors && result.ResponseObject != null && result.ResponseObject.IsSuccessful;
                    var statusDetails = string.Format(LogCommonStrings.UPDATED_TELEGENCE_CARRIER_RATE_PLAN_SUCCESSFULLY, changeRequest.PhoneNumber);
                    if (!isSuccessful)
                    {
                        statusDetails = string.Format(LogCommonStrings.UPDATED_TELEGENCE_CARRIER_RATE_PLAN_FAILED, changeRequest.PhoneNumber, result.ResponseObject.TelegenceDeviceDetailResponse?.ErrorDescription);
                    }

                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = result.HasErrors ? JsonConvert.SerializeObject(result.ResponseObject.TelegenceDeviceDetailResponse) : null,
                        HasErrors = result.HasErrors,
                        LogEntryDescription = "Carrier Rate Plan Change: Telegence API",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = isSuccessful ? BulkChangeStatus.PROCESSED : BulkChangeStatus.ERROR,
                        RequestText = result.ActionText + Environment.NewLine + JsonConvert.SerializeObject(result.RequestObject),
                        ResponseText = result.ResponseObject != null ? JsonConvert.SerializeObject(result.ResponseObject) : statusDetails
                    });

                    if (isSuccessful)
                    {
                        var dbResult = await UpdateTelegenceDeviceCarrierRatePlan(context, changeRequest, syncPolicy);
                        logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                            HasErrors = !isSuccessful,
                            LogEntryDescription = "Carrier Rate Plan Change: AMOP Update",
                            MobilityDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                            RequestText = dbResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(dbResult.RequestObject),
                            ResponseText = dbResult.ResponseObject
                        });
                    }

                    await MarkProcessedForMobilityDeviceChangeAsync(context, change.Id, isSuccessful, statusDetails);
                });

            return true;
        }

        public virtual async Task<DeviceChangeResult<CarrierRatePlanUpdateRequest, TelegenceDeviceDetailsProxyResponse>> ProcessTelegenceCarrierRatePlanUpdateAsync(CarrierRatePlanChange changeRequest,
            TelegenceAPIClient telegenceApiClient, IAsyncPolicy httpRetryPolicy)
        {
            DeviceChangeResult<CarrierRatePlanUpdateRequest, TelegenceDeviceDetailsProxyResponse> apiResult = null;

            var carrierRatePlanUpdateUrl = TelegenceSubscriberUpdateURL + changeRequest.PhoneNumber;
            var effectiveDate = changeRequest.EffectiveDate?.ToString(CommonConstants.AMOP_UTC_DAY_TIME_FORMAT);
            if (string.IsNullOrWhiteSpace(effectiveDate))
            {
                effectiveDate = DateTime.UtcNow.ToString(CommonConstants.AMOP_UTC_DAY_TIME_FORMAT);
            }
            var carrierRatePlanUpdateRequest = new CarrierRatePlanUpdateRequest
            {
                EffectiveDate = effectiveDate,
                ServiceCharacteristicList = new List<ServiceCharacteristic>
                {
                    new ServiceCharacteristic {Name = "singleUserCode", Value = changeRequest.RatePlanName}
                }
            };
            await httpRetryPolicy.ExecuteAsync(async () =>
            {
                apiResult = await telegenceApiClient.UpdateCarrierRatePlanAsync(carrierRatePlanUpdateRequest, carrierRatePlanUpdateUrl);
            });

            return apiResult;
        }

        private async Task ProcessAssociateCustomerAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes)
        {
            context.logger.LogInfo("SUB", $"ProcessAssociateCustomerAsync()");

            //Http Retry Policy
            var httpRetryPolicy = GetHttpRetryPolicy(context);

            //Sql Retry Policy
            var sqlRetryPolicy = GetSqlTransientAsyncRetryPolicy(context);

            if (changes == null || changes.Count == 0)
            {
                context.logger.LogInfo("WARN", $"No unprocessed changes found for bulk associate customer {bulkChange.Id}");

                await sqlRetryPolicy.ExecuteAsync(async () =>
                {
                    await UpdateDeviceRevServiceLinks(context);
                });

                return;
            }
            else
            {
                context.logger.LogInfo("INFO", $"Found {changes.Count} changes to process");
            }

            var revIOAuthenticationRepository = new RevioAuthenticationRepository(context.CentralDbConnectionString, new Base64Service());
            var request = JsonConvert
                .DeserializeObject<BulkChangeAssociateCustomer>(changes.FirstOrDefault()?.ChangeRequest);
            var integrationAuthenticationId = request.IntegrationAuthenticationId;
            var effectiveDate = request.EffectiveDate;

            var revIOAuthentication = revIOAuthenticationRepository.GetRevioApiAuthentication(integrationAuthenticationId);
            var revApiClient = new RevioApiClient(new SingletonHttpClientFactory(), _httpRequestFactory, revIOAuthentication,
                    context.IsProduction);
            var sqlRetryPolicyCustomerRatePlan = GetSqlTransientRetryPolicy(context);
            int? currentCustomerRatePlanId = null;
            int? currentCustomerRatePoolId = null;

            foreach (var change in changes)
            {
                var changeRequest = JsonConvert.DeserializeObject<BulkChangeAssociateCustomer>(change.ChangeRequest);
                var apiResult = new CreateResponse();

                // Create Rev Service if opted-in 
                if (changeRequest.CreateRevService)
                {
                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.CREATING_REV_SERVICE_FOR_DEVICE, changeRequest.ICCID));
                    var createRevServiceResult = await CreateRevServiceAsync(context, logRepo, sqlRetryPolicy,
                        bulkChange, change, revApiClient, changeRequest, integrationAuthenticationId);

                    apiResult = createRevServiceResult.ResponseObject;

                    if (apiResult == null || !apiResult.Ok)
                    {
                        // Not logging to DeviceBulkChangeLogRepository b/c it is logged in CreateRevServiceAsync
                        LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.ERROR_WHILE_CREATING_REV_SERVICE_FOR_DEVICE, changeRequest.ICCID));
                    }
                }
                else
                {
                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.REV_SERVICE_CREATION_SKIPPED_FOR_DEVICE, changeRequest.ICCID));
                    // need to mark as successful, when no rev service to create
                    apiResult.Ok = true;
                }

                if (apiResult.Ok)
                {
                    int? customerRatePlanIdToSubmit = null;
                    int customerRatePlanId = 0;
                    if (int.TryParse(changeRequest.CustomerRatePlan, out customerRatePlanId))
                    {
                        customerRatePlanIdToSubmit = customerRatePlanId;
                    }

                    int? customerRatePoolIdToSubmit = null;
                    int customerRatePoolId = 0;
                    if (int.TryParse(changeRequest.CustomerRatePool, out customerRatePoolId))
                    {
                        customerRatePoolIdToSubmit = customerRatePoolId;
                    }

                    if (customerRatePlanIdToSubmit != null || customerRatePoolIdToSubmit != null)
                    {
                        context.logger.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.PROCESSING_CUSTOMER_RATE_PLAN, changeRequest.ICCID));
                        var ratePlanChangeResult = new DeviceChangeResult<string, string>();
                        if (customerRatePlanIdToSubmit != currentCustomerRatePlanId || customerRatePoolIdToSubmit != currentCustomerRatePoolId)
                        {
                            if (changeRequest.EffectiveDate == null || changeRequest.EffectiveDate?.ToUniversalTime() <= DateTime.UtcNow)
                            {
                                ratePlanChangeResult = await ProcessCustomerRatePlanChangeAsync(bulkChange.Id, customerRatePlanIdToSubmit,
                        effectiveDate, null, customerRatePoolIdToSubmit, context.CentralDbConnectionString, context.logger, sqlRetryPolicyCustomerRatePlan, false);
                            }
                            else
                            {
                                ratePlanChangeResult = await ProcessAddCustomerRatePlanChangeToQueueAsync(bulkChange, customerRatePlanIdToSubmit,
                                    changeRequest.EffectiveDate, null, customerRatePoolIdToSubmit, context);
                            }
                            currentCustomerRatePlanId = customerRatePlanIdToSubmit;
                            currentCustomerRatePoolId = customerRatePoolIdToSubmit;
                        }
                        else
                        {
                            string requestObject = string.Format(LogCommonStrings.CUSTOMER_RATE_PLAN_CHANGE_REQUEST_OBJECT, customerRatePlanId, customerRatePoolId);
                            ratePlanChangeResult = new DeviceChangeResult<string, string>()
                            {
                                ActionText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_CUSTOMER_RATE_PLAN_CHANGE_UPDATE_FOR_DEVICES,
                                HasErrors = false,
                                RequestObject = requestObject,
                                ResponseObject = CommonConstants.OK
                            };
                        }

                        if (bulkChange.PortalTypeId == PortalTypeM2M)
                        {
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(ratePlanChangeResult, bulkChange.Id, change.Id, LogCommonStrings.ASSOCIATE_CUSTOMER_UPDATE_CUSTOMER_RATE_PLAN));
                        }
                        else
                        {
                            logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog(ratePlanChangeResult, bulkChange.Id, change.Id));
                        }

                        if (ratePlanChangeResult.HasErrors)
                        {
                            apiResult.Ok = false;
                            apiResult.Error = ratePlanChangeResult.ResponseObject;
                        }
                    }
                    else if (!string.IsNullOrEmpty(changeRequest.RevCustomerId) && bulkChange.PortalTypeId == PortalTypeMobility)
                    {
                        await bulkChangeRepository.UpdateRevCustomer(context, changeRequest, bulkChange.TenantId);
                    }
                }

                await sqlRetryPolicy.ExecuteAsync(async () =>
                {
                    await MarkProcessedForRevService(context, change.Id, apiResult?.Ok != null && (bool)apiResult?.Ok,
                        JsonConvert.SerializeObject(apiResult), bulkChange.PortalTypeId);
                });
            }

            await UpdateDeviceHistoryAsync(bulkChange.Id, effectiveDate, context.CentralDbConnectionString, context.logger, sqlRetryPolicyCustomerRatePlan);

            await sqlRetryPolicy.ExecuteAsync(async () =>
            {
                await UpdateDeviceRevServiceLinks(context);
            });
        }

        private async Task<DeviceChangeResult<string, CreateResponse>> CreateRevServiceAsync(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo,
            IAsyncPolicy sqlRetryPolicy, BulkChange bulkChange, BulkChangeDetailRecord change,
            RevioApiClient revApiClient, RevServiceProductCreateModel changeRequest,
            int integrationAuthenticationId)
        {
            var revServiceProducts = changeRequest != null && changeRequest.RevProductIdList != null ? changeRequest.RevProductIdList : new List<int?>();
            context.logger.LogInfo("SUB", $"CreateRevServiceAsync(,,,,bulkChange.Id:{bulkChange.Id},change.Id:{change.Id},,changeRequest.RevProductIdList:{revServiceProducts.Count})");
            var number = !string.IsNullOrEmpty(change.MSISDN) ? change.MSISDN : change.DeviceIdentifier;
            var number2 = !string.IsNullOrEmpty(change.ICCID) ? change.ICCID : string.Empty;
            var request = new CreateServiceLineBody
            {
                CustomerId = Convert.ToInt32(changeRequest.RevCustomerId),
                Number = number,
                ServiceTypeId = changeRequest.ServiceTypeId,
                Number2 = number2
            };

            if (changeRequest.ProviderId != null)
            {
                request.ProviderId = changeRequest.ProviderId.Value;
            }

            if (changeRequest.Prorate && changeRequest.EffectiveDate != null)
            {
                request.EffectiveDate = $"{changeRequest.EffectiveDate:yyyy-MM-dd}Z";
            }

            if (changeRequest.ActivatedDate != null)
            {
                request.ActivatedDate = $"{changeRequest.ActivatedDate:yyyy-MM-dd}Z";
            }

            if (changeRequest.UsagePlanGroupId != null)
            {
                request.UsagePlanGroupId = changeRequest.UsagePlanGroupId.Value;
            }

            if (changeRequest.RevPackageId != null)
            {
                request.RevPackageId = Convert.ToInt32(changeRequest.RevPackageId);
            }

            DeviceChangeResult<string, CreateResponse> apiResult = null;

            apiResult = await revApiClient.CreateServiceLineAsync(request, (message) => { }, context.logger);

            if (bulkChange.PortalTypeId == PortalTypeM2M)
            {
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = apiResult.HasErrors ? JsonConvert.SerializeObject(apiResult.ResponseObject) : null,
                    HasErrors = apiResult.HasErrors,
                    LogEntryDescription = "Create Rev.io Service: Rev.io API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = apiResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = apiResult.ActionText + Environment.NewLine + apiResult.RequestObject,
                    ResponseText = JsonConvert.SerializeObject(apiResult.ResponseObject)
                });
            }
            else
            {
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = apiResult.HasErrors ? JsonConvert.SerializeObject(apiResult.ResponseObject) : null,
                    HasErrors = apiResult.HasErrors,
                    LogEntryDescription = "Create Rev.io Service: Rev.io API",
                    MobilityDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = apiResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = apiResult.ActionText + Environment.NewLine + apiResult.RequestObject,
                    ResponseText = JsonConvert.SerializeObject(apiResult.ResponseObject)
                });
            }

            if (!apiResult.HasErrors)
            {
                var dbResult = await SaveRevServiceAsync(context, changeRequest, integrationAuthenticationId,
                    apiResult.ResponseObject.Id, number, sqlRetryPolicy);

                if (bulkChange.PortalTypeId == PortalTypeM2M)
                {
                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                        HasErrors = dbResult.HasErrors,
                        LogEntryDescription = "Create Rev.io Service: Update AMOP",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = dbResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(dbResult.RequestObject),
                        ResponseText = dbResult.ResponseObject
                    });
                }
                else
                {
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                        HasErrors = dbResult.HasErrors,
                        LogEntryDescription = "Create Rev.io Service: Update AMOP",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = dbResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(dbResult.RequestObject),
                        ResponseText = dbResult.ResponseObject
                    });
                }

                //if we are using RevIO products, we will create the products for the service that we created before
                //if not (using RevIO package), we dont need to create the products for the service
                //because the products is included in the package
                if (changeRequest.RevPackageId == null)
                {
                    return await ProcessServiceProducts(context, logRepo, bulkChange, change, revApiClient, changeRequest,
                        apiResult.ResponseObject.Id, integrationAuthenticationId, sqlRetryPolicy);
                }
            }

            return apiResult;
        }

        private async Task<bool> ProcessCustomerAssignmentRatePlanUpdate(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, BulkChangeDetailRecord change,
            JasperAuthentication jasperAuthentication,
            EnvironmentRepository environmentRepo, IJasperDeviceDetailService jasperDeviceDetailService,
            BulkChangeAssociateCustomer changeRequest, bool writeAccountInfo)
        {
            context.logger.LogInfo("SUB", $"ProcessCustomerAssignmentRatePlanUpdate(,,,,,{writeAccountInfo})");
            if (!jasperAuthentication.WriteIsEnabled)
            {
                LogInfo(context, "WARN", "Writes disabled for service provider");
                return false;
            }

            var deviceRepository = new DeviceRepository(context.logger, environmentRepo, context.Context);
            var jasperDeviceRepository =
                new JasperDeviceRepository(context.logger, context.GeneralProviderSettings.JasperDbConnectionString);

            var jasperDeviceDetail = new JasperDeviceDetail
            {
                ICCID = changeRequest.ICCID,
                CarrierRatePlan = changeRequest.CarrierRatePlan,
                CommunicationPlan = changeRequest.CommPlan
            };

            if (writeAccountInfo)
                jasperDeviceDetail.JasperDeviceID = changeRequest.JasperDeviceID;

            jasperDeviceDetail.SiteId = changeRequest.SiteId;

            return await UpdateJasperRatePlanAsync(context, logRepo, bulkChange, change, jasperAuthentication, deviceRepository,
                jasperDeviceRepository, jasperDeviceDetailService, jasperDeviceDetail);
        }

        private async Task<DeviceChangeResult<RevServiceProductCreateModel, string>> SaveRevServiceAsync(KeySysLambdaContext context,
            RevServiceProductCreateModel bulkChange, int integrationAuthenticationId, int revServiceId, string number, IAsyncPolicy sqlRetryPolicy)
        {
            try
            {
                context.logger.LogInfo("SUB", $"SaveRevServiceAsync(,bulkChange.RevCustomerId:{bulkChange.RevCustomerId},,{revServiceId},{number},)");
                await sqlRetryPolicy.ExecuteAsync(async () =>
                {
                    await using var conn = new SqlConnection(context.CentralDbConnectionString);
                    await using var cmd =
                        new SqlCommand("usp_RevService_Create_Service", conn) { CommandType = CommandType.StoredProcedure };
                    cmd.Parameters.AddWithValue("@RevCustomerId", bulkChange.RevCustomerId);
                    cmd.Parameters.AddWithValue("@Number", number);
                    cmd.Parameters.AddWithValue("@RevServiceId", revServiceId);
                    cmd.Parameters.AddWithValue("@RevServiceTypeId", bulkChange.ServiceTypeId);
                    cmd.Parameters.AddWithValue("@RevActivatedDate", bulkChange.ActivatedDate == null ? DateTime.UtcNow : bulkChange.ActivatedDate);
                    if (bulkChange.ProviderId.HasValue)
                    {
                        cmd.Parameters.AddWithValue("@RevProviderId", bulkChange.ProviderId);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@RevProviderId", DBNull.Value);

                    }
                    cmd.Parameters.AddWithValue("@RevUsagePlanGroupId", bulkChange.UsagePlanGroupId != null ? bulkChange.UsagePlanGroupId.Value : 0);
                    cmd.Parameters.AddWithValue("@DeviceId", bulkChange.DeviceId);
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
                    conn.Open();
                    await cmd.ExecuteNonQueryAsync();
                });

                return new DeviceChangeResult<RevServiceProductCreateModel, string>()
                {
                    ActionText = "usp_RevService_Create_Service",
                    HasErrors = false,
                    RequestObject = bulkChange,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing usp_RevService_Create_Service: {ex.Message} {ex.StackTrace}");
                return new DeviceChangeResult<RevServiceProductCreateModel, string>()
                {
                    ActionText = "usp_RevService_Create_Service",
                    HasErrors = true,
                    RequestObject = bulkChange,
                    ResponseObject = $"Error Executing Stored Procedure. Ref: {logId}"
                };
            }
        }

        private static async Task MarkProcessedForRevService(KeySysLambdaContext context, long changeId, bool apiResult, string statusDetails, int portalType)
        {
            context.logger.LogInfo("SUB", $"MarkProcessedForRevService(...{apiResult}...)");
            var storedProc = portalType == PortalTypeM2M
                ? "usp_DeviceBulkChange_RevService_UpdateM2MChange"
                : "usp_DeviceBulkChange_RevService_UpdateMobilityChange";

            await using var conn = new SqlConnection(context.CentralDbConnectionString);
            await using var cmd =
                new SqlCommand(storedProc, conn)
                {
                    CommandType = CommandType.StoredProcedure
                };
            cmd.Parameters.AddWithValue("@ChangeId", changeId);
            cmd.Parameters.AddWithValue("@apiCallResult", apiResult ? 1 : 0);
            cmd.Parameters.AddWithValue("@statusDetails", statusDetails);
            conn.Open();
            await cmd.ExecuteNonQueryAsync();
        }

        private static async Task UpdateDeviceRevServiceLinks(KeySysLambdaContext context)
        {
            LogInfo(context, LogTypeConstant.Sub, "");
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.UPDATE_DEVICE_REV_SERVICE_LINKS;
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        connection.Open();

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, ex.Message);
            }
        }

        private async Task<DeviceChangeResult<string, CreateResponse>> ProcessServiceProducts(
            KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange,
            BulkChangeDetailRecord change, RevioApiClient revioApiClient,
            RevServiceProductCreateModel revServiceProduct, int serviceId, int integrationAuthenticationId, IAsyncPolicy sqlRetryPolicy)
        {
            var revServiceProducts = revServiceProduct != null && revServiceProduct.RevProductIdList != null ? revServiceProduct.RevProductIdList : new List<int?>();
            context.logger.LogInfo("SUB", $"ProcessServiceProducts(,,,,,revServiceProduct.RevProductIdList.Count:{revServiceProducts.Count},{serviceId})");
            //if multiple => new process
            if (revServiceProducts.Count > 0)
            {
                context.logger.LogInfo("INFO", "Processing multiple service products");
                var productRateDict = revServiceProducts
                    .Select((key, index) => new { key, value = revServiceProduct.RateList[index] })
                    .ToDictionary(x => x.key, x => x.value);
                DeviceChangeResult<string, CreateResponse> referenceResult = null;
                foreach (var productRate in productRateDict)
                {
                    var result = await ProcessSingleServiceProduct(context, logRepo, bulkChange, change, revioApiClient, revServiceProduct,
                    serviceId, integrationAuthenticationId, sqlRetryPolicy, productRate.Key.GetValueOrDefault(0), productRate.Value.GetValueOrDefault(0));

                    referenceResult = result;
                }
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"Process Service Products",
                    RequestObject = "Process Service Products",
                    HasErrors = false,
                    ResponseObject = referenceResult.ResponseObject
                };
            }
            else
            {
                context.logger.LogInfo("INFO", "Processing only 1 service product");
                //if single device => old process
                return await ProcessSingleServiceProduct(context, logRepo, bulkChange, change, revioApiClient, revServiceProduct,
                    serviceId, integrationAuthenticationId, sqlRetryPolicy);
            }
        }

        private async Task<DeviceChangeResult<string, CreateResponse>> ProcessSingleServiceProduct(
         KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange,
         BulkChangeDetailRecord change, RevioApiClient revioApiClient,
         RevServiceProductCreateModel revServiceProduct, int serviceId, int integrationAuthenticationId,
         IAsyncPolicy sqlRetryPolicy, int productId = 0, decimal rate = -1)
        {
            context.logger.LogInfo("SUB", $"ProcessSingleServiceProduct(,,,,,,{serviceId},,,{productId},{rate})");

            //If a product id is specified, use it, else use the one in create model
            var serviceProduct = new CreateServiceProductBody
            {
                ServiceId = serviceId,
                ProductId = productId > 0 ? productId : revServiceProduct.RevProductId.GetValueOrDefault(0),
                Rate = rate >= 0 ? Decimal.ToDouble(rate) : revServiceProduct.Rate.GetValueOrDefault(0),
                Prorate = revServiceProduct.Prorate,
                CustomerId = Convert.ToInt32(revServiceProduct.RevCustomerId),
                Quantity = 1
            };

            context.logger.LogInfo("INFO", JsonConvert.SerializeObject(serviceProduct));

            if (revServiceProduct.EffectiveDate != null)
            {
                serviceProduct.EffectiveDate = $"{revServiceProduct.EffectiveDate:yyyy-MM-dd}Z";
            }

            if (revServiceProduct.ActivatedDate != null)
            {
                serviceProduct.ActivatedDate = $"{revServiceProduct.ActivatedDate:yyyy-MM-dd}Z";
            }

            if (revServiceProduct.RevPackageId != null)
            {
                serviceProduct.RevPackageId = revServiceProduct.RevPackageId;
            }

            DeviceChangeResult<string, CreateResponse> apiResult = new DeviceChangeResult<string, CreateResponse>();

            apiResult = await revioApiClient.CreateServiceProductAsync(serviceProduct, (message) => { }, context.logger);

            if (bulkChange.PortalTypeId == PortalTypeM2M)
            {
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = apiResult.HasErrors ? JsonConvert.SerializeObject(apiResult.ResponseObject) : null,
                    HasErrors = apiResult.HasErrors,
                    LogEntryDescription = "Create Rev.io Service Product: Rev.io API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = apiResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = apiResult.ActionText + Environment.NewLine + apiResult.RequestObject,
                    ResponseText = JsonConvert.SerializeObject(apiResult.ResponseObject)
                });
            }
            else
            {
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = apiResult.HasErrors ? JsonConvert.SerializeObject(apiResult.ResponseObject) : null,
                    HasErrors = apiResult.HasErrors,
                    LogEntryDescription = "Create Rev.io Service Product: Rev.io API",
                    MobilityDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = apiResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = apiResult.ActionText + Environment.NewLine + apiResult.RequestObject,
                    ResponseText = JsonConvert.SerializeObject(apiResult.ResponseObject)
                });
            }

            if (!apiResult.ResponseObject.Ok)
            {
                context.logger.LogInfo("ERROR", $"Error creating Service product for Service Id {serviceId}");
                return apiResult;
            }

            var dbResult = await SaveRevProductAsync(context, revServiceProduct, serviceProduct, integrationAuthenticationId, serviceId,
                apiResult.ResponseObject.Id, sqlRetryPolicy);

            if (bulkChange.PortalTypeId == PortalTypeM2M)
            {
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                    HasErrors = dbResult.HasErrors,
                    LogEntryDescription = "Create Rev.io Service Product: Update AMOP",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = dbResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(dbResult.RequestObject),
                    ResponseText = dbResult.ResponseObject
                });
            }
            else
            {
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                    HasErrors = dbResult.HasErrors,
                    LogEntryDescription = "Create Rev.io Service Product: Update AMOP",
                    MobilityDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = dbResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(dbResult.RequestObject),
                    ResponseText = dbResult.ResponseObject
                });
            }

            if (dbResult.HasErrors)
            {
                apiResult.HasErrors = true;
            }

            return apiResult;
        }

        private async Task<DeviceChangeResult<RevServiceProductCreateModel, string>> SaveRevProductAsync(KeySysLambdaContext context, RevServiceProductCreateModel bulkChange, CreateServiceProductBody requestedProduct, int integrationAuthenticationId,
            int revServiceId, int revServiceProductId, IAsyncPolicy sqlRetryPolicy)
        {
            try
            {
                context.logger.LogInfo("SUB", $"SaveRevProductAsync(,,,,{revServiceId},{revServiceProductId},)");
                await sqlRetryPolicy.ExecuteAsync(async () =>
                {
                    await using var conn = new SqlConnection(context.CentralDbConnectionString);
                    await using var cmd =
                        new SqlCommand("usp_RevService_Create_ServiceProduct", conn)
                        {
                            CommandType = CommandType.StoredProcedure
                        };
                    cmd.Parameters.AddWithValue("@RevCustomerId", bulkChange.RevCustomerId);
                    cmd.Parameters.AddWithValue("@RevServiceProductId", revServiceProductId);
                    cmd.Parameters.AddWithValue("@RevProductId", requestedProduct.ProductId);
                    cmd.Parameters.AddWithValue("@RevServiceId", revServiceId);
                    SqlParameter rateParam = new SqlParameter("@Rate", SqlDbType.Decimal)
                    {
                        Precision = 25,
                        Scale = 4,
                        Value = Convert.ToDecimal(requestedProduct.Rate, CultureInfo.InvariantCulture)
                    };
                    cmd.Parameters.Add(rateParam);

                    cmd.Parameters.AddWithValue("@Description",
                        !string.IsNullOrEmpty(bulkChange.Description) ? bulkChange.Description : string.Empty);
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
                    cmd.Parameters.AddWithValue("@Status", "ACTIVE");
                    cmd.Parameters.AddWithValue("@PackageId", requestedProduct.RevPackageId ?? 0);
                    conn.Open();
                    await cmd.ExecuteNonQueryAsync();
                });

                return new DeviceChangeResult<RevServiceProductCreateModel, string>()
                {
                    ActionText = "usp_RevService_Create_ServiceProduct",
                    HasErrors = false,
                    RequestObject = bulkChange,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing usp_RevService_Create_ServiceProduct: {ex.Message} {ex.StackTrace}");
                return new DeviceChangeResult<RevServiceProductCreateModel, string>()
                {
                    ActionText = "usp_RevService_Create_ServiceProduct",
                    HasErrors = true,
                    RequestObject = bulkChange,
                    ResponseObject = $"Error Executing Stored Procedure. Ref: {logId}"
                };
            }
        }

        private static async Task ProcessCustomerRatePlanChangeAsync(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, ISyncPolicy syncPolicy)
        {
            var change = GetDeviceChanges(context, bulkChange.Id, bulkChange.PortalTypeId, 1).FirstOrDefault();
            if (change != null)
            {
                var changeRequest = JsonConvert.DeserializeObject<BulkChangeRequest>(change.ChangeRequest);
                var customerRatePlanId = changeRequest?.CustomerRatePlanUpdate?.CustomerRatePlanId;
                var customerRatePoolId = changeRequest?.CustomerRatePlanUpdate?.CustomerPoolId;
                var effectiveDate = changeRequest?.CustomerRatePlanUpdate?.EffectiveDate;
                var customerDataAllocationMB = changeRequest?.CustomerRatePlanUpdate?.CustomerDataAllocationMB;

                var dbResult = new DeviceChangeResult<string, string>();
                if (effectiveDate == null || effectiveDate?.ToUniversalTime() <= DateTime.UtcNow)
                {
                    dbResult = await ProcessCustomerRatePlanChangeAsync(bulkChange.Id, customerRatePlanId,
                        effectiveDate, customerDataAllocationMB, customerRatePoolId, context.CentralDbConnectionString, context.logger, syncPolicy);
                }
                else
                {
                    dbResult = await ProcessAddCustomerRatePlanChangeToQueueAsync(bulkChange, customerRatePlanId,
                        effectiveDate, customerDataAllocationMB, customerRatePoolId, context);
                }

                if (bulkChange.PortalTypeId == PortalTypeM2M)
                {
                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                        HasErrors = dbResult.HasErrors,
                        LogEntryDescription = "Change Customer Rate Plan: Update AMOP",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = dbResult.ActionText + Environment.NewLine + dbResult.RequestObject,
                        ResponseText = dbResult.ResponseObject
                    });
                }
                else
                {
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                        HasErrors = dbResult.HasErrors,
                        LogEntryDescription = "Change Customer Rate Plan: Update AMOP",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = dbResult.ActionText + Environment.NewLine + dbResult.RequestObject,
                        ResponseText = dbResult.ResponseObject
                    });
                }
            }
            else
            {
                context.logger.LogInfo("WARN", $"No unprocessed changes found for bulk customer rate plan change {bulkChange.Id}");
            }
        }

        private static async Task<DeviceChangeResult<string, string>> ProcessAddCustomerRatePlanChangeToQueueAsync(BulkChange bulkChange, int? customerRatePlanId, DateTime? effectiveDate, decimal? customerDataAllocationMB,
            int? customerRatePoolId, KeySysLambdaContext context)
        {
            context.logger.LogInfo("SUB", $"ProcessAddCustomerRatePlanChangeToQueueAsync({bulkChange.Id},{customerRatePlanId},{customerRatePoolId}...)");

            string requestObject = $"customerRatePlanId: {customerRatePlanId}, customerRatePoolId: {customerRatePoolId}";
            try
            {
                var changes = GetDeviceChanges(context, bulkChange.Id, bulkChange.PortalTypeId, PageSize).ToList();
                //add to datatable
                DataTable table = new DataTable();
                table.Columns.Add("Id");
                table.Columns.Add("DeviceId");
                table.Columns.Add("CustomerRatePlanId");
                table.Columns.Add("CustomerRatePoolId");
                table.Columns.Add("CustomerDataAllocationMB");
                table.Columns.Add("EffectiveDate");
                table.Columns.Add("PortalType");
                table.Columns.Add("TenantId");
                table.Columns.Add("CreatedBy");
                table.Columns.Add("CreatedDate");
                table.Columns.Add("ModifiedBy");
                table.Columns.Add("ModifiedDate");
                table.Columns.Add("IsActive");


                if (changes != null && changes.Count > 0)
                {
                    foreach (var change in changes)
                    {
                        var dr = table.NewRow();
                        dr[1] = change.DeviceId;
                        dr[2] = customerRatePlanId;
                        dr[3] = customerRatePoolId;
                        dr[4] = customerDataAllocationMB;
                        dr[5] = effectiveDate;
                        dr[6] = bulkChange.PortalTypeId;
                        dr[7] = bulkChange.TenantId;
                        dr[8] = "AWS Lambda - Device Bulk Change";
                        dr[9] = DateTime.UtcNow;
                        dr[10] = null;
                        dr[11] = null;
                        dr[12] = true;
                        table.Rows.Add(dr);
                    }
                }

                // insert to db
                SqlBulkCopy(context, context.CentralDbConnectionString, table, CustomerRatePlanDeviceQueueTable);

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = $"Insert into {CustomerRatePlanDeviceQueueTable} table",
                    HasErrors = false,
                    RequestObject = requestObject,
                    ResponseObject = $"Customer Rate Plan/Customer Rate Pool will be change in ${((DateTime)effectiveDate).ToString("yyyy-dd-MM")}"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                context.logger.LogInfo("ERROR", $"{logId} Error ProcessAddCustomerRatePlanChangeToQueueAsync: {ex.Message} {ex.StackTrace}");
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = $"Insert into {CustomerRatePlanDeviceQueueTable} table",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Error insert into {CustomerRatePlanDeviceQueueTable} table. Ref: {logId}"
                };
            }
        }

        private static async Task<DeviceChangeResult<string, string>> ProcessCustomerRatePlanChangeAsync(
            long bulkChangeId, int? customerRatePlanId, DateTime? effectiveDate, decimal? customerDataAllocationMB, int? customerRatePoolId, string connectionString, IKeysysLogger logger, ISyncPolicy syncPolicy, bool needToMarkProcess = true)
        {
            logger.LogInfo("SUB", $"ProcessCustomerRatePlanChangeAsync({bulkChangeId},{customerRatePlanId},{customerRatePoolId}...)");

            string requestObject = $"customerRatePlanId: {customerRatePlanId}, customerRatePoolId: {customerRatePoolId}";
            try
            {
                await syncPolicy.Execute(async () =>
                {
                    using (var conn = new SqlConnection(connectionString))
                    {
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_CUSTOMER_RATE_PLAN_CHANGE_UPDATE_DEVICE;
                            cmd.Parameters.AddWithValue(CommonSQLParameterNames.EFFECTIVE_DATE, effectiveDate ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue(CommonSQLParameterNames.BULK_CHANGE_ID, bulkChangeId);
                            cmd.Parameters.AddWithValue(CommonSQLParameterNames.CUSTOMER_RATE_PLAN_ID, customerRatePlanId ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue(CommonSQLParameterNames.CUSTOMER_RATE_POOL_ID, customerRatePoolId ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue(CommonSQLParameterNames.CUSTOMER_DATA_ALLOCATION_MB, customerDataAllocationMB ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue(CommonSQLParameterNames.NEED_TO_MARK_PROCESSED, needToMarkProcess);
                            cmd.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                            conn.Open();

                            await cmd.ExecuteNonQueryAsync();
                        }
                    }
                });
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDevices",
                    HasErrors = false,
                    RequestObject = requestObject,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                logger.LogInfo("ERROR", $"{logId} Error Executing usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDevices: {ex.Message} {ex.StackTrace}");
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDevices",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Error Executing Stored Procedure. Ref: {logId}"
                };
            }
        }

        private static async Task<DeviceChangeResult<string, string>> ProcessCustomerRatePlanChangeForDevicesAsync(
            long bulkChangeId, int? customerRatePlanId, decimal? customerDataAllocationMB, int? customerRatePoolId, string connectionString, IKeysysLogger logger, ISyncPolicy syncPolicy, bool needToMarkProcess = true)
        {
            logger.LogInfo(CommonConstants.SUB, $"({bulkChangeId},{customerRatePlanId},{customerRatePoolId})");

            string requestObject = string.Format(LogCommonStrings.CUSTOMER_RATE_PLAN_CHANGE_REQUEST_OBJECT, customerRatePlanId, customerRatePoolId);
            try
            {
                await syncPolicy.Execute(async () =>
                {
                    using (var connection = new SqlConnection(connectionString))
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandType = CommandType.StoredProcedure;
                            command.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_CUSTOMER_RATE_PLAN_CHANGE_UPDATE_FOR_DEVICES;
                            command.Parameters.AddWithValue(CommonSQLParameterNames.BULK_CHANGE_ID, bulkChangeId);
                            command.Parameters.AddWithValue(CommonSQLParameterNames.CUSTOMER_RATE_PLAN_ID, customerRatePlanId ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue(CommonSQLParameterNames.CUSTOMER_RATE_POOL_ID, customerRatePoolId ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue(CommonSQLParameterNames.CUSTOMER_DATA_ALLOCATION_MB, customerDataAllocationMB ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue(CommonSQLParameterNames.NEED_TO_MARK_PROCESSED, needToMarkProcess);
                            command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                            connection.Open();

                            await command.ExecuteNonQueryAsync();
                        }
                    }
                });
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_CUSTOMER_RATE_PLAN_CHANGE_UPDATE_FOR_DEVICES,
                    HasErrors = false,
                    RequestObject = requestObject,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                logger.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.ERROR_WHEN_EXECUTING_STORED_PROCEDURE, logId, ex.Message, ex.StackTrace));
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_CUSTOMER_RATE_PLAN_CHANGE_UPDATE_FOR_DEVICES,
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = string.Format(LogCommonStrings.ERROR_WHEN_EXECUTING_STORED_PROCEDURE, logId, ex.Message, ex.StackTrace)
                };
            }
        }

        private static async Task UpdateDeviceHistoryAsync(long bulkChangeId, DateTime? effectiveDate, string connectionString, IKeysysLogger logger, ISyncPolicy syncPolicy)
        {
            try
            {
                await syncPolicy.Execute(async () =>
                {
                    using (var connection = new SqlConnection(connectionString))
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandType = CommandType.StoredProcedure;
                            command.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_UPDATE_DEVICE_HISTORY;
                            command.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                            command.Parameters.AddWithValue("@effectiveDate", effectiveDate ?? (object)DBNull.Value);
                            command.CommandTimeout = Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds;
                            connection.Open();

                            await command.ExecuteNonQueryAsync();
                        }
                    }
                });
            }
            catch (SqlException ex)
            {
                logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                logger.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                logger.LogInfo(CommonConstants.EXCEPTION, ex.Message);
            }
        }

        public static async Task<DeviceChangeResult<string, string>> ProcessCustomerRatePlanChangeBySubNumberAsync(
            long bulkChangeId, string subscriberNumber, int? customerRatePlanId, int? customerRatePoolId, DateTime? effectiveDate, decimal? customerDataAllocationMB, string connectionString, IKeysysLogger logger)
        {
            logger.LogInfo("SUB", $"ProcessCustomerRatePlanChangeBySubNumberAsync({bulkChangeId},{subscriberNumber},{customerRatePlanId},{customerRatePoolId}...)");

            string requestObject = $"customerRatePlanId: {customerRatePlanId}, customerRatePoolId: {customerRatePoolId}";
            try
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "dbo.usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDeviceByNumber";
                        cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                        cmd.Parameters.AddWithValue("@subscriberNumber", subscriberNumber);
                        cmd.Parameters.AddWithValue("@customerRatePlanId", customerRatePlanId ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@customerRatePoolId", customerRatePoolId ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@effectiveDate", effectiveDate ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@customerDataAllocationMB", customerDataAllocationMB ?? (object)DBNull.Value);

                        conn.Open();

                        await cmd.ExecuteNonQueryAsync();
                    }
                }

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDeviceByNumber",
                    HasErrors = false,
                    RequestObject = requestObject,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                logger.LogInfo("ERROR", $"{logId} Error Executing usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDeviceByNumber: {ex.Message} {ex.StackTrace}");
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDeviceByNumber",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Error Executing Stored Procedure. Ref: {logId}"
                };
            }
        }

        private static async Task ProcessArchivalAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            long bulkChangeId, ICollection<BulkChangeDetailRecord> changes)
        {
            LogInfo(context, "SUB", $"ProcessArchivalAsync({bulkChangeId},...)");

            var bulkChange = GetBulkChange(context, bulkChangeId);
            if (changes.Count > 0)
            {
                var change = changes.First();

                DeviceChangeResult<string, string> dbResult;
                try
                {
                    var changeIdsString = string.Join(",", changes.Select(x => x.Id).ToList());
                    using (var conn = new SqlConnection(context.CentralDbConnectionString))
                    {
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandText = "dbo.usp_DeviceBulkChange_Archival_ArchiveDevices";
                            cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                            cmd.Parameters.AddWithValue("@changeIds", changeIdsString);
                            cmd.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                            conn.Open();

                            await cmd.ExecuteNonQueryAsync();
                        }
                    }

                    dbResult = new DeviceChangeResult<string, string>()
                    {
                        ActionText = "usp_DeviceBulkChange_Archival_ArchiveDevices",
                        HasErrors = false,
                        RequestObject = $"bulkChangeId: {bulkChangeId}",
                        ResponseObject = "OK"
                    };
                }
                catch (Exception ex)
                {
                    var logId = Guid.NewGuid();
                    LogInfo(context, "ERROR", $"{logId} Error Executing usp_DeviceBulkChange_Archival_ArchiveDevices: {ex.Message} {ex.StackTrace}");
                    dbResult = new DeviceChangeResult<string, string>()
                    {
                        ActionText = "usp_DeviceBulkChange_Archival_ArchiveDevices",
                        HasErrors = true,
                        RequestObject = $"bulkChangeId: {bulkChangeId}",
                        ResponseObject = $"Error Executing Stored Procedure. Ref: {logId}"
                    };
                }

                if (bulkChange.PortalTypeId == PortalTypeM2M)
                {
                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                        HasErrors = dbResult.HasErrors,
                        LogEntryDescription = "Archive Devices: Update AMOP",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = dbResult.ActionText + Environment.NewLine + dbResult.RequestObject,
                        ResponseText = dbResult.ResponseObject
                    });
                }
                else if (bulkChange.PortalTypeId == PortalTypeMobility)
                {
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                        HasErrors = dbResult.HasErrors,
                        LogEntryDescription = "Archive Devices: Update AMOP",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = dbResult.ActionText + Environment.NewLine + dbResult.RequestObject,
                        ResponseText = dbResult.ResponseObject
                    });
                }
                else
                {
                    logRepo.AddLNPLogEntry(new CreateLNPDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                        HasErrors = dbResult.HasErrors,
                        LogEntryDescription = "Archive Devices: Update AMOP",
                        LNPDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = dbResult.ActionText + Environment.NewLine + dbResult.RequestObject,
                        ResponseText = dbResult.ResponseObject
                    });
                }
            }
        }

        private async Task<bool> ProcessStatusUpdateAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes, int retryNumber)
        {
            var result = false;
            var revIOAuthenticationRepository = new RevioAuthenticationRepository(context.CentralDbConnectionString, new Base64Service());
            StatusUpdateRequest<dynamic> statusUpdateRequest;
            if (changes != null && changes.Count > 0)
            {
                statusUpdateRequest =
                    JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(changes.FirstOrDefault()?.ChangeRequest);
            }
            else
            {
                // empty list to process
                LogInfo(context, LogTypeConstant.Warning, $"No unprocessed changes found for status change {bulkChange.Id}");
                return true;
            }

            var integrationAuthenticationId = 0;
            if (statusUpdateRequest.RevService != null)
            {
                integrationAuthenticationId = statusUpdateRequest.RevService.IntegrationAuthenticationId;
            }
            if (statusUpdateRequest.RevServiceProductCreateModel != null)
            {
                integrationAuthenticationId = statusUpdateRequest.IntegrationAuthenticationId;
            }

            var revIOAuthentication = revIOAuthenticationRepository.GetRevioApiAuthentication(integrationAuthenticationId);
            var revApiClient = new RevioApiClient(new SingletonHttpClientFactory(), _httpRequestFactory, revIOAuthentication,
                context.IsProduction);

            //Http Retry Policy
            var httpRetryPolicy = GetHttpRetryPolicy(context);

            //Sql Retry Policy
            var sqlRetryPolicy = GetSqlTransientAsyncRetryPolicy(context);
            switch (bulkChange.IntegrationId)
            {
                case (int)IntegrationType.eBonding:
                    await EnqueueDeviceBulkChangesAsync(context, bulkChange.Id, eBondingDeviceStatusChangeQueueUrl, SQS_SHORT_DELAY_SECONDS, retryNumber);
                    break;
                case (int)IntegrationType.ThingSpace:
                    result = await ProcessThingSpaceStatusUpdateAsync(context, logRepo, bulkChange, changes,
                        httpRetryPolicy, sqlRetryPolicy, revApiClient, integrationAuthenticationId);
                    break;
                case (int)IntegrationType.Jasper:
                case (int)IntegrationType.POD19:
                case (int)IntegrationType.TMobileJasper:
                case (int)IntegrationType.Rogers:
                    result = await ProcessJasperStatusUpdateAsync(context, logRepo, bulkChange, changes,
                        httpRetryPolicy, sqlRetryPolicy, revApiClient, integrationAuthenticationId);
                    break;
                case (int)IntegrationType.Telegence:
                    result = await ProcessTelegenceStatusUpdateAsync(context, logRepo, bulkChange, changes,
                        httpRetryPolicy, sqlRetryPolicy, revApiClient, integrationAuthenticationId);
                    break;
                case (int)IntegrationType.Teal:
                    result = await ProcessTealStatusUpdateAsync(context, logRepo, bulkChange, changes,
                        httpRetryPolicy, sqlRetryPolicy, revApiClient, integrationAuthenticationId);
                    break;
                case (int)IntegrationType.Pond:
                    result = await ProcessPondStatusUpdateAsync(context, logRepo, bulkChange, changes,
                        httpRetryPolicy, sqlRetryPolicy, revApiClient, integrationAuthenticationId);
                    break;
                default:
                    throw new Exception($"Error Process Status Change {bulkChange.Id}: Integration Type {bulkChange.IntegrationId} is unsupported.");
            }

            return result;
        }

        private async Task ProcessRevServiceCreation<T>(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            IAsyncPolicy httpRetryPolicy, IAsyncPolicy sqlRetryPolicy, RevioApiClient revApiClient,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes, int integrationAuthenticationId)
        {
            foreach (var change in changes)
            {
                var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<T>>(change.ChangeRequest);
                if (changeRequest.RevService == null && changeRequest.RevServiceProductCreateModel == null)
                    continue;

                if (string.IsNullOrEmpty(change.DeviceIdentifier))
                    continue;

                if (changeRequest.RevService != null)
                {
                    var response = await CreateRevServiceAsync(context, logRepo, sqlRetryPolicy,
                        bulkChange, change, revApiClient,
                        changeRequest.RevService, integrationAuthenticationId);

                    if (response == null || response.HasErrors)
                    {
                        context.logger.LogInfo("ERROR", $"Error Creating Service Line for {changeRequest.RevService.Number}");
                    }
                }

                if (changeRequest.RevServiceProductCreateModel != null)
                {
                    var response = await CreateRevServiceAsync(context, logRepo, sqlRetryPolicy,
                        bulkChange, change, revApiClient,
                        changeRequest.RevServiceProductCreateModel, integrationAuthenticationId);

                    if (response == null || response.HasErrors)
                    {
                        context.logger.LogInfo("ERROR", $"Error Creating Service Line for {changeRequest.RevService.Number}");
                    }
                }

            }
        }

        private async Task<bool> ProcessNewServiceActivationAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, SQSEvent.SQSMessage message, long additionBulkChangeId, int retryNumber)
        {
            LogInfo(context, LogTypeConstant.Sub, "ProcessNewServiceActivationAsync()");
            LogInfo(context, LogTypeConstant.Info, $"Bulk Change Id: {bulkChange.Id}");

            switch (bulkChange.IntegrationId)
            {
                case (int)IntegrationType.Telegence:
                    return await ProcessTelegenceNewServiceActivationStepsAsync(context, logRepo, bulkChange, message, additionBulkChangeId, retryNumber);
                default:
                    throw new Exception($"Error Activating New Service {bulkChange.Id}: Integration Type {bulkChange.IntegrationId} is unsupported.");
            }
        }

        private async Task<bool> ProcessTelegenceNewServiceActivationStepsAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, SQSEvent.SQSMessage message, long additionBulkChangeId, int retryNumber)
        {
            LogInfo(context, LogTypeConstant.Sub, "Start Process Telegence New Service Activation Steps Async");

            long processStep = 0;

            if (message.MessageAttributes.ContainsKey("TelegenceNewServiceActivationStep") && !string.IsNullOrWhiteSpace(message.MessageAttributes["TelegenceNewServiceActivationStep"].StringValue))
            {
                processStep = long.Parse(message.MessageAttributes["TelegenceNewServiceActivationStep"].StringValue);
            }
            LogInfo(context, LogTypeConstant.Info, $"Telegence New Activation Step: {processStep}");

            switch (processStep)
            {
                case (int)TelegenceNewActivationStep.ActivateDevice:
                    var changes = GetDeviceChanges(context, bulkChange.Id, bulkChange.PortalTypeId, PageSize);

                    if (changes == null || changes.Count == 0)
                    {
                        // empty list to process
                        LogInfo(context, LogTypeConstant.Warning, $"No unprocessed changes found for new service activation {bulkChange.Id}");
                        return true;
                    }

                    return await ProcessTelegenceNewServiceActivationsAsync(context, logRepo, bulkChange, changes, additionBulkChangeId, retryNumber);
                case (int)TelegenceNewActivationStep.RequestIPProvision:
                    return await ProcessTelegenceStaticIPProvisioning(context, bulkChange, message);
                case (int)TelegenceNewActivationStep.CheckIPProvisionStatus:
                    return await ProcessTelegenceCheckIPProvision(context, bulkChange, message);
                //possible refactor to move activation request check here
                default:
                    throw new Exception($"Error Activating New Service {bulkChange.Id}: Integration Type {bulkChange.IntegrationId} is unsupported.");
            }
        }

        private async Task<bool> ProcessTealStatusUpdateAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes, IAsyncPolicy httpRetryPolicy, IAsyncPolicy sqlRetryPolicy,
            RevioApiClient revApiClient, int integrationAuthenticationId)
        {
            var tealRepository = new TealRepository(context);
            var tealAuthentication = tealRepository.GetTealAuthenticationInformation(bulkChange.ServiceProviderId);
            if (tealAuthentication != null)
            {
                if (tealAuthentication.WriteIsEnabled)
                {
                    var tealAPIService = new TealAPIService(tealAuthentication, new Base64Service(), new SingletonHttpClientFactory(), new HttpRequestFactory());
                    foreach (var change in changes)
                    {
                        if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                        {
                            return true;
                        }
                        // Use StatusUpdateRequest<dynamic> here because we don't need to use Request in this function, so don't have specific type for it 
                        var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(change.ChangeRequest);
                        var url = "";
                        switch (changeRequest.UpdateStatus.ToLower())
                        {
                            case CommonConstants.TEAL_ONLINE_STATUS:
                            case CommonConstants.TEAL_WAITING_STATUS:
                                url = URLConstants.TEAL_ENABLE_DEVICE;
                                break;
                            case CommonConstants.TEAL_STOPPED_STATUS:
                                url = URLConstants.TEAL_DISABLE_DEVICE;
                                break;
                            default:
                                context.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.DEVICE_STATUS_OF_ICCID_WAS_NOT_UPDATED_UNSUPPORTED_STATUS, change.ICCID, changeRequest.UpdateStatus));
                                break;
                        }
                        if (!string.IsNullOrWhiteSpace(url))
                        {
                            var deviceRepository = new DeviceRepository(context.logger, new EnvironmentRepository(), context.Context);
                            var deviceData = deviceRepository.Get(change.ICCID, change.TenantId);
                            var currentStatus = deviceData.DeviceStatusName.ToLower();
                            var apiResult = new DeviceChangeResult<string, string>();
                            var tealRequest = new TealAPIRequest<string>() { Entries = new List<string>() { deviceData?.EID } };
                            var tealRequestJson = JsonConvert.SerializeObject(tealRequest);
                            if (currentStatus == CommonConstants.TEAL_ONLINE_STATUS || currentStatus == CommonConstants.TEAL_WAITING_STATUS || currentStatus == CommonConstants.TEAL_STOPPED_STATUS)
                            {
                                apiResult = await tealAPIService.ProcessTealUpdateAsync(URLConstants.TEAL_DISABLE_DEVICE, TealHelper.CommonString.TEAL_UPDATE_DEVICE_STATUS, tealRequestJson, context.logger);
                                if (apiResult?.HasErrors == false)
                                {
                                    await ProcessRevServiceCreation<dynamic>(context, logRepo, httpRetryPolicy, sqlRetryPolicy,
                                        revApiClient, bulkChange, new List<BulkChangeDetailRecord>() { change }, integrationAuthenticationId);
                                }
                            }
                            else
                            {
                                context.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.DEVICE_STATUS_OF_ICCID_WAS_NOT_UPDATED_UNSUPPORTED_STATUS, change.ICCID, deviceData.DeviceStatusName));
                                apiResult = new DeviceChangeResult<string, string>()
                                {
                                    ActionText = TealHelper.CommonString.TEAL_UPDATE_DEVICE_STATUS,
                                    HasErrors = true,
                                    RequestObject = tealRequestJson,
                                    ResponseObject = string.Format(LogCommonStrings.DEVICE_STATUS_OF_ICCID_WAS_NOT_UPDATED_UNSUPPORTED_STATUS, change.ICCID, deviceData.DeviceStatusName)
                                };
                            }
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(apiResult, bulkChange.Id, change.Id, apiResult?.ResponseObject));
                            MarkProcessed(context, bulkChange.Id, change.Id, !apiResult?.HasErrors ?? false, changeRequest.PostUpdateStatusId, apiResult?.ResponseObject);
                        }
                    }
                    return true;
                }
                else
                {
                    context.LogInfo(CommonConstants.WARNING, string.Format(LogCommonStrings.DEVICE_STATUS_OF_ICCID_WAS_NOT_UPDATED_WRITE_IS_NOT_ENABLED, changes.Select(x => x.ICCID).ToString()));
                    return false;
                }
            }
            else
            {
                context.LogInfo(CommonConstants.WARNING, string.Format(LogCommonStrings.DEVICE_STATUS_OF_ICCID_WAS_NOT_UPDATED_WRITE_IS_NOT_ENABLED, changes.Select(x => x.ICCID).ToString()));
                return false;
            }
        }

        private async Task<bool> ProcessPondStatusUpdateAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes, IAsyncPolicy httpRetryPolicy, IAsyncPolicy sqlRetryPolicy, RevioApiClient revApiClient, int integrationAuthenticationId)
        {
            var environmentRepo = new EnvironmentRepository();
            var httpClientFactory = new KeysysHttpClientFactory();
            var pondRepository = new PondRepository(context.CentralDbConnectionString, context.logger);
            var base64Service = new Base64Service();
            var pondAuthentication = pondRepository.GetPondAuthentication(ParameterizedLog(context), base64Service, bulkChange.ServiceProviderId);

            var emailFactory = new SimpleEmailServiceFactory();
            using var client = emailFactory.getClient(AwsSesCredentials(context), RegionEndpoint.USEast1);
            var awsEnv = context.EnvironmentRepo.GetEnvironmentVariable(context.Context, "AWSEnv");
            var emailSender = new EmailSender(client, context.logger, awsEnv);

            var pondApiService = new PondApiService(pondAuthentication, _httpRequestFactory, context.IsProduction);
            var deviceRepository = new DeviceRepository(context.logger, environmentRepo, context.Context);

            if (!pondAuthentication.WriteIsEnabled)
            {
                string message = string.Format(LogCommonStrings.WRITE_IS_DISABLED_FOR_SERVICE_PROVIDER_ID, bulkChange.ServiceProviderId);
                LogInfo(context, CommonConstants.WARNING, message);

                var change = changes.First();
                var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(change.ChangeRequest);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = message,
                    HasErrors = true,
                    LogEntryDescription = LogCommonStrings.UPDATE_POND_STATUS_WITH_POND_API,
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = LogCommonStrings.ALTAWORX_DEVICE_BULK_CHANGE,
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = JsonConvert.SerializeObject(change),
                    ResponseText = message
                });

                // Mark item processed
                MarkProcessed(context, bulkChange.Id, change.Id, false, changeRequest.PostUpdateStatusId, message);

                return false;
            }

            foreach (var change in changes)
            {
                if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                {
                    // Processing should continue, we just need to requeue
                    return true;
                }

                var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(change.ChangeRequest);
                var iccid = change.DeviceIdentifier;

                PondUpdateServiceStatusRequest updateStatsusRequest;
                if (changeRequest.UpdateStatus == DeviceStatusConstant.POND_ACTIVE)
                {
                    // Enable all service status
                    updateStatsusRequest = new PondUpdateServiceStatusRequest();
                }
                else
                {
                    // Disable all service status
                    updateStatsusRequest = new PondUpdateServiceStatusRequest(false);
                }

                // Handle update device status here before update service statuses
                // Steps:
                // 1. Update device status
                // 2. Check has error or not
                // 3. Add log by logRepo.AddM2MLogEntry

                // Handle update service statuses
                var updateServiceStatusResult = await pondApiService.UpdateServiceStatus(httpClientFactory.GetClient(), iccid, updateStatsusRequest, context.logger);

                string responseStatus = BulkChangeStatus.PROCESSED;

                if (updateServiceStatusResult.HasErrors)
                {
                    responseStatus = BulkChangeStatus.ERROR;
                }

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = updateServiceStatusResult.ResponseObject,
                    HasErrors = updateServiceStatusResult.HasErrors,
                    LogEntryDescription = LogCommonStrings.UPDATE_POND_STATUS_WITH_POND_API,
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = LogCommonStrings.ALTAWORX_DEVICE_BULK_CHANGE,
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = responseStatus,
                    RequestText = updateServiceStatusResult.ActionText + Environment.NewLine + updateServiceStatusResult.RequestObject,
                    ResponseText = updateServiceStatusResult.ResponseObject
                });

                // Check if the device status and the service status are updated successfully before handle next processes
                // Process rev service
                if (!updateServiceStatusResult.HasErrors)
                {
                    await ProcessRevServiceCreation<ThingSpaceStatusUpdateRequest>(context, logRepo, httpRetryPolicy, sqlRetryPolicy,
                        revApiClient, bulkChange, new List<BulkChangeDetailRecord>() { change }, integrationAuthenticationId);
                }

                // Mark item processed
                MarkProcessed(context, bulkChange.Id, change.Id, !updateServiceStatusResult.HasErrors, changeRequest.PostUpdateStatusId,
                    updateServiceStatusResult.ResponseObject);
            }
            return true;
        }

        //Update
        private async Task<bool> ProcessTelegenceStatusUpdateAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes, IAsyncPolicy httpRetryPolicy, IAsyncPolicy sqlRetryPolicy,
            RevioApiClient revApiClient, int integrationAuthenticationId)
        {
            //Update : Declared Vaiable 
            bool allSuccessful = true;
            var telegenceAuthenticationInfo =
                TelegenceCommon.GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, bulkChange.ServiceProviderId);
            if (telegenceAuthenticationInfo != null)
            {
                if (telegenceAuthenticationInfo.WriteIsEnabled)
                {
                    var telegenceAuthentication = telegenceAuthenticationInfo;
                    List<TelegenceActivationRequest> telegenceActivationRequests = new List<TelegenceActivationRequest>();
                    int statusId = 0;
                    foreach (var change in changes)
                    {
                        if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                        {
                            // processing should continue, we just need to requeue
                            // Update: Ensure DeviceBulkChange status remains Processing if requeued to avoid leaving it stale
                            await sqlRetryPolicy.ExecuteAsync(() => bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChange.Id, "PROCESSING"));
                            return true;
                        }

                        var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(change.ChangeRequest);
                        if (changeRequest.UpdateStatus == "A")
                        {
                            var activationRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<TelegenceActivationRequest>>(change.ChangeRequest).Request;
                            telegenceActivationRequests.Add(activationRequest);
                            statusId = changeRequest.PostUpdateStatusId;
                        }
                        else
                        {
                            var updateRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<TelegenceSubscriberUpdateRequest>>(change.ChangeRequest).Request;
                            var apiResult = await UpdateTelegenceSubscriberAsync(context.logger, logRepo, bulkChange, change,
                                new Base64Service(), telegenceAuthentication, context.IsProduction, updateRequest,
                                change.DeviceIdentifier, TelegenceSubscriberUpdateURL, ProxyUrl);

                            // Update: Track API failure to set bulk change status to Error if any call fails
                            if (apiResult?.HasErrors == true)
                            {
                                allSuccessful = false;
                            }

                            // process rev service
                            if (apiResult?.HasErrors == false)
                            {
                                await ProcessRevServiceCreation<dynamic>(context, logRepo, httpRetryPolicy, sqlRetryPolicy,
                                    revApiClient, bulkChange, new List<BulkChangeDetailRecord>() { change }, integrationAuthenticationId);
                            }

                            MarkProcessed(context, bulkChange.Id, change.Id, !apiResult?.HasErrors ?? false, changeRequest.PostUpdateStatusId,
                                apiResult?.ResponseObject?.Response);
                        }
                    }

                    if (telegenceActivationRequests.Count > 0)
                    {
                        var apiResult = await UpdateTelegenceDeviceStatusAsync(context.logger, logRepo, bulkChange, changes.First(),
                            new Base64Service(), telegenceAuthentication, context.IsProduction, telegenceActivationRequests,
                            TelegenceDeviceStatusUpdateURL, ProxyUrl);
                        foreach (var change in changes)
                        {

                            // Update: Track API failure for activation requests to set bulk change status to Error if any call fails
                            if (apiResult?.HasErrors == true)
                            {
                                allSuccessful = false;
                            }


                            // process new rev service
                            if (apiResult?.HasErrors == false)
                            {
                                await ProcessRevServiceCreation<dynamic>(context, logRepo, httpRetryPolicy, sqlRetryPolicy,
                                    revApiClient, bulkChange, new List<BulkChangeDetailRecord>() { change }, integrationAuthenticationId);
                            }

                            MarkProcessed(context, bulkChange.Id, change.Id, !apiResult?.HasErrors ?? false,
                                statusId, apiResult?.ResponseObject?.Response);
                        }
                    }

                    // Update: Call MarkBulkChangeStatusAsync to update DeviceBulkChange status to Processed or Error based on API results
                    string finalStatus = allSuccessful ? "PROCESSED" : "ERROR";
                    await sqlRetryPolicy.ExecuteAsync(() => bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChange.Id, finalStatus));
                    return allSuccessful;

                    //return true;
                }
                else
                {
                    LogInfo(context, "WARN", "Writes disabled for this service provider.");
                    // Update: Set DeviceBulkChange status to Error if writes are disabled
                    await sqlRetryPolicy.ExecuteAsync(() => bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChange.Id, ""));
                    return false;
                }
            }
            else
            {
                var change = changes.First();

                string errorMessage = $"Error Sending {bulkChange.Id}: Failed to get Telegence Authentication Information.";
                LogInfo(context, "ERROR", errorMessage);
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Telegence Status Update: Telegence API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    RequestText = change.ChangeRequest,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    ResponseText = errorMessage
                });

                // Update: Set DeviceBulkChange status to Error if authentication fails
                await sqlRetryPolicy.ExecuteAsync(() => bulkChangeRepository.MarkBulkChangeStatusAsync(context, bulkChange.Id, "Error"));

                return false;
            }
        }

        private async Task<bool> ProcessTelegenceNewServiceActivationsAsync(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes, long additionBulkChangeId, int retryNumber)
        {
            var serviceProviderId = bulkChange.ServiceProviderId;
            LogInfo(context, LogTypeConstant.Info, $"Telegence: Processing New Service Activation for Service Provider: {serviceProviderId}");
            var telegenceApiAuthentication =
                GetTelegenceApiAuthentication(context.CentralDbConnectionString, serviceProviderId);

            if (telegenceApiAuthentication == null)
            {
                var errorMessage = $"Unable to get Telegence API Authentication for Service Provider: {serviceProviderId}";
                LogInfo(context, LogTypeConstant.Error, errorMessage);

                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Telegence New Service Activation: Pre-flight Check",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });

                // mark item processed
                await MarkProcessedForNewServiceActivationAsync(context, bulkChange.Id, false, errorMessage, null);
                if (additionBulkChangeId > 0)
                {
                    await bulkChangeRepository.MarkBulkChangeStatusAsync(context, additionBulkChangeId, BulkChangeStatus.PROCESSED);
                }

                return false;
            }

            if (!telegenceApiAuthentication.WriteIsEnabled)
            {
                var errorMessage = $"Write is disabled for Service Provider: {serviceProviderId}";
                LogInfo(context, LogTypeConstant.Error, errorMessage);

                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Telegence New Service Activation: Pre-flight Check",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = firstChange.ChangeRequest,
                    ResponseText = errorMessage
                });

                // mark item processed
                await MarkProcessedForNewServiceActivationAsync(context, bulkChange.Id, false, errorMessage, null);
                if (additionBulkChangeId > 0)
                {
                    await bulkChangeRepository.MarkBulkChangeStatusAsync(context, additionBulkChangeId, BulkChangeStatus.PROCESSED);
                }
                return false;
            }

            var telegenceActivationListItems = new List<TelegenceActivationRequest>();
            var carrierDataGroup = string.Empty;
            var carrierRatePool = string.Empty;
            foreach (var change in changes)
            {
                LogInfo(context, LogTypeConstant.Info, $"Processing Change: {change.Id} with change request: {change.ChangeRequest}");
                var telegenceChangeRequest = JsonConvert.DeserializeObject<TelegenceActivationChangeRequest>(change.ChangeRequest);

                telegenceActivationListItems.Add(telegenceChangeRequest.TelegenceActivationRequest);
                carrierDataGroup = telegenceChangeRequest.CarrierDataGroup;
                carrierRatePool = telegenceChangeRequest.CarrierRatePool;
            }

            if (_telegenceApiPostClient == null)
                _telegenceApiPostClient = new TelegenceAPIClient(telegenceApiAuthentication,
                    context.IsProduction, $"{ProxyUrl}/api/Proxy/Post", context.logger);
            using (var httpClient = new HttpClient(new LambdaLoggingHandler()))
            {
                //Init httpclient
                httpClient.BaseAddress = new Uri($"{ProxyUrl}/api/Proxy/Post");

                DeviceChangeResult<List<TelegenceActivationRequest>, TelegenceActivationProxyResponse> apiResult = null;

                var isAllBatchesSuccess = true;
                var activationListInBatches = telegenceActivationListItems.SplitCollection(MAX_TELEGENCE_SERVICES_PER_REQUEST);
                int index = 0;
                //Split in batches as API only allow maximum of 200 service activations per request
                foreach (var telegenceActivationList in activationListInBatches)
                {
                    LogInfo(context, CommonConstants.INFO, $"Processing batch {index} of {activationListInBatches.Count()}");
                    var httpRetryPolicy = GetHttpRetryPolicy(context);

                    //filter remove ServiceCharacteristic("rmeove offering code")
                    var tlActivationListSliceRemoveOfferingCode = telegenceActivationList.Select(t => new TelegenceActivationRequest()
                    {
                        BillingAccount = t.BillingAccount,
                        RelatedParty = t.RelatedParty,
                        Service = new Amop.Core.Models.Telegence.Api.Service()
                        {
                            Category = t.Service.Category,
                            Error = t.Service.Error,
                            Name = t.Service.Name,
                            ServiceSpecification = t.Service.ServiceSpecification,
                            ServiceCharacteristic = t.Service.ServiceCharacteristic.Where(t => !t.Name.Equals(Common.CommonString.REMOVE_SOC_CODE_STRING)).ToList(),
                            Status = t.Service.Status,
                            SubscriberNumber = t.Service.SubscriberNumber,
                            ServiceQualification = t.Service.ServiceQualification
                        }
                    }).ToList();

                    await httpRetryPolicy.ExecuteAsync(async () =>
                    {
                        apiResult = await _telegenceApiPostClient.ActivateDevicesAsync(tlActivationListSliceRemoveOfferingCode, TelegenceDeviceStatusUpdateURL, httpClient);
                    });

                    var isSuccessful = !apiResult?.HasErrors ?? false;
                    if (!isSuccessful)
                    {
                        isAllBatchesSuccess = false;
                        LogVariableValue(context, nameof(apiResult.ResponseObject.ErrorMessage), apiResult.ResponseObject.ErrorMessage);
                    }

                    LogVariableValue(context, nameof(isSuccessful), isSuccessful);
                    LogVariableValue(context, nameof(isAllBatchesSuccess), isAllBatchesSuccess);

                    //logging into each change based on iccid
                    foreach (var requestedDevice in telegenceActivationList)
                    {
                        var iccid = requestedDevice.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "sim")?.Value;

                        //create bulkchange log activate new service 
                        logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = apiResult.HasErrors ? JsonConvert.SerializeObject(apiResult.ResponseObject.TelegenceActivationResponse) : null,
                            HasErrors = apiResult.HasErrors,
                            LogEntryDescription = "Telegence New Service Activation: Telegence API",
                            MobilityDeviceChangeId = (long)(changes.FirstOrDefault(ch => ch.ICCID == iccid)?.Id),
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            ResponseStatus = isSuccessful ? BulkChangeStatus.PENDING : BulkChangeStatus.ERROR,
                            RequestText = apiResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(apiResult.RequestObject),
                            ResponseText = apiResult.ResponseObject != null ? JsonConvert.SerializeObject(apiResult.ResponseObject) : string.Empty
                        });
                    }

                    // apiResult.ResponseObject.TelegenceActivationResponse is the list of devices in the request
                    var activationResponse = apiResult?.ResponseObject?.TelegenceActivationResponse != null ? JsonConvert.SerializeObject(apiResult.ResponseObject.TelegenceActivationResponse) : string.Empty;
                    var statusText = isSuccessful ? BulkChangeStatus.PENDING : BulkChangeStatus.ERROR;


                    //get iccid list of activated services
                    var iccidList = new List<string>();
                    telegenceActivationList.ForEach(activation => iccidList.Add(activation.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "sim")?.Value));
                    var relatedParty = telegenceActivationList.FirstOrDefault()?.RelatedParty;
                    var firstName = relatedParty.FirstOrDefault()?.FirstName;
                    var lastName = relatedParty.FirstOrDefault()?.LastName;
                    string userName = null;
                    if (firstName != null && lastName != null)
                    {
                        userName = firstName + " " + lastName;
                    }
                    //this mark each change in one bulk change as processed / error
                    // should be done once for each iccid/device
                    //Idea: use activation list in stored procedure to mark all related or use only
                    await MarkProcessedForNewServiceActivationAsync(context, bulkChange.Id, isSuccessful, activationResponse, iccidList, serviceProviderId, userName);

                    index++;
                }
                //Send message to start the check process for service activation info
                if (isAllBatchesSuccess)
                {
                    await EnqueueDeviceBulkChangesAsync(context, bulkChange.Id, DeviceBulkChangeQueueUrl, SQS_SHORT_DELAY_SECONDS, retryNumber, true,
                        serviceProviderId, carrierRatePool, carrierDataGroup, 1, additionBulkChangeId);
                }
                else
                {
                    var iccidList = new List<string>();
                    var additionBulkChangeDetails = GetDeviceChanges(context, additionBulkChangeId, PortalTypeMobility, int.MaxValue, false);
                    if (additionBulkChangeDetails != null && additionBulkChangeDetails.Count > 0)
                    {
                        iccidList.AddRange(additionBulkChangeDetails.Select(x => x.ICCID));
                        var message = string.Format(LogCommonStrings.ASSIGN_CUSTOMER_FAILED_BECAUSE_ACTIVE_NEW_SERVICE_FAILED, string.Join(',', iccidList));
                        await MarkProcessedForNewServiceActivationAsync(context, additionBulkChangeId, isAllBatchesSuccess, message, iccidList);
                        await bulkChangeRepository.MarkBulkChangeStatusAsync(context, additionBulkChangeId, BulkChangeStatus.PROCESSED);
                    }
                }

                return isAllBatchesSuccess;
            }
        }

        private async Task<bool> ProcessJasperStatusUpdateAsync(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, IEnumerable<BulkChangeDetailRecord> changes,
            IAsyncPolicy httpRetryPolicy, IAsyncPolicy sqlRetryPolicy, RevioApiClient revApiClient, int integrationAuthenticationId)
        {
            var environmentRepo = new EnvironmentRepository();
            var httpClientFactory = new KeysysHttpClientFactory();
            var jasperAuthentication = JasperCommon.GetJasperAuthenticationInformation(context.CentralDbConnectionString,
                bulkChange.ServiceProviderId);

            var emailFactory = new SimpleEmailServiceFactory();
            using var client = emailFactory.getClient(AwsSesCredentials(context), RegionEndpoint.USEast1);
            var awsEnv = context.EnvironmentRepo.GetEnvironmentVariable(context.Context, "AWSEnv");
            var emailSender = new EmailSender(client, context.logger, awsEnv);

            var jasperRatePlanRepository =
                new JasperRatePlanRepository(context.logger, environmentRepo, context.Context);
            var jasperDeviceDetailService = new JasperDeviceDetailService(jasperAuthentication,
                new Base64Service(), httpClientFactory, httpRetryPolicy);
            var deviceRepository = new DeviceRepository(context.logger, environmentRepo, context.Context);

            if (!jasperAuthentication.WriteIsEnabled)
            {
                string message = "Writes disabled for service provider";
                LogInfo(context, "WARN", message);

                var change = changes.First();
                var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(change.ChangeRequest);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = message,
                    HasErrors = true,
                    LogEntryDescription = "Update Jasper Status: Jasper API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = JsonConvert.SerializeObject(change),
                    ResponseText = message
                });

                // mark item processed
                MarkProcessed(context, bulkChange.Id, change.Id, false, changeRequest.PostUpdateStatusId, message);

                return false;
            }

            var changeRequests = new List<StatusUpdateRequest<dynamic>>();
            var revCustomerDefaultCustomerRatePlans = new List<RevCustomerDefaultCustomerRatePlans>();
            var revServices = GetRevServices(context, changes.Select(x => x.DeviceIdentifier).Distinct().ToList(), bulkChange.TenantId, bulkChange.ServiceProviderId);
            foreach (var change in changes)
            {
                changeRequests.Add(JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(change.ChangeRequest));
            }
            if (changeRequests.Any())
            {
                revCustomerDefaultCustomerRatePlans = GetCustomerRatePlans(context, changeRequests.Select(x => x.AccountNumber).Distinct().ToList(), bulkChange.TenantId);
            }

            foreach (var change in changes)
            {
                if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                {
                    // processing should continue, we just need to requeue
                    return true;
                }

                bool ratePlanUpdateIsSuccessful = true;
                var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(change.ChangeRequest);
                var iccid = change.DeviceIdentifier;
                if (changeRequest.UpdateStatus.ToLower().Trim() == "deactivated" && DeviceHasCommunicationPlan(context, iccid) && !sqsValues.IsFromAutomatedUpdateDeviceStatusLambda)
                {
                    ratePlanUpdateIsSuccessful = await UpdateJasperRatePlanAsync(context, logRepo, bulkChange, change,
                        jasperDeviceDetailService, jasperRatePlanRepository, deviceRepository, iccid, change.TenantId);
                }

                ApiResponse apiResult;
                if (ratePlanUpdateIsSuccessful)
                {
                    var updateResult = await UpdateJasperDeviceStatusAsync(context, jasperAuthentication, iccid, changeRequest.UpdateStatus);
                    apiResult = new ApiResponse() { IsSuccess = !updateResult.HasErrors, Response = JsonConvert.SerializeObject(updateResult.ResponseObject) };

                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = updateResult.HasErrors ? JsonConvert.SerializeObject(updateResult.ResponseObject) : null,
                        HasErrors = updateResult.HasErrors,
                        LogEntryDescription = "Update Jasper Status: Jasper API",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = updateResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = updateResult.ActionText + Environment.NewLine + updateResult.RequestObject,
                        ResponseText = JsonConvert.SerializeObject(updateResult.ResponseObject)
                    });
                }
                else
                {
                    apiResult = new ApiResponse() { IsSuccess = false, Response = "Error Updating Rate Plan for Deactivation" };
                }

                // process rev service
                if (apiResult.IsSuccess)
                {
                    await ProcessRevServiceCreation<ThingSpaceStatusUpdateRequest>(context, logRepo, httpRetryPolicy, sqlRetryPolicy,
                        revApiClient, bulkChange, new List<BulkChangeDetailRecord>() { change }, integrationAuthenticationId);

                    if (!string.IsNullOrWhiteSpace(changeRequest.AccountNumber))
                    {
                        var revCustomerDefaultCustomerRatePlan = revCustomerDefaultCustomerRatePlans.FirstOrDefault(x => x.RevCustomerId == changeRequest.AccountNumber);
                        var revService = revServices.Where(x => x.ICCID == change.DeviceIdentifier && x.TenantId == change.TenantId).OrderBy(x => x.ActivatedDate).FirstOrDefault();
                        if (revService?.RevServiceId > 0 && revService?.DisconnectedDate == null)
                        {
                            LogInfo(context, CommonConstants.INFO, LogCommonStrings.ACTIVE_SERVICE_LINE_CANNOT_CHANGE_CUSTOMERS);
                        }
                        else
                        {
                            UpdateRevCustomer(context, change.DeviceIdentifier, changeRequest.AccountNumber, change.TenantId, change.ServiceProviderId, changeRequest.PostUpdateStatusId, revCustomerDefaultCustomerRatePlan?.DefaultCustomerRatePlans);
                        }
                    }
                }

                // mark item processed
                MarkProcessed(context, bulkChange.Id, change.Id, apiResult?.IsSuccess ?? false, changeRequest.PostUpdateStatusId,
                    apiResult?.Response);
            }

            var updateRatePlanErrorList = jasperDeviceDetailService.UpdateDeviceDetailErrorList;
            if (updateRatePlanErrorList.Count > 0)
            {
                var lambdaContext = context.Context;
                var environmentRepository = context.EnvironmentRepo;
                var logger = new KeysysLambdaLogger(lambdaContext.Logger, lambdaContext, environmentRepository);
                var updateDeviceDetailsErrorNotification =
                    new UpdateDeviceDetailsErrorNotification(context.OptimizationSettings, logger, emailSender);
                await updateDeviceDetailsErrorNotification.SendErrorEmailNotificationAsync(updateRatePlanErrorList, bulkChange.ServiceProviderId, jasperAuthentication.ServiceProvider, context.CentralDbConnectionString);
            }

            return true;
        }

        private async Task<bool> ProcessThingSpaceStatusUpdateAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, IEnumerable<BulkChangeDetailRecord> changes, IAsyncPolicy httpRetryPolicy, IAsyncPolicy sqlRetryPolicy,
            RevioApiClient revApiClient, int integrationAuthenticationId)
        {
            var thingSpaceAuthentication =
                ThingSpaceCommon.GetThingspaceAuthenticationInformation(context.CentralDbConnectionString, bulkChange.ServiceProviderId);
            var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, bulkChange.ServiceProviderId);
            var accessToken = ThingSpaceCommon.GetAccessToken(thingSpaceAuthentication);
            if (accessToken != null)
            {
                var sessionToken = ThingSpaceCommon.GetSessionToken(thingSpaceAuthentication, accessToken);
                if (sessionToken == null)
                {
                    var change = changes.First();

                    string errorMessage = $"Error Sending {bulkChange.Id}: Session Token Request failed for ThingSpace.";
                    LogInfo(context, "ERROR", errorMessage);
                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = errorMessage,
                        HasErrors = true,
                        LogEntryDescription = "ThingSpace Device Status Update: ThingSpace API",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        RequestText = change.ChangeRequest,
                        ResponseStatus = BulkChangeStatus.ERROR,
                        ResponseText = errorMessage
                    });

                    return false;
                }

                var processedICCIDs = new List<string>();
                var updateStatusAction = string.Empty;
                foreach (var change in changes)
                {
                    if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                    {
                        // Should continue processing, we just need to requeue
                        break;
                    }

                    var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>>(change.ChangeRequest);
                    var apiResult = await UpdateThingSpaceDeviceStatusAsync(context, logRepo, thingSpaceAuthentication, accessToken, sessionToken,
                        bulkChange, change, changeRequest, bulkChange.ServiceProviderId, serviceProvider.RegisterCarrierServiceCallBack);

                    if (changeRequest.UpdateStatus.Equals(DeviceStatusConstant.ThingSpace_Active) && !changeRequest.IsIgnoreCurrentStatus)
                    {
                        updateStatusAction = DeviceStatusConstant.ThingSpace_Pending_Activate;
                    }
                    else
                    {
                        updateStatusAction = changeRequest.UpdateStatus;
                    }
                    processedICCIDs.Add(change.ICCID);

                    // process rev service
                    if (apiResult?.HasErrors == false)
                    {
                        await ProcessRevServiceCreation<ThingSpaceStatusUpdateRequest>(context, logRepo, httpRetryPolicy, sqlRetryPolicy,
                            revApiClient, bulkChange, new List<BulkChangeDetailRecord>() { change }, integrationAuthenticationId);
                    }

                    // mark item processed
                    MarkProcessed(context, bulkChange.Id, change.Id, !apiResult?.HasErrors ?? false, changeRequest.PostUpdateStatusId,
                        apiResult?.ResponseObject?.Response, apiResult.IsProcessed);
                }

                if (processedICCIDs.Count > 0 && !string.IsNullOrWhiteSpace(updateStatusAction) && (updateStatusAction.Equals(DeviceStatusConstant.ThingSpace_Inventory) || updateStatusAction.Equals(DeviceStatusConstant.ThingSpace_Pending_Activate)))
                {
                    deviceRepository.SetDefaultSite(context, serviceProvider.IntegrationId, bulkChange.TenantId, bulkChange.PortalTypeId, string.Join(",", processedICCIDs));
                }

                return true;
            }
            else
            {
                var change = changes.First();

                string errorMessage = $"Error Sending {bulkChange.Id}: Access Token Request failed for ThingSpace.";
                LogInfo(context, "ERROR", errorMessage);
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "ThingSpace Device Status Update: ThingSpace API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    RequestText = change.ChangeRequest,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    ResponseText = errorMessage
                });

                return false;
            }
        }

        private static bool DeviceHasCommunicationPlan(KeySysLambdaContext context, string iccid)
        {
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "Select CommunicationPlan FROM [dbo].[Device] WHERE ICCID = @ICCID";
                    cmd.Parameters.AddWithValue("@ICCID", iccid);

                    con.Open();
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var commPlan = rdr[0].ToString();
                        if (string.IsNullOrEmpty(commPlan))
                        {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        private static async Task<bool> UpdateJasperRatePlanAsync(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, BulkChangeDetailRecord change,
            IJasperDeviceDetailService jasperDeviceDetailService, IJasperRatePlanRepository jasperRatePlanRepository,
            IDeviceRepository deviceRepository, string iccid, int tenantId)
        {
            LogInfo(context, "SUB", $"UpdateJasperRatePlanAsync {iccid},{tenantId}");
            var ratePlans = jasperRatePlanRepository.GetRatePlans(iccid).OrderBy(x => x.BaseRate).ToList();
            if (ratePlans.Count == 0)
            {
                LogInfo(context, "SUB", $"Try Update Rate Plan for {iccid}. No Rate Plans found.");
                return false;
            }

            var device = deviceRepository.Get(iccid, tenantId);
            LogInfo(context, "SUB", $"Usage {iccid}, {device.DataUsageMB}.");
            var min = ratePlans.FirstOrDefault(ratePlan => ratePlan.PlanMB >= device.DataUsageMB) ??
                      ratePlans.ElementAt(ratePlans.Count - 1);

            var ratePlanUpdateIsSuccessful = false;
            try
            {
                LogInfo(context, "SUB", $"Try Update Rate Plan for {iccid} to {min.RatePlanCode} with Plan MB of {min.PlanMB}");
                var jasperDeviceDetail = new JasperDeviceDetail { ICCID = iccid, CarrierRatePlan = min.RatePlanCode };
                var updateResult =
                    await jasperDeviceDetailService.UpdateJasperDeviceDetailsAsync(jasperDeviceDetail, context.EnvironmentRepo,
                        context.Context, context.logger);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = updateResult.HasErrors ? updateResult.ResponseObject : null,
                    HasErrors = updateResult.HasErrors,
                    LogEntryDescription = "Update Jasper Rate Plan: Jasper API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = updateResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = updateResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(updateResult.RequestObject),
                    ResponseText = updateResult.ResponseObject
                });

                ratePlanUpdateIsSuccessful = !updateResult.HasErrors;

                if (!updateResult.HasErrors)
                {
                    var updateRatePlanDbResult = deviceRepository.UpdateRatePlan(min, device.ICCID);

                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = updateRatePlanDbResult.HasErrors ? updateRatePlanDbResult.ResponseObject : null,
                        HasErrors = updateRatePlanDbResult.HasErrors,
                        LogEntryDescription = "Update Jasper Rate Plan: Update AMOP Device Details",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = updateRatePlanDbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = updateRatePlanDbResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(updateRatePlanDbResult.RequestObject),
                        ResponseText = updateRatePlanDbResult.ResponseObject
                    });

                    ratePlanUpdateIsSuccessful = !updateRatePlanDbResult.HasErrors;
                }
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                string errorText = $"{logId} Error Updating Rate Plan for {device.ICCID}: {ex.Message}";
                LogInfo(context, "ERROR", errorText);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorText,
                    HasErrors = true,
                    LogEntryDescription = "Update Jasper Rate Plan: General",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = iccid,
                    ResponseText = $"Error Updating Rate Plan. Ref: {logId}"
                });

                ratePlanUpdateIsSuccessful = false;
            }

            return ratePlanUpdateIsSuccessful;
        }

        private static async Task<bool> UpdateJasperRatePlanAsync(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, BulkChangeDetailRecord change,
            JasperAuthentication jasperAuthentication,
            IDeviceRepository deviceRepository, IJasperDeviceRepository jasperDeviceRepository,
            IJasperDeviceDetailService jasperDeviceDetailService, JasperDeviceDetail jasperDeviceDetail)
        {
            LogInfo(context, "SUB", $"UpdateJasperRatePlanAsync {jasperDeviceDetail.ICCID}");

            if (!jasperAuthentication.WriteIsEnabled)
            {
                LogInfo(context, "WARN", "Writes disabled for service provider");
                return false;
            }

            var ratePlanUpdateIsSuccessful = false;
            try
            {
                var updateResult =
                    await jasperDeviceDetailService.UpdateJasperDeviceDetailsAsync(jasperDeviceDetail, context.EnvironmentRepo,
                        context.Context, context.logger);
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = updateResult.HasErrors ? updateResult.ResponseObject : null,
                    HasErrors = updateResult.HasErrors,
                    LogEntryDescription = "Update Jasper Rate Plan: Jasper API",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = updateResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = updateResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(updateResult.RequestObject),
                    ResponseText = updateResult.ResponseObject
                });

                if (!updateResult.HasErrors)
                {
                    context.logger.LogInfo("INFO", $"Rate Plan successfully updated in Jasper for {jasperDeviceDetail.ICCID}");
                    context.logger.LogInfo("INFO", $"Updating DB with Rate Plan changes for {jasperDeviceDetail.ICCID}");
                    var updateDbDetailsResult = await deviceRepository.UpdateDeviceDetailsAsync(jasperDeviceDetail);
                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = updateDbDetailsResult.HasErrors ? updateDbDetailsResult.ResponseObject : null,
                        HasErrors = updateDbDetailsResult.HasErrors,
                        LogEntryDescription = "Update Jasper Rate Plan: Update AMOP Device Details",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = updateDbDetailsResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = updateDbDetailsResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(updateDbDetailsResult.RequestObject),
                        ResponseText = updateDbDetailsResult.ResponseObject
                    });

                    var updateDbRatePlanResult = await jasperDeviceRepository.UpdateRatePlanAsync(jasperDeviceDetail.CarrierRatePlan,
                        jasperDeviceDetail.CommunicationPlan, jasperDeviceDetail.ICCID);

                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = updateDbRatePlanResult.HasErrors ? updateDbRatePlanResult.ResponseObject : null,
                        HasErrors = updateDbRatePlanResult.HasErrors,
                        LogEntryDescription = "Update Jasper Rate Plan: Update AMOP Rate Plan",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = updateDbRatePlanResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = updateDbRatePlanResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(updateDbRatePlanResult.RequestObject),
                        ResponseText = updateDbRatePlanResult.ResponseObject
                    });

                    ratePlanUpdateIsSuccessful = !updateDbDetailsResult.HasErrors && !updateDbRatePlanResult.HasErrors;
                }

            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                string errorText = $"{logId} Error Updating Rate Plan for {jasperDeviceDetail.ICCID}: {ex.Message}";
                LogInfo(context, "ERROR", errorText);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorText,
                    HasErrors = true,
                    LogEntryDescription = "Update Jasper Rate Plan: General",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = JsonConvert.SerializeObject(jasperDeviceDetail),
                    ResponseText = $"Error Updating Rate Plan. Ref: {logId}"
                });
            }

            return ratePlanUpdateIsSuccessful;
        }

        private async Task<bool> UpdateJasperDeviceIDAsync(KeySysLambdaContext context,
            DeviceBulkChangeLogRepository logRepo,
            EnvironmentRepository environmentRepo, IJasperDeviceDetailService jasperDeviceDetailService,
            BulkChange bulkChange, BulkChangeDetailRecord change, JasperDeviceDetail jasperDeviceDetail)
        {
            LogInfo(context, "SUB", $"UpdateJasperDeviceIDAsync {jasperDeviceDetail.ICCID}");
            try
            {
                var apiResult =
                    await jasperDeviceDetailService.UpdateJasperDeviceDetailsAsync(jasperDeviceDetail, environmentRepo,
                    context.Context, context.logger);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = apiResult.HasErrors ? apiResult.ResponseObject : null,
                    HasErrors = apiResult.HasErrors,
                    LogEntryDescription = "Update Jasper Device ID: Update Jasper",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = apiResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = apiResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(apiResult.RequestObject),
                    ResponseText = apiResult.ResponseObject
                });

                if (apiResult.HasErrors)
                {
                    return false;
                }

                context.logger.LogInfo("INFO", $"Jasper Device ID update successful for {jasperDeviceDetail.ICCID}");
                var deviceRepository = new DeviceRepository(context.logger, environmentRepo, context.Context);
                var dbResult = await deviceRepository.UpdateDeviceDetailsAsync(jasperDeviceDetail);

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                    HasErrors = dbResult.HasErrors,
                    LogEntryDescription = "Update Jasper Device ID: Update AMOP",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                    RequestText = dbResult.ActionText + Environment.NewLine + JsonConvert.SerializeObject(dbResult.RequestObject),
                    ResponseText = dbResult.ResponseObject
                });

                return true;
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Updating Rate Plan for {jasperDeviceDetail.ICCID}: {ex.Message} {ex.StackTrace}");

                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = $"Error Updating Rate Plan for {jasperDeviceDetail.ICCID}",
                    HasErrors = true,
                    LogEntryDescription = "Update Jasper Device ID: UpdateJasperDeviceIDAsync",
                    M2MDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    ResponseStatus = BulkChangeStatus.ERROR,
                    RequestText = JsonConvert.SerializeObject(jasperDeviceDetail),
                    ResponseText = $"Error Executing Update. Ref: {logId}"
                });

                return false;
            }
        }

        private static BulkChange GetBulkChange(KeySysLambdaContext context, long bulkChangeId)
        {
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "dbo.usp_DeviceBulkChange_GetBulkChange";
                    cmd.Parameters.AddWithValue("@id", bulkChangeId);
                    conn.Open();

                    var rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        return new BulkChange
                        {
                            Id = long.Parse(rdr[CommonColumnNames.Id].ToString()),
                            TenantId = int.Parse(rdr[CommonColumnNames.TenantId].ToString()),
                            Status = rdr[CommonColumnNames.Status].ToString(),
                            ChangeRequestTypeId = int.Parse(rdr[CommonColumnNames.ChangeRequestTypeId].ToString()),
                            ChangeRequestType = rdr[CommonColumnNames.ChangeRequestType].ToString(),
                            ServiceProviderId = int.Parse(rdr[CommonColumnNames.ServiceProviderId].ToString()),
                            ServiceProvider = rdr[CommonColumnNames.ServiceProvider].ToString(),
                            IntegrationId = int.Parse(rdr[CommonColumnNames.IntegrationId].ToString()),
                            Integration = rdr[CommonColumnNames.IntegrationId].ToString(),
                            PortalTypeId = int.Parse(rdr[CommonColumnNames.PortalTypeId].ToString()),
                            CreatedBy = rdr[CommonColumnNames.CreatedBy].ToString()
                        };
                    }
                }
            }

            return null;
        }

        private static string GetBulkChangeRequest(KeySysLambdaContext context, long bulkChangeId, int portalTypeId)
        {
            string record = "";
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_GET_BULK_CHANGE_REQUEST;
                        command.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                        command.Parameters.AddWithValue("@portalTypeId", portalTypeId);
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        connection.Open();

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                if (!reader.IsDBNull(0))
                                {
                                    record = reader.GetString(0);
                                }
                                else
                                {
                                    LogInfo(context, LogTypeConstant.Info, LogCommonStrings.BULK_CHANGE_DOES_NOT_HAVE_ANY_CHANGE_REQUESTS);
                                }
                            }
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, ex.Message);
            }

            return record;
        }

        private static ICollection<BulkChangeDetailRecord> GetDeviceChanges(KeySysLambdaContext context, long bulkChangeId, int portalTypeId,
            int pageSize, bool unprocessedChangesOnly = true)
        {
            context.logger.LogInfo(LogTypeConstant.Sub, $"({nameof(context)},{bulkChangeId},{portalTypeId},{pageSize})");
            string procedureName;
            switch (portalTypeId)
            {
                case PortalTypeM2M:
                    procedureName = Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_GET_M2M_CHANGES;
                    break;
                case PortalTypeMobility:
                    procedureName = Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_GET_MOBILITY_CHANGES;
                    break;
                default:
                    procedureName = Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_GET_LNP_CHANGES;
                    break;
            }

            List<BulkChangeDetailRecord> records = new List<BulkChangeDetailRecord>();
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = procedureName;
                        command.Parameters.AddWithValue("@count", pageSize);
                        command.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                        command.Parameters.AddWithValue("@unprocessedOnly", unprocessedChangesOnly);
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        connection.Open();

                        SqlDataReader reader = command.ExecuteReader();
                        if (portalTypeId != PortalTypeLNP)
                        {
                            while (reader.Read())
                            {
                                var deviceRecord = BulkChangeDetailRecord.DeviceRecordFromReader(reader, portalTypeId);
                                records.Add(deviceRecord);
                            }
                        }
                        else
                        {
                            while (reader.Read())
                            {
                                var deviceRecord = BulkChangeDetailRecord.LNPDeviceRecordFromReader(reader);
                                records.Add(deviceRecord);
                            }
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, ex.Message);
            }

            return records;
        }

        private static List<BulkChangeDetailRecord> GetBatchedWithAdditionalStepMobilityDeviceChanges(KeySysLambdaContext context, long bulkChangeId,
            int batchSize = 100, List<string> additionalStatusToFilterBy = null, bool onlyUnprocessed = true, string statusText = ChangeStatus.PROCESSED)
        {
            context.logger.LogInfo(LogTypeConstant.Sub, $"({nameof(context)},{bulkChangeId},{batchSize})");
            string procedureName = Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_GET_BATCHED_WITH_ADDITIONAL_STEP_MOBILITY_DEVICE_CHANGES;
            List<BulkChangeDetailRecord> records = new List<BulkChangeDetailRecord>();
            try
            {
                var additionalStepFilterStatus = (object)DBNull.Value;
                if (additionalStatusToFilterBy != null)
                {
                    additionalStepFilterStatus = string.Join(',', additionalStatusToFilterBy);
                }
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = procedureName;
                        command.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                        command.Parameters.AddWithValue("@batchSize", batchSize);
                        command.Parameters.AddWithValue("@statusText", statusText);
                        command.Parameters.AddWithValue("@additionalStepFilterStatus", additionalStepFilterStatus);
                        command.Parameters.AddWithValue("@onlyUnprocessed", onlyUnprocessed);
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        connection.Open();

                        SqlDataReader reader = command.ExecuteReader();
                        while (reader.Read())
                        {
                            var deviceRecord = BulkChangeDetailRecord.ProcessedDeviceRecordFromReader(reader);
                            records.Add(deviceRecord);
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, ex.Message);
            }

            return records;
        }

        private async Task<DeviceChangeResult<string, ApiResponse>> UpdateJasperDeviceStatusAsync(KeySysLambdaContext context, JasperAuthentication jasperAuthentication, string deviceIccid, string deviceStatus)
        {
            if (jasperAuthentication.WriteIsEnabled)
            {
                var decodedPassword = context.Base64Service.Base64Decode(jasperAuthentication.Password);
                using (var client = new HttpClient(new LambdaLoggingHandler()))
                {
                    client.BaseAddress = new Uri($"{jasperAuthentication.ProductionApiUrl.TrimEnd('/')}/{string.Format(DeviceStatusUpdatePath, deviceIccid).TrimStart('/')}");
                    LogInfo(context, "Endpoint", client.BaseAddress);
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    var encoded = context.Base64Service.Base64Encode(jasperAuthentication.Username + ":" + decodedPassword);
                    client.DefaultRequestHeaders.Add("Authorization", "Basic " + encoded);
                    var requestString = "{\"status\": \"" + deviceStatus + "\"}";
                    var content = new StringContent(requestString, Encoding.ASCII, "application/json");
                    var response = await client.PutAsync(client.BaseAddress, content);

                    var responseBody = await response.Content.ReadAsStringAsync();
                    if (!response.IsSuccessStatusCode)
                    {
                        LogInfo(context, "Response Error", responseBody);
                        return new DeviceChangeResult<string, ApiResponse>()
                        {
                            ActionText = $"PUT {client.BaseAddress}",
                            HasErrors = true,
                            RequestObject = requestString,
                            ResponseObject = new ApiResponse { Response = responseBody, IsSuccess = false, StatusCode = response.StatusCode }
                        };
                    }

                    var updateDeviceStatusResult = JsonConvert.DeserializeObject<UpdateDeviceStatusResult>(responseBody);
                    if (string.IsNullOrWhiteSpace(updateDeviceStatusResult.iccid))
                    {
                        LogInfo(context, "ERROR", $"Device Status for '{deviceIccid}' Not Updated");
                        return new DeviceChangeResult<string, ApiResponse>()
                        {
                            ActionText = $"PUT {client.BaseAddress}",
                            HasErrors = true,
                            RequestObject = requestString,
                            ResponseObject = new ApiResponse { Response = responseBody, IsSuccess = false, StatusCode = response.StatusCode }
                        };
                    }

                    return new DeviceChangeResult<string, ApiResponse>()
                    {
                        ActionText = $"PUT {client.BaseAddress}",
                        HasErrors = false,
                        RequestObject = requestString,
                        ResponseObject = new ApiResponse { Response = responseBody, IsSuccess = true, StatusCode = response.StatusCode }
                    };
                }
            }

            LogInfo(context, "WARN", "Writes disabled for service provider");
            return new DeviceChangeResult<string, ApiResponse>()
            {
                ActionText = $"Jasper API: Update Status",
                HasErrors = true,
                RequestObject = null,
                ResponseObject = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" }
            };
        }

        private async Task<DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>>
            UpdateThingSpaceDeviceStatusAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
                ThingSpaceAuthentication thingSpaceAuth, ThingSpaceTokenResponse accessToken,
                ThingSpaceLoginResponse sessionToken, BulkChange bulkChange, BulkChangeDetailRecord change,
                StatusUpdateRequest<ThingSpaceStatusUpdateRequest> changeRequest, int serviceProviderId, bool useCallbackResult)
        {
            var thingspaceRequest = changeRequest.Request;
            switch (changeRequest.UpdateStatus?.ToLower())
            {
                case "pending activation":
                    if (ThingSpaceDeviceIsValidForAdd(context, thingspaceRequest, change.Id))
                    {
                        // PORT-311: if pending actiavte with ICCID & IMEI and Rev AccountNumber -> Pending activate -> Active device
                        // ICCID: already exists -> error, else -> extive
                        // ICCID & IMEI: -> ICCDI already exists -> error or IMEI already exists -> error, else -> pending

                        if (!changeRequest.IsIgnoreCurrentStatus)
                        {
                            var addThingSpaceResult = await AddThingSpaceDeviceAsync(context, logRepo, bulkChange, change, thingSpaceAuth, accessToken,
                                sessionToken, thingspaceRequest, serviceProviderId, changeRequest.UpdateStatus);

                            //if (!string.IsNullOrEmpty(changeRequest.Request.RevAccountNumber) && !string.IsNullOrEmpty(changeRequest.Request.ICCID)
                            //    && !string.IsNullOrEmpty(changeRequest.Request.IMEI) && !addThingSpaceResult.HasErrors)
                            //{
                            //    // new activate device
                            //    var newActivateThingSpaceResponse = await ActivateThingSpaceDeviceAsync(context, logRepo, bulkChange, change,
                            //        thingSpaceAuth, accessToken, sessionToken, thingspaceRequest, serviceProviderId);

                            //    return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                            //    {
                            //        ActionText = addThingSpaceResult.ActionText,
                            //        HasErrors = addThingSpaceResult.HasErrors && newActivateThingSpaceResponse.HasErrors,
                            //        RequestObject = changeRequest,
                            //        ResponseObject = addThingSpaceResult.ResponseObject,
                            //        IsProcessed = newActivateThingSpaceResponse.IsProcessed
                            //    };
                            //}

                            return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                            {
                                ActionText = addThingSpaceResult.ActionText,
                                HasErrors = addThingSpaceResult.HasErrors,
                                RequestObject = changeRequest,
                                ResponseObject = addThingSpaceResult.ResponseObject
                            };
                        }
                        else
                        {
                            // new activate device
                            var newActivateThingSpaceResponse = await ActivateThingSpaceDeviceAsync(context, logRepo, bulkChange, change,
                                thingSpaceAuth, accessToken, sessionToken, thingspaceRequest, serviceProviderId, useCallbackResult);

                            return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                            {
                                ActionText = newActivateThingSpaceResponse.ActionText,
                                HasErrors = newActivateThingSpaceResponse.HasErrors,
                                RequestObject = changeRequest,
                                ResponseObject = newActivateThingSpaceResponse.ResponseObject,
                                IsProcessed = newActivateThingSpaceResponse.IsProcessed
                            };
                        }
                    }

                    return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                    {
                        ActionText = "ThingSpaceDeviceIsValidForAdd",
                        HasErrors = true,
                        RequestObject = changeRequest,
                        ResponseObject = new ApiResponse { IsSuccess = false, Response = "Request invalid, missing required information" }
                    };
                case "active":
                    // PORT-311: When active with ICCID & IMEI 
                    // - Pending: if pending success -> Active
                    // - When active with ICCID -> only active 

                    if (!changeRequest.IsIgnoreCurrentStatus)
                    {
                        var addThingSpaceResult = await AddThingSpaceDeviceAsync(context, logRepo, bulkChange, change, thingSpaceAuth, accessToken,
                            sessionToken, thingspaceRequest, serviceProviderId, changeRequest.UpdateStatus);

                        if (!string.IsNullOrEmpty(changeRequest.Request.ICCID) && !string.IsNullOrEmpty(changeRequest.Request.IMEI) && !addThingSpaceResult.HasErrors)
                        {
                            // new activate device
                            var newActivateThingSpaceResponse = await ActivateThingSpaceDeviceAsync(context, logRepo, bulkChange, change,
                                thingSpaceAuth, accessToken, sessionToken, thingspaceRequest, serviceProviderId, useCallbackResult);

                            return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                            {
                                ActionText = addThingSpaceResult.ActionText,
                                HasErrors = addThingSpaceResult.HasErrors || newActivateThingSpaceResponse.HasErrors,
                                RequestObject = changeRequest,
                                ResponseObject = addThingSpaceResult.ResponseObject,
                                IsProcessed = newActivateThingSpaceResponse.IsProcessed
                            };
                        }

                        return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                        {
                            ActionText = addThingSpaceResult.ActionText,
                            HasErrors = addThingSpaceResult.HasErrors,
                            RequestObject = changeRequest,
                            ResponseObject = addThingSpaceResult.ResponseObject
                        };
                    }
                    else
                    {
                        // new activate device
                        var newActivateThingSpaceResponse = await ActivateThingSpaceDeviceAsync(context, logRepo, bulkChange, change,
                            thingSpaceAuth, accessToken, sessionToken, thingspaceRequest, serviceProviderId, useCallbackResult);

                        return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                        {
                            ActionText = newActivateThingSpaceResponse.ActionText,
                            HasErrors = newActivateThingSpaceResponse.HasErrors,
                            RequestObject = changeRequest,
                            ResponseObject = newActivateThingSpaceResponse.ResponseObject,
                            IsProcessed = newActivateThingSpaceResponse.IsProcessed
                        };
                    }
                case DeviceStatusConstant.ThingSpace_Inventory:
                    var addThingSpaceInventoryResult = AddThingSpaceDeviceInventoryAsync(context, logRepo, bulkChange, change,
                        thingSpaceAuth, thingspaceRequest, serviceProviderId, changeRequest.UpdateStatus);

                    return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                    {
                        ActionText = addThingSpaceInventoryResult.ActionText,
                        HasErrors = addThingSpaceInventoryResult.HasErrors,
                        RequestObject = changeRequest,
                        ResponseObject = addThingSpaceInventoryResult.ResponseObject,
                    };
                case "deactive":
                    var deactivateThingSpaceResult = await DeactivateThingSpaceDeviceAsync(context, logRepo, bulkChange, change,
                        thingSpaceAuth, accessToken, sessionToken, change.DeviceIdentifier, thingspaceRequest.ReasonCode);
                    return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                    {
                        ActionText = deactivateThingSpaceResult.ActionText,
                        HasErrors = deactivateThingSpaceResult.HasErrors,
                        RequestObject = changeRequest,
                        ResponseObject = deactivateThingSpaceResult.ResponseObject
                    };
                case "suspend":
                    var suspendThingSpaceResult = await SuspendThingSpaceDeviceAsync(context, logRepo, bulkChange, change, thingSpaceAuth, accessToken,
                        sessionToken, change.DeviceIdentifier);

                    return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                    {
                        ActionText = suspendThingSpaceResult.ActionText,
                        HasErrors = suspendThingSpaceResult.HasErrors,
                        RequestObject = changeRequest,
                        ResponseObject = suspendThingSpaceResult.ResponseObject
                    };
                case "restore":
                    var restoreThingSpaceResult = await RestoreThingSpaceDeviceAsync(context, logRepo, bulkChange, change, thingSpaceAuth, accessToken,
                        sessionToken, change.DeviceIdentifier);

                    return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                    {
                        ActionText = restoreThingSpaceResult.ActionText,
                        HasErrors = restoreThingSpaceResult.HasErrors,
                        RequestObject = changeRequest,
                        ResponseObject = restoreThingSpaceResult.ResponseObject
                    };
                default:
                    var message = $"Device Status for '{change.DeviceIdentifier}' Not Updated. Unsupported Status {changeRequest.UpdateStatus}";
                    LogInfo(context, "ERROR", message);

                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = message,
                        HasErrors = true,
                        LogEntryDescription = "Update ThingSpace Device Status: General",
                        M2MDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        RequestText = JsonConvert.SerializeObject(changeRequest),
                        ResponseStatus = BulkChangeStatus.ERROR,
                        ResponseText = message
                    });

                    return new DeviceChangeResult<StatusUpdateRequest<ThingSpaceStatusUpdateRequest>, ApiResponse>()
                    {
                        ActionText = "UpdateThingSpaceDeviceStatus",
                        HasErrors = true,
                        RequestObject = changeRequest,
                        ResponseObject = new ApiResponse { IsSuccess = false, Response = message }
                    };
            }
        }
        private static async Task<DeviceChangeResult<string, string>> UpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync(ThingSpaceStatusUpdateRequest request, string baseUrl,
            ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, KeySysLambdaContext context, int serviceProviderId, string accountName, string requestId, long bulkChangeId, int tenantId, ThingSpaceCallBackResponseLog thingSpaceCallBackLog, bool useCallbackResult)
        {
            LogInfo(context, "INFO", $"UpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync.");
            if (useCallbackResult && thingSpaceCallBackLog != null)
            {
                return NewFlowUpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync(request, context, serviceProviderId, requestId, bulkChangeId, thingSpaceCallBackLog);
            }
            return await OldFlowUpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync(request, baseUrl, accessToken, sessionToken, context, serviceProviderId, accountName, requestId, bulkChangeId, tenantId);
        }

        private static async Task<DeviceChangeResult<string, string>> ActivatedWithThingSpacePPUThingSpaceDeviceAsync(ThingSpaceStatusUpdateRequest request, string baseUrl, ThingSpaceTokenResponse accessToken,
            ThingSpaceLoginResponse sessionToken, KeySysLambdaContext context, int serviceProviderId, string requestId, string getStatusUrl, ThingSpaceAuthentication thingSpaceAuth, long bulkChangeId, int tenantId, ThingSpaceCallBackResponseLog thingSpaceCallBackLog, bool useCallbackResult)
        {
            LogInfo(context, "INFO", $"ActivatedWithThingSpacePPUThingSpaceDeviceAsync.");
            if (useCallbackResult && thingSpaceCallBackLog != null)
            {
                //new flow use callback                
                return NewFlowActivatedWithThingSpacePPUThingSpaceDeviceAsync(request, context, serviceProviderId, requestId, bulkChangeId, thingSpaceCallBackLog);
            }
            return await OldFlowActivatedWithThingSpacePPUThingSpaceDeviceAsync(request, baseUrl, accessToken, sessionToken, context, serviceProviderId, requestId, getStatusUrl, thingSpaceAuth, bulkChangeId);
        }

        private static string GetExtendedAttributesByKey(List<ExtendedAttribute> extendedAttributes, string key)
        {
            return extendedAttributes.Where(x => x.key.Equals(key)).Select(x => x.value).FirstOrDefault();
        }

        private static string GetThingSpacePPUFromResponseAPI(KeySysLambdaContext context, List<ExtendedAttribute> extendedAttributes)
        {
            LogInfo(context, "INFO", $"GetThingSpacePPUFromResponseAPI.");
            var thingSpacePPU = new ThingSpacePPU()
            {
                FirstName = GetExtendedAttributesByKey(extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseFirstName)),
                LastName = GetExtendedAttributesByKey(extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseLastName)),
                AddressLine = GetExtendedAttributesByKey(extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseAddressLine1)),
                City = GetExtendedAttributesByKey(extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseCity)),
                State = GetExtendedAttributesByKey(extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseState)),
                ZipCode = GetExtendedAttributesByKey(extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseZipCode)),
                Country = GetExtendedAttributesByKey(extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseCountry)),
            };

            return JsonConvert.SerializeObject(thingSpacePPU);
        }

        public static async Task<ThingSpaceServiceAmop.Models.DeviceResponse> GetThingSpaceDeviceAsync(string iccid, string baseUrl, ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, IKeysysLogger logger)
        {
            using (var client = new HttpClient(new LambdaLoggingHandler()))
            {
                client.BaseAddress = new Uri($"{baseUrl.TrimEnd('/')}/api/m2m/v1/devices/actions/list");
                logger.LogInfo("Endpoint", client.BaseAddress);
                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);
                var jsonDeviceContent = $"{{\"deviceId\":{{\"id\":\"{iccid}\",\"kind\":\"iccid\"}}}}";
                var contDevice = new StringContent(jsonDeviceContent, Encoding.UTF8, "application/json");
                var response = await client.PostAsync(client.BaseAddress, contDevice);
                if (response.IsSuccessStatusCode)
                {
                    string responseBody = await response.Content.ReadAsStringAsync();
                    var body = JsonConvert.DeserializeObject<ThingSpaceDeviceResponseRootObject>(responseBody);
                    return body?.devices?.FirstOrDefault();
                }
                else
                {
                    var responseBody = await response.Content.ReadAsStringAsync();
                    logger.LogInfo("EXCEPTION", responseBody);
                    return null;
                }
            }
        }
        public static async Task<ThingSpaceRequestStatusResponse> GetThingSpaceRequestStatusAsync(string accountName, string requestId, string baseUrl, ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, IKeysysLogger logger = null)
        {
            using (var client = new HttpClient(new LambdaLoggingHandler()))
            {
                client.BaseAddress = new Uri($"{baseUrl.TrimEnd('/')}/api/m2m/v1/accounts/{accountName}/requests/{requestId}/status");
                if (logger != null)
                    logger.LogInfo("Endpoint", client.BaseAddress);
                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);
                var response = await client.GetAsync(client.BaseAddress);
                if (response.IsSuccessStatusCode)
                {
                    string responseBody = await response.Content.ReadAsStringAsync();
                    var body = JsonConvert.DeserializeObject<ThingSpaceRequestStatusResponse>(responseBody);
                    return body;
                }
                else
                {
                    var responseBody = await response.Content.ReadAsStringAsync();
                    logger.LogInfo("EXCEPTION", responseBody);
                    return null;
                }
            }
        }
        private static bool ThingSpaceDeviceIsValidForAdd(KeySysLambdaContext context, ThingSpaceStatusUpdateRequest request, long changeDetailId)
        {
            if (string.IsNullOrWhiteSpace(request?.ICCID))
            {
                LogInfo(context, "ERROR", $"ICCID required to add device for change {changeDetailId}");
                return false;
            }

            // record is valid
            return true;
        }

        private DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse> AddThingSpaceDeviceInventoryAsync(
            KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, BulkChangeDetailRecord change, ThingSpaceAuthentication thingSpaceAuth, ThingSpaceStatusUpdateRequest request,
            int serviceProviderId, string targetStatus)
        {
            var apiResponse = new ApiResponse();
            var processedBy = context.Context.FunctionName;
            var isSuccess = false;

            if (thingSpaceAuth.WriteIsEnabled)
            {
                var jsonDeviceContent = GetThingspaceAddDeviceBody(thingSpaceAuth, request);
                // add device to the database
                using (var conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    conn.Open();

                    int deviceCount = deviceRepository.CountDeviceNumber(context, serviceProviderId, request.ICCID);
                    if (deviceCount > 0)
                    {
                        UpdateStatusThingSpaceDevice(context, logRepo, bulkChange, change, request, serviceProviderId, targetStatus, (int)ThingSpaceDeviceStatus.inventory, conn, processedBy, jsonDeviceContent);
                    }
                    else
                    {
                        // Add the new device to ThingSpaceDevice table, ThingSpaceDeviceUsage table, ThingSpaceDeviceDetail table and Device table
                        AddThingSpaceDevice(context, logRepo, bulkChange, change, request, serviceProviderId, targetStatus, conn, processedBy, jsonDeviceContent, thingSpaceRepository);
                        // Add the new device to the device tenant table to show this device in the Inventory page
                        AddThingSpaceDeviceToInventory(context, logRepo, bulkChange, change, request, conn, processedBy, jsonDeviceContent);
                    }
                }

                isSuccess = true;
                apiResponse = new ApiResponse { IsSuccess = isSuccess, Response = LogCommonStrings.MESSAGE_ADD_THINGSPACE_DEVICE_SUCCESS, StatusCode = HttpStatusCode.OK };
                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                {
                    ActionText = string.Empty,
                    HasErrors = !isSuccess,
                    RequestObject = request,
                    ResponseObject = apiResponse
                };
            }
            else
            {
                LogInfo(context, LogTypeConstant.Warning, LogCommonStrings.SERVICE_PROVIDER_IS_DISABLED);
                apiResponse = new ApiResponse { IsSuccess = isSuccess, Response = LogCommonStrings.SERVICE_PROVIDER_IS_DISABLED };
                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                {
                    ActionText = $"AddThingSpaceDeviceInventory: General",
                    HasErrors = !isSuccess,
                    RequestObject = request,
                    ResponseObject = apiResponse
                };
            }
        }

        private static void UpdateStatusThingSpaceDevice(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, BulkChangeDetailRecord change, ThingSpaceStatusUpdateRequest request,
            int serviceProviderId, string targetStatus, int targetStatusId, SqlConnection conn, string processedBy, string jsonDeviceContent)
        {
            LogInfo(context, LogTypeConstant.Warning, string.Format(LogCommonStrings.WARNING_DEVICE_EXISTS, request.ICCID, CommonConstants.THINGSPACE));
            var dbResult = UpdateThingSpaceDeviceStatus(context, request, serviceProviderId, targetStatusId, conn, targetStatus);

            var logEntryDesc = string.Format(LogCommonStrings.MESSAGE_UPDATED_DEVICE_STATUS, CommonConstants.THINGSPACE);
            var responseText = string.Format(LogCommonStrings.MESSAGE_UPDATE_DEVICE_STATUS_SUCCESS, request.ICCID, "Inventory");
            AddThingSpaceBulkChangeLog(logRepo, bulkChange.Id, dbResult, change.Id, processedBy, jsonDeviceContent, responseText, logEntryDesc);
        }

        private static void AddThingSpaceDevice(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, BulkChangeDetailRecord change, ThingSpaceStatusUpdateRequest request,
            int serviceProviderId, string targetStatus, SqlConnection conn, string processedBy, string jsonDeviceContent, IThingSpaceRepository thingSpaceRepo)
        {
            LogInfo(context, LogTypeConstant.Sub, "");

            var dbResult = thingSpaceRepo.AddThingSpaceDevice(context, serviceProviderId, request, targetStatus, processedBy, (int)ThingSpaceDeviceStatus.inventory);

            var logEntryDesc = string.Format(LogCommonStrings.MESSAGE_ADD_DEVICE, CommonConstants.THINGSPACE);
            var responseText = string.Format(LogCommonStrings.DEVICE_ADDED_SUCCESS, request.ICCID);
            AddThingSpaceBulkChangeLog(logRepo, bulkChange.Id, dbResult, change.Id, processedBy, jsonDeviceContent, responseText, logEntryDesc);
        }

        private static void AddThingSpaceDeviceToInventory(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, BulkChangeDetailRecord change, ThingSpaceStatusUpdateRequest request, SqlConnection conn, string processedBy, string jsonDeviceContent)
        {
            LogInfo(context, LogTypeConstant.Sub, "");

            var dbResult = AddDeviceToDbInventory(context, request, conn);
            AddThingSpaceBulkChangeLog(logRepo, bulkChange.Id, dbResult, change.Id, processedBy, jsonDeviceContent, LogCommonStrings.MESSAGE_INSERT_M2M_DEVICE, LogCommonStrings.MESSAGE_INSERT_M2M_DEVICE);
        }

        private static void AddThingSpaceBulkChangeLog(DeviceBulkChangeLogRepository logRepo, long bulkChangeId, DeviceChangeResult<ThingSpaceStatusUpdateRequest, string> dbResult, long changeId, string processedBy, string requestText, string responseText, string logEntryDescription)
        {
            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
            {
                BulkChangeId = bulkChangeId,
                ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                HasErrors = dbResult.HasErrors,
                LogEntryDescription = logEntryDescription,
                M2MDeviceChangeId = changeId,
                ProcessBy = processedBy,
                ProcessedDate = DateTime.UtcNow,
                RequestText = requestText,
                ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                ResponseText = responseText
            });
        }

        private static void AddBulkChangeLog(DeviceBulkChangeLogRepository logRepo, long bulkChangeId, bool hasErrors, string responseObject, long changeId, string processedBy, string requestText, string responseText, string logEntryDescription)
        {
            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
            {
                BulkChangeId = bulkChangeId,
                ErrorText = hasErrors ? responseObject : null,
                HasErrors = hasErrors,
                LogEntryDescription = logEntryDescription,
                M2MDeviceChangeId = changeId,
                ProcessBy = processedBy,
                ProcessedDate = DateTime.UtcNow,
                RequestText = requestText,
                ResponseStatus = hasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                ResponseText = responseText
            });
        }

        private static async Task<DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>> AddThingSpaceDeviceAsync(
            KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, BulkChangeDetailRecord change, ThingSpaceAuthentication thingSpaceAuth,
            ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, ThingSpaceStatusUpdateRequest request,
            int serviceProviderId, string targetStatus)
        {
            var apiResponse = new ApiResponse();

            if (thingSpaceAuth.WriteIsEnabled)
            {
                using (var client = new HttpClient(new LambdaLoggingHandler()))
                {
                    var baseUrl = thingSpaceAuth.BaseUrl.TrimEnd('/');
                    client.BaseAddress = new Uri(baseUrl + "/api/m2m/v1/devices/actions/add");
                    LogInfo(context, "Endpoint", client.BaseAddress);
                    client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                    client.DefaultRequestHeaders.Add("Accept", "application/json");
                    client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);
                    var jsonDeviceContent = GetThingspaceAddDeviceBody(thingSpaceAuth, request);
                    var contDevice = new StringContent(jsonDeviceContent, Encoding.UTF8, "application/json");
                    var response = await client.PostAsync(client.BaseAddress, contDevice);
                    if (response.IsSuccessStatusCode)
                    {
                        string responseBody = await response.Content.ReadAsStringAsync();
                        AddThingSpaceDeviceResult[] addDeviceResult;
                        try
                        {
                            addDeviceResult = JsonConvert.DeserializeObject<AddThingSpaceDeviceResult[]>(responseBody);
                        }
                        catch (Exception)
                        {
                            string errorMessage = $"Device '{request.ICCID}' not added. Cannot check response: {responseBody}";
                            LogInfo(context, "ERROR", errorMessage);
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = errorMessage,
                                HasErrors = true,
                                LogEntryDescription = "Add ThingSpace Device: ThingSpace API",
                                M2MDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonDeviceContent,
                                ResponseStatus = ((int)response.StatusCode).ToString(),
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                            return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }

                        if (addDeviceResult == null || addDeviceResult.Length == 0 ||
                            string.IsNullOrWhiteSpace(addDeviceResult.FirstOrDefault()?.response) ||
                            (addDeviceResult.FirstOrDefault()?.response != "Success" &&
                             addDeviceResult.FirstOrDefault()?.response != "Error. Device already exists."))
                        {
                            string errorMessage = $"Device '{request.ICCID}' not added. {responseBody}";
                            LogInfo(context, "ERROR", errorMessage);
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = errorMessage,
                                HasErrors = true,
                                LogEntryDescription = "Add ThingSpace Device: ThingSpace API",
                                M2MDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonDeviceContent,
                                ResponseStatus = ((int)response.StatusCode).ToString(),
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                            return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                        else
                        {
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = null,
                                HasErrors = false,
                                LogEntryDescription = "Add ThingSpace Device: ThingSpace API",
                                M2MDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonDeviceContent,
                                ResponseStatus = ((int)response.StatusCode).ToString(),
                                ResponseText = responseBody
                            });
                        }

                        // add device to the database
                        using (var conn = new SqlConnection(context.CentralDbConnectionString))
                        {
                            conn.Open();

                            // check if device is in database
                            int deviceCount = 0;
                            using (var cmd = conn.CreateCommand())
                            {
                                cmd.CommandType = CommandType.Text;
                                cmd.CommandText =
                                    "SELECT COUNT(1) FROM Device WHERE ServiceProviderId = @ServiceProviderId AND ICCID = @ICCID";
                                cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                                cmd.Parameters.AddWithValue("@ICCID", request.ICCID);

                                object oDeviceCount = cmd.ExecuteScalar();
                                if (oDeviceCount != null && oDeviceCount != DBNull.Value)
                                {
                                    deviceCount = int.Parse(oDeviceCount.ToString());
                                }
                            }

                            var dbResult = new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>();
                            if (deviceCount > 0)
                            {
                                LogInfo(context, "WARN", $"Device '{request.ICCID}' added to ThingSpace, but already exists in AMOP.");
                                dbResult = UpdateThingSpaceDeviceStatus(context, request, serviceProviderId, THINGSPACE_DEVICESTATUS_PENDINGACTIVATION, conn, targetStatus);

                                // log result
                                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                                {
                                    BulkChangeId = bulkChange.Id,
                                    ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                                    HasErrors = dbResult.HasErrors,
                                    LogEntryDescription = "Update ThingSpace Device Status: AMOP DB",
                                    M2MDeviceChangeId = change.Id,
                                    ProcessBy = "AltaworxDeviceBulkChange",
                                    ProcessedDate = DateTime.UtcNow,
                                    RequestText = jsonDeviceContent,
                                    ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                                    ResponseText = responseBody
                                });
                            }
                            else
                            {
                                if (await GetThingspaceDeviceCountAsync(conn, request.ICCID, serviceProviderId) == 0)
                                {
                                    dbResult = InsertNewThingSpaceDevice(context, request, serviceProviderId, targetStatus, conn);

                                    // log result
                                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                                    {
                                        BulkChangeId = bulkChange.Id,
                                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                                        HasErrors = dbResult.HasErrors,
                                        LogEntryDescription = "Insert ThingSpace Device: AMOP DB",
                                        M2MDeviceChangeId = change.Id,
                                        ProcessBy = "AltaworxDeviceBulkChange",
                                        ProcessedDate = DateTime.UtcNow,
                                        RequestText = jsonDeviceContent,
                                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                                        ResponseText = responseBody
                                    });
                                }

                                if (await GetThingspaceDeviceDetailCountAsync(conn, request.ICCID) == 0)
                                {
                                    dbResult = InsertNewThingSpaceDeviceDetail(context, request, conn);

                                    // log result
                                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                                    {
                                        BulkChangeId = bulkChange.Id,
                                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                                        HasErrors = dbResult.HasErrors,
                                        LogEntryDescription = "Insert ThingSpace Device Detail: AMOP DB",
                                        M2MDeviceChangeId = change.Id,
                                        ProcessBy = "AltaworxDeviceBulkChange",
                                        ProcessedDate = DateTime.UtcNow,
                                        RequestText = jsonDeviceContent,
                                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                                        ResponseText = responseBody
                                    });
                                }

                                if (await GetThingspaceDeviceUsageCountAsync(conn, request.ICCID) == 0)
                                {
                                    dbResult = InsertNewThingSpaceDeviceUsage(context, request, targetStatus, conn);

                                    // log result
                                    logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                                    {
                                        BulkChangeId = bulkChange.Id,
                                        ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                                        HasErrors = dbResult.HasErrors,
                                        LogEntryDescription = "Insert ThingSpace Device Usage: AMOP DB",
                                        M2MDeviceChangeId = change.Id,
                                        ProcessBy = "AltaworxDeviceBulkChange",
                                        ProcessedDate = DateTime.UtcNow,
                                        RequestText = jsonDeviceContent,
                                        ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                                        ResponseText = responseBody
                                    });
                                }

                                int? carrierRatePlanId = null;
                                using (var cmd = conn.CreateCommand())
                                {
                                    cmd.CommandType = CommandType.Text;
                                    cmd.CommandText = "SELECT TOP 1 id FROM JasperCarrierRatePlan WHERE IsDeleted = 0 AND ServiceProviderId = @ServiceProviderId AND RatePlanCode = @RatePlan";
                                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                                    cmd.Parameters.AddWithValue("@RatePlan", request.RatePlanCode);

                                    using (var reader = cmd.ExecuteReader())
                                    {
                                        while (reader.Read())
                                        {
                                            carrierRatePlanId = reader.GetInt32(0);
                                        }
                                    }
                                }

                                dbResult = InsertNewDevice(context, request, serviceProviderId, targetStatus, conn, carrierRatePlanId);
                                // log result
                                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                                {
                                    BulkChangeId = bulkChange.Id,
                                    ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                                    HasErrors = dbResult.HasErrors,
                                    LogEntryDescription = "Insert M2M Device: AMOP DB",
                                    M2MDeviceChangeId = change.Id,
                                    ProcessBy = "AltaworxDeviceBulkChange",
                                    ProcessedDate = DateTime.UtcNow,
                                    RequestText = jsonDeviceContent,
                                    ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                                    ResponseText = responseBody
                                });

                                dbResult = AddDeviceToDbInventory(context, request, conn);

                                // log result
                                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                                {
                                    BulkChangeId = bulkChange.Id,
                                    ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                                    HasErrors = dbResult.HasErrors,
                                    LogEntryDescription = "Insert M2M Device Inventory: AMOP DB",
                                    M2MDeviceChangeId = change.Id,
                                    ProcessBy = "AltaworxDeviceBulkChange",
                                    ProcessedDate = DateTime.UtcNow,
                                    RequestText = jsonDeviceContent,
                                    ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                                    ResponseText = responseBody
                                });
                            }

                            conn.Close();
                        }

                        apiResponse = new ApiResponse { IsSuccess = true, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = false,
                            RequestObject = request,
                            ResponseObject = apiResponse
                        };
                    }
                    else
                    {
                        var responseBody = response.Content.ReadAsStringAsync().Result;
                        LogInfo(context, "Response Error", responseBody);
                        apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = true,
                            RequestObject = request,
                            ResponseObject = apiResponse
                        };
                    }
                }
            }
            else
            {
                LogInfo(context, "WARN", "Writes disabled for service provider");
                apiResponse = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" };
                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                {
                    ActionText = $"AddThingSpaceDevice: General",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = apiResponse
                };
            }
        }

        private static DeviceChangeResult<ThingSpaceStatusUpdateRequest, string> AddDeviceToDbInventory(KeySysLambdaContext context, ThingSpaceStatusUpdateRequest request, SqlConnection conn)
        {
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "[dbo].[usp_DeviceBulkChange_AddToDeviceInventory]";

                    cmd.Parameters.AddWithValue("@ICCID", request.ICCID);
                    cmd.Parameters.AddWithValue("@AccountNumber", request.RevAccountNumber ?? (object)DBNull.Value);

                    cmd.ExecuteNonQuery();
                }

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "usp_DeviceBulkChange_AddToDeviceInventory",
                    HasErrors = false,
                    RequestObject = request,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing Stored Procedure: {ex.Message} {ex.StackTrace}");

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "usp_DeviceBulkChange_AddToDeviceInventory",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = $"Error Executing Stored Procedure. Ref: {logId}"
                };
            }
        }

        private static DeviceChangeResult<ThingSpaceStatusUpdateRequest, string> InsertNewDevice(KeySysLambdaContext context, ThingSpaceStatusUpdateRequest request, int serviceProviderId, string targetStatus, SqlConnection conn, int? carrierRatePlanId)
        {
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "INSERT INTO Device(ServiceProviderId, ICCID, IMEI, DeviceStatusId, Status, CarrierRatePlanId, RatePlan, ProviderDateAdded, CreatedBy, CreatedDate, IsActive, IsDeleted) VALUES(@ServiceProviderId, @ICCID, @IMEI, @DeviceStatusId, @Status, @CarrierRatePlanId, @RatePlan, GETUTCDATE(), 'AltaworxDeviceBulkChange', GETUTCDATE(), 1, 0)";
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.Parameters.AddWithValue("@ICCID", request.ICCID);
                    cmd.Parameters.AddWithValue("@IMEI", request.IMEI);
                    cmd.Parameters.AddWithValue("@DeviceStatusId", THINGSPACE_DEVICESTATUS_PENDINGACTIVATION);
                    cmd.Parameters.AddWithValue("@Status", targetStatus);
                    cmd.Parameters.AddWithValue("@CarrierRatePlanId", carrierRatePlanId);
                    cmd.Parameters.AddWithValue("@RatePlan", request.RatePlanCode);

                    cmd.ExecuteNonQuery();
                }

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "INSERT INTO Device",
                    HasErrors = false,
                    RequestObject = request,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing Query: {ex.Message} {ex.StackTrace}");

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "INSERT INTO Device",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = $"Error Executing Query. Ref: {logId}"
                };
            }
        }

        private static DeviceChangeResult<ThingSpaceStatusUpdateRequest, string> InsertNewThingSpaceDeviceUsage(KeySysLambdaContext context, ThingSpaceStatusUpdateRequest request, string targetStatus, SqlConnection conn)
        {
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "INSERT INTO ThingSpaceDeviceUsage(ICCID, IMEI, Status, RatePlan, CreatedBy, CreatedDate, IsActive, IsDeleted, DeviceStatusId) VALUES(@ICCID, @IMEI, @Status, @RatePlan, 'AltaworxDeviceBulkChange', GETUTCDATE(), 1, 0, @DeviceStatusId)";
                    cmd.Parameters.AddWithValue("@ICCID", request.ICCID);
                    cmd.Parameters.AddWithValue("@IMEI", request.IMEI);
                    cmd.Parameters.AddWithValue("@Status", targetStatus);
                    cmd.Parameters.AddWithValue("@RatePlan", request.RatePlanCode);
                    cmd.Parameters.AddWithValue("@DeviceStatusId", THINGSPACE_DEVICESTATUS_PENDINGACTIVATION);

                    cmd.ExecuteNonQuery();
                }

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "INSERT INTO ThingSpaceDeviceUsage",
                    HasErrors = false,
                    RequestObject = request,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing Query: {ex.Message} {ex.StackTrace}");

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "INSERT INTO ThingSpaceDeviceUsage",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = $"Error Executing Query. Ref: {logId}"
                };
            }
        }

        private static DeviceChangeResult<ThingSpaceStatusUpdateRequest, string> InsertNewThingSpaceDeviceDetail(KeySysLambdaContext context, ThingSpaceStatusUpdateRequest request, SqlConnection conn)
        {
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "INSERT INTO ThingSpaceDeviceDetail(ICCID, CreatedBy, CreatedDate, IsActive, IsDeleted, AccountNumber, ThingSpaceDateAdded) VALUES(@ICCID, 'AltaworxDeviceBulkChange', GETUTCDATE(), 1, 0, @AccountNumber, GETUTCDATE())";
                    cmd.Parameters.AddWithValue("@ICCID", request.ICCID);
                    cmd.Parameters.AddWithValue("@AccountNumber", request.RevAccountNumber ?? (object)DBNull.Value);

                    cmd.ExecuteNonQuery();
                }

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "INSERT INTO ThingSpaceDeviceDetail",
                    HasErrors = false,
                    RequestObject = request,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing Query: {ex.Message} {ex.StackTrace}");

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "INSERT INTO ThingSpaceDeviceDetail",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = $"Error Executing Query. Ref: {logId}"
                };
            }
        }

        private static DeviceChangeResult<ThingSpaceStatusUpdateRequest, string> InsertNewThingSpaceDevice(KeySysLambdaContext context, ThingSpaceStatusUpdateRequest request, int serviceProviderId, string targetStatus, SqlConnection conn)
        {
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = @"INSERT INTO ThingSpaceDevice(ICCID, Status, RatePlan, CreatedBy, CreatedDate, IsActive, IsDeleted, DeviceStatusId, ServiceProviderId) 
                                        VALUES(@ICCID, @Status, @RatePlan, 'AltaworxDeviceBulkChange', GETUTCDATE(), 1, 0, @DeviceStatusId, @ServiceProviderId)";
                    cmd.Parameters.AddWithValue("@ICCID", request.ICCID);
                    cmd.Parameters.AddWithValue("@Status", targetStatus);
                    cmd.Parameters.AddWithValue("@RatePlan", request.RatePlanCode);
                    cmd.Parameters.AddWithValue("@DeviceStatusId", THINGSPACE_DEVICESTATUS_PENDINGACTIVATION);
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                    cmd.ExecuteNonQuery();
                }

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "INSERT INTO ThingSpaceDevice",
                    HasErrors = false,
                    RequestObject = request,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing Query: {ex.Message} {ex.StackTrace}");

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "INSERT INTO ThingSpaceDevice",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = $"Error Executing Query. Ref: {logId}"
                };
            }
        }

        private static DeviceChangeResult<ThingSpaceStatusUpdateRequest, string> UpdateThingSpaceDeviceStatus(KeySysLambdaContext context, ThingSpaceStatusUpdateRequest request, int serviceProviderId, int targetStatusId, SqlConnection conn, string targetStatus)
        {
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = @"UPDATE ThingSpaceDeviceDetail 
                                        SET AccountNumber = @AccountNumber, 
                                        ModifiedBy = 'AltaworxDeviceBulkChange PendingStatus', 
                                        ModifiedDate = GETUTCDATE(), 
                                        IsDeleted = 0, 
                                        IsActive = 1 
                                        WHERE ServiceProviderId = @ServiceProviderId 
                                        AND ICCID = @ICCID

                                        UPDATE ThingSpaceDevice
                                        SET IMEI = @imei,
                                        DeviceStatusId = @deviceStatusId,
                                        Status = @status,
                                        ModifiedBy = 'AltaworxDeviceBulkChange PendingStatus', 
                                        ModifiedDate = GETUTCDATE(), 
                                        IsDeleted = 0, 
                                        IsActive = 1 
                                        WHERE ServiceProviderId = @ServiceProviderId 
                                        AND ICCID = @ICCID

                                        UPDATE Device
                                        SET IMEI = @imei,
                                        DeviceStatusId = @deviceStatusId,
                                        Status = @status,
                                        ModifiedBy = 'AltaworxDeviceBulkChange PendingStatus', 
                                        ModifiedDate = GETUTCDATE(), 
                                        IsDeleted = 0, 
                                        IsActive = 1 
                                        WHERE ServiceProviderId = @ServiceProviderId 
                                        AND ICCID = @ICCID";
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.Parameters.AddWithValue("@ICCID", request.ICCID);
                    cmd.Parameters.AddWithValue("@imei", request.IMEI);
                    cmd.Parameters.AddWithValue("@deviceStatusId", targetStatusId);
                    cmd.Parameters.AddWithValue("@status", targetStatus);
                    cmd.Parameters.AddWithValue("@AccountNumber", request.RevAccountNumber ?? (object)DBNull.Value);

                    cmd.ExecuteNonQuery();
                }

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "UPDATE ThingSpaceDeviceDetail",
                    HasErrors = false,
                    RequestObject = request,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing Query: {ex.Message} {ex.StackTrace}");

                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, string>()
                {
                    ActionText = "UPDATE ThingSpaceDeviceDetail",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = $"Error Executing Query. Ref: {logId}"
                };
            }
        }

        private async Task<DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>>
            ActivateThingSpaceDeviceAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, BulkChangeDetailRecord change, ThingSpaceAuthentication thingSpaceAuth,
            ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, ThingSpaceStatusUpdateRequest request, int serviceProviderId, bool useCallbackResult)
        {
            var apiResponse = new ApiResponse();

            if (thingSpaceAuth.WriteIsEnabled)
            {
                using (var client = new HttpClient(new LambdaLoggingHandler()))
                {
                    var baseUrl = thingSpaceAuth.BaseUrl.TrimEnd('/');
                    client.BaseAddress = new Uri(baseUrl + "/api/m2m/v1/devices/actions/activate");
                    LogInfo(context, "Endpoint", client.BaseAddress);
                    client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                    client.DefaultRequestHeaders.Add("Accept", "application/json");
                    client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);
                    var jsonDeviceContent = GetThingspaceActivateDeviceBody(thingSpaceAuth, request);
                    var contDevice = new StringContent(jsonDeviceContent, Encoding.UTF8, "application/json");
                    var response = await client.PostAsync(client.BaseAddress, contDevice);
                    var responseBody = await response.Content.ReadAsStringAsync();
                    if (response.IsSuccessStatusCode)
                    {
                        var updateDeviceStatusResult = JsonConvert.DeserializeObject<UpdateThingSpaceDeviceStatusResult>(responseBody);
                        if (string.IsNullOrWhiteSpace(updateDeviceStatusResult.requestId))
                        {
                            string errorMessage = $"Device Status for '{request.ICCID}' Not Updated";
                            LogInfo(context, "ERROR", errorMessage);
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = errorMessage,
                                HasErrors = true,
                                LogEntryDescription = "Activate ThingSpace Device: ThingSpace API",
                                M2MDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonDeviceContent,
                                ResponseStatus = ((int)response.StatusCode).ToString(),
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                            return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }

                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = null,
                            HasErrors = false,
                            LogEntryDescription = "Activate ThingSpace Device: ThingSpace API",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseBody
                        });

                        DeviceChangeResult<string, string> dbResult = new DeviceChangeResult<string, string>();
                        var thingSpaceCallBackLog = CheckRequestIdExist(context, updateDeviceStatusResult.requestId, bulkChange.TenantId, serviceProviderId);
                        if (!string.IsNullOrEmpty(request.thingSpacePPU.FirstName) && !string.IsNullOrEmpty(request.thingSpacePPU.LastName))
                        {
                            dbResult = await ActivatedWithThingSpacePPUThingSpaceDeviceAsync(request, thingSpaceAuth.BaseUrl, accessToken, sessionToken, context, serviceProviderId, updateDeviceStatusResult.requestId, ThingSpaceGetStatusRequestURL, thingSpaceAuth, bulkChange.Id, bulkChange.TenantId, thingSpaceCallBackLog, useCallbackResult);
                            if (!useCallbackResult || thingSpaceCallBackLog == null)
                            {
                                await SendMessageToCheckThingSpaceDeviceNewActivate(context, bulkChange.TenantId, bulkChange.Id, 900);
                                // delay 15 minutes, new activate device with PPU takes about 30minute 
                            }
                        }
                        else
                        {
                            dbResult = await UpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync(request, thingSpaceAuth.BaseUrl, accessToken, sessionToken, context, serviceProviderId, thingSpaceAuth.AccountNumber, updateDeviceStatusResult.requestId, bulkChange.Id, bulkChange.TenantId, thingSpaceCallBackLog, useCallbackResult);
                            if ((!dbResult.IsProcessed && !useCallbackResult) || thingSpaceCallBackLog == null)
                            {
                                await SendMessageToCheckThingSpaceDeviceNewActivate(context, 1, bulkChange.Id, 900);
                            }
                        }

                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = dbResult.HasErrors ? dbResult.ResponseObject : null,
                            HasErrors = dbResult.HasErrors,
                            LogEntryDescription = "Activate ThingSpace Device: Update DB with Details",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = dbResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                            ResponseText = dbResult.ResponseObject
                        });

                        // We treat errors from UpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync as warnings
                        // So, we use the API response for status here
                        apiResponse = new ApiResponse { IsSuccess = true, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = dbResult.HasErrors,
                            RequestObject = request,
                            ResponseObject = apiResponse
                        };
                    }
                    else
                    {
                        string errorMessage = $"Device '{request.ICCID}' not actviated. Respone: {responseBody}";
                        LogInfo(context, "ERROR", errorMessage);
                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = errorMessage,
                            HasErrors = true,
                            LogEntryDescription = "Activate ThingSpace Device: ThingSpace API",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseBody
                        });

                        apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = true,
                            RequestObject = request,
                            ResponseObject = apiResponse
                        };
                    }
                }
            }
            else
            {
                LogInfo(context, "WARN", "Writes disabled for service provider");

                apiResponse = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" };
                return new DeviceChangeResult<ThingSpaceStatusUpdateRequest, ApiResponse>()
                {
                    ActionText = $"ActivateThingSpaceDeviceAsync: General",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = apiResponse
                };
            }
        }

        private static string GetThingspaceAddDeviceBody(ThingSpaceAuthentication thingSpaceAuth, ThingSpaceStatusUpdateRequest request)
        {
            return JsonConvert.SerializeObject(
                new ThingSpaceAddDevicesRequest
                {
                    DevicesToAdd = new[]
                    {
                        new ThingSpaceDeviceRequest
                        {
                            DeviceIds = new List<ThingSpaceDeviceId>()
                            {
                                new ThingSpaceDeviceId
                                {
                                    Kind = "iccid",
                                    Id = request.ICCID
                                },
                                new ThingSpaceDeviceId
                                {
                                    Kind = "imei",
                                    Id = request.IMEI
                                }
                            }
                        }
                    },
                    AccountName = thingSpaceAuth.AccountNumber,
                    State = "preactive"
                }, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        }

        private static string GetThingspaceActivateDeviceBody(ThingSpaceAuthentication thingSpaceAuth, ThingSpaceStatusUpdateRequest request)
        {
            if (request.thingSpacePPU != null)
            {
                if (!string.IsNullOrEmpty(request.thingSpacePPU.FirstName) && !string.IsNullOrEmpty(request.thingSpacePPU.LastName))
                {
                    return ThingSpaceCommon.GetThingspaceActivateDeviceBody(thingSpaceAuth, request, true);
                }
            }
            return ThingSpaceCommon.GetThingspaceActivateDeviceBody(thingSpaceAuth, request, false);
        }

        private static string GetThingSpaceUpdateDeviceStatusBody(ThingSpaceAuthentication thingSpaceAuth, string iccid, string reasonCode = null)
        {
            return JsonConvert.SerializeObject(
                new ThingSpaceUpdateDeviceStatusRequest
                {
                    Devices = new[]
                    {
                        new ThingSpaceDeviceRequest
                        {
                            DeviceIds =  new List<ThingSpaceDeviceId>()
                            {
                                new ThingSpaceDeviceId
                                {
                                    Kind = "iccid",
                                    Id = iccid
                                }
                            }
                        }
                    },
                    AccountName = thingSpaceAuth.AccountNumber,
                    ReasonCode = reasonCode
                }, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        }

        private static string GetThingSpaceSuspendDeviceBody(ThingSpaceAuthentication thingSpaceAuth, string iccid)
        {
            return GetThingSpaceUpdateDeviceStatusBody(thingSpaceAuth, iccid);
        }

        private static string GetThingSpaceRestoreDeviceBody(ThingSpaceAuthentication thingSpaceAuth, string iccid)
        {
            return GetThingSpaceUpdateDeviceStatusBody(thingSpaceAuth, iccid);
        }

        private static string GetThingSpaceDeactivateDeviceBody(ThingSpaceAuthentication thingSpaceAuth, string iccid, string reasonCode)
        {
            return GetThingSpaceUpdateDeviceStatusBody(thingSpaceAuth, iccid, reasonCode);
        }

        private static async Task<DeviceChangeResult<string, ApiResponse>>
            DeactivateThingSpaceDeviceAsync(
                KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange,
                BulkChangeDetailRecord change, ThingSpaceAuthentication thingSpaceAuth, ThingSpaceTokenResponse accessToken,
                ThingSpaceLoginResponse sessionToken, string deviceIccid, string reasonCode)
        {
            string requestObject = $"ICCID: {deviceIccid}, ReasonCode: {reasonCode}";
            var apiResponse = new ApiResponse();
            if (thingSpaceAuth.WriteIsEnabled)
            {
                using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
                {
                    var baseUrl = thingSpaceAuth.BaseUrl.TrimEnd('/');
                    client.BaseAddress = new Uri(baseUrl + "/api/m2m/v1/devices/actions/deactivate");
                    LogInfo(context, "Endpoint", client.BaseAddress);
                    client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                    client.DefaultRequestHeaders.Add("Accept", "application/json");
                    client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);

                    string jsonDeviceContent = GetThingSpaceDeactivateDeviceBody(thingSpaceAuth, deviceIccid, reasonCode);
                    var contDevice = new StringContent(jsonDeviceContent, Encoding.UTF8, "application/json");
                    var response = await client.PostAsync(client.BaseAddress, contDevice);
                    var responseBody = await response.Content.ReadAsStringAsync();
                    if (response.IsSuccessStatusCode)
                    {
                        var updateDeviceStatusResult = JsonConvert.DeserializeObject<UpdateThingSpaceDeviceStatusResult>(responseBody);
                        if (string.IsNullOrWhiteSpace(updateDeviceStatusResult.requestId))
                        {
                            string errorMessage = $"Device Status for '{deviceIccid}' Not Updated";
                            LogInfo(context, "ERROR", errorMessage);
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = errorMessage,
                                HasErrors = true,
                                LogEntryDescription = "Deactivate ThingSpace Device: ThingSpace API",
                                M2MDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonDeviceContent,
                                ResponseStatus = ((int)response.StatusCode).ToString(),
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                            return new DeviceChangeResult<string, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = requestObject,
                                ResponseObject = apiResponse
                            };
                        }

                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = null,
                            HasErrors = false,
                            LogEntryDescription = "Deactivate ThingSpace Device: ThingSpace API",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseBody
                        });

                        apiResponse = new ApiResponse { IsSuccess = true, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<string, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = false,
                            RequestObject = requestObject,
                            ResponseObject = apiResponse
                        };
                    }
                    else
                    {
                        LogInfo(context, "Response Error", responseBody);
                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = $"Response Error: {responseBody}",
                            HasErrors = true,
                            LogEntryDescription = "Deactivate ThingSpace Device: ThingSpace API",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseBody
                        });

                        apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<string, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = true,
                            RequestObject = requestObject,
                            ResponseObject = apiResponse
                        };
                    }
                }
            }
            else
            {
                LogInfo(context, "WARN", "Writes disabled for service provider");

                apiResponse = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" };
                return new DeviceChangeResult<string, ApiResponse>()
                {
                    ActionText = $"DeactivateThingSpaceDeviceAsync: General",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = apiResponse
                };
            }
        }

        private static async Task<DeviceChangeResult<string, ApiResponse>>
            SuspendThingSpaceDeviceAsync(
                KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange,
                BulkChangeDetailRecord change, ThingSpaceAuthentication thingSpaceAuth,
                ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, string deviceIccid)
        {
            var apiResponse = new ApiResponse();
            if (thingSpaceAuth.WriteIsEnabled)
            {
                using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
                {
                    var baseUrl = thingSpaceAuth.BaseUrl.TrimEnd('/');
                    client.BaseAddress = new Uri(baseUrl + "/api/m2m/v1/devices/actions/suspend");
                    LogInfo(context, "Endpoint", client.BaseAddress);
                    client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                    client.DefaultRequestHeaders.Add("Accept", "application/json");
                    client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);

                    string jsonDeviceContent = GetThingSpaceSuspendDeviceBody(thingSpaceAuth, deviceIccid);
                    var contDevice = new StringContent(jsonDeviceContent, Encoding.UTF8, "application/json");
                    var response = await client.PostAsync(client.BaseAddress, contDevice);
                    string responseBody = await response.Content.ReadAsStringAsync();
                    if (response.IsSuccessStatusCode)
                    {
                        var updateDeviceStatusResult = JsonConvert.DeserializeObject<UpdateThingSpaceDeviceStatusResult>(responseBody);
                        if (string.IsNullOrWhiteSpace(updateDeviceStatusResult.requestId))
                        {
                            string errorMessage = $"Device Status for '{deviceIccid}' Not Updated";
                            LogInfo(context, "ERROR", errorMessage);
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = errorMessage,
                                HasErrors = true,
                                LogEntryDescription = "Suspend ThingSpace Device: ThingSpace API",
                                M2MDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonDeviceContent,
                                ResponseStatus = ((int)response.StatusCode).ToString(),
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                            return new DeviceChangeResult<string, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = deviceIccid,
                                ResponseObject = apiResponse
                            };
                        }

                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = null,
                            HasErrors = false,
                            LogEntryDescription = "Suspend ThingSpace Device: ThingSpace API",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseBody
                        });

                        apiResponse = new ApiResponse { IsSuccess = true, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<string, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = false,
                            RequestObject = deviceIccid,
                            ResponseObject = apiResponse
                        };
                    }
                    else
                    {
                        LogInfo(context, "Response Error", responseBody);
                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = $"Response Error: {responseBody}",
                            HasErrors = true,
                            LogEntryDescription = "Suspend ThingSpace Device: ThingSpace API",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseBody
                        });

                        apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<string, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = true,
                            RequestObject = deviceIccid,
                            ResponseObject = apiResponse
                        };
                    }
                }
            }
            else
            {
                LogInfo(context, "WARN", "Writes disabled for service provider");

                apiResponse = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" };
                return new DeviceChangeResult<string, ApiResponse>()
                {
                    ActionText = $"SuspendThingSpaceDeviceAsync: General",
                    HasErrors = true,
                    RequestObject = deviceIccid,
                    ResponseObject = apiResponse
                };
            }
        }

        private static async Task<DeviceChangeResult<string, ApiResponse>>
            RestoreThingSpaceDeviceAsync(
                KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange,
                BulkChangeDetailRecord change, ThingSpaceAuthentication thingSpaceAuth,
                ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, string deviceIccid)
        {
            ApiResponse apiResponse;
            if (thingSpaceAuth.WriteIsEnabled)
            {
                using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
                {
                    var baseUrl = thingSpaceAuth.BaseUrl.TrimEnd('/');
                    client.BaseAddress = new Uri(baseUrl + "/api/m2m/v1/devices/actions/restore");
                    LogInfo(context, "Endpoint", client.BaseAddress);
                    client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                    client.DefaultRequestHeaders.Add("Accept", "application/json");
                    client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);

                    string jsonDeviceContent = GetThingSpaceRestoreDeviceBody(thingSpaceAuth, deviceIccid);
                    var contDevice = new StringContent(jsonDeviceContent, Encoding.UTF8, "application/json");
                    var response = await client.PostAsync(client.BaseAddress, contDevice);
                    var responseBody = await response.Content.ReadAsStringAsync();
                    if (response.IsSuccessStatusCode)
                    {
                        var updateDeviceStatusResult = JsonConvert.DeserializeObject<UpdateThingSpaceDeviceStatusResult>(responseBody);
                        if (string.IsNullOrWhiteSpace(updateDeviceStatusResult.requestId))
                        {
                            string errorMessage = $"Device Status for '{deviceIccid}' Not Updated";
                            LogInfo(context, "ERROR", errorMessage);
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = errorMessage,
                                HasErrors = true,
                                LogEntryDescription = "Restore ThingSpace Device: ThingSpace API",
                                M2MDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonDeviceContent,
                                ResponseStatus = ((int)response.StatusCode).ToString(),
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                            return new DeviceChangeResult<string, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = deviceIccid,
                                ResponseObject = apiResponse
                            };
                        }

                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = null,
                            HasErrors = false,
                            LogEntryDescription = "Restore ThingSpace Device: ThingSpace API",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseBody
                        });

                        apiResponse = new ApiResponse { IsSuccess = true, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<string, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = false,
                            RequestObject = deviceIccid,
                            ResponseObject = apiResponse
                        };
                    }
                    else
                    {
                        LogInfo(context, "Response Error", responseBody);
                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChange.Id,
                            ErrorText = $"Response Error: {responseBody}",
                            HasErrors = true,
                            LogEntryDescription = "Restore ThingSpace Device: ThingSpace API",
                            M2MDeviceChangeId = change.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = jsonDeviceContent,
                            ResponseStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseBody
                        });

                        apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody, StatusCode = response.StatusCode };
                        return new DeviceChangeResult<string, ApiResponse>()
                        {
                            ActionText = $"POST {client.BaseAddress}",
                            HasErrors = true,
                            RequestObject = deviceIccid,
                            ResponseObject = apiResponse
                        };
                    }
                }
            }
            else
            {
                LogInfo(context, "WARN", "Writes disabled for service provider");

                apiResponse = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" };
                return new DeviceChangeResult<string, ApiResponse>()
                {
                    ActionText = $"RestoreThingSpaceDeviceAsync: General",
                    HasErrors = true,
                    RequestObject = deviceIccid,
                    ResponseObject = apiResponse
                };
            }
        }

        private static void MarkProcessed(KeySysLambdaContext context, long bulkChangeId, long changeDetailId, bool apiResult, int newDeviceStatusId, string statusDetails, bool isProcessed = true)
        {
            LogInfo(context, "SUB", $"MarkProcessed({bulkChangeId},{changeDetailId},{apiResult},{newDeviceStatusId},{statusDetails},{isProcessed})");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "dbo.usp_DeviceBulkChange_StatusUpdate_UpdateDeviceRecords";
                    cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                    cmd.Parameters.AddWithValue("@changeDetailId", changeDetailId);
                    cmd.Parameters.AddWithValue("@apiCallResult", apiResult ? 1 : 0);
                    cmd.Parameters.AddWithValue("@newDeviceStatusId", newDeviceStatusId);
                    cmd.Parameters.AddWithValue("@statusDetails", statusDetails);
                    cmd.Parameters.AddWithValue("@IsProcessed", isProcessed ? 1 : 0);
                    using (var jasperCn = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
                    {
                        cmd.Parameters.AddWithValue("@JasperDbName", jasperCn.Database);
                    }
                    cmd.CommandTimeout = 900;
                    conn.Open();

                    cmd.ExecuteNonQuery();

                    conn.Close();
                }
            }
        }

        public static async Task MarkProcessedForMobilityDeviceChangeAsync(KeySysLambdaContext context, long changeId, bool apiResult, string statusDetails)
        {
            LogInfo(context, "SUB", $"MarkProcessedForMobilityDeviceChangeAsync({changeId},{apiResult},{statusDetails})");
            await using var conn = new SqlConnection(context.CentralDbConnectionString);
            await using var cmd =
                new SqlCommand("usp_DeviceBulkChange_UpdateMobilityDeviceChange", conn)
                {
                    CommandType = CommandType.StoredProcedure
                };
            cmd.Parameters.AddWithValue("@Id", changeId);
            cmd.Parameters.AddWithValue("@apiCallResult", apiResult ? 1 : 0);
            cmd.Parameters.AddWithValue("@statusDetails", statusDetails);
            conn.Open();
            await cmd.ExecuteNonQueryAsync();
        }

        public virtual async Task MarkProcessedForM2MDeviceChangeAsync(KeySysLambdaContext context, long changeId, bool apiResult, string statusDetails, bool isDeviceChangeProcessing = false)
        {
            LogInfo(context, CommonConstants.SUB, $"({changeId},{apiResult},{statusDetails})");

            var deviceChangeStatus = BulkChangeStatus.PROCESSED;
            if (!apiResult)
            {
                deviceChangeStatus = BulkChangeStatus.API_FAILED;
            }

            if (isDeviceChangeProcessing)
            {
                deviceChangeStatus = BulkChangeStatus.PROCESSING;
            }

            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.ID, changeId),
                new SqlParameter(CommonSQLParameterNames.API_CALL_RESULT, apiResult),
                new SqlParameter(CommonSQLParameterNames.DEVICE_CHANGE_STATUS, deviceChangeStatus),
                new SqlParameter(CommonSQLParameterNames.STATUS_DETAILS, statusDetails)
            };
            Amop.Core.Helpers.SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult(ParameterizedLog(context),
                            context.CentralDbConnectionString,
                            Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_UPDATE_M2M_DEVICE_CHANGE,
                            parameters,
                            commandTimeout: Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds);
        }

        //Mark a list of changes to Processed or Error based on the Bulk change id and a list of iccid.
        public static async Task MarkProcessedForNewServiceActivationAsync(KeySysLambdaContext context, long bulkChangeId, bool apiResult, string statusDetails, List<string> iccidList, int? serviceProviderId = null, string userName = "")
        {
            var policyFactory = new PolicyFactory(context.logger);
            var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES);
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.BULK_CHANGE_ID_PASCAL_CASE, bulkChangeId),
                new SqlParameter(CommonSQLParameterNames.API_CALL_RESULT_PASCAL_CASE, apiResult ? 1 : 0),
                new SqlParameter(CommonSQLParameterNames.STATUS_DETAILS_PASCAL_CASE, statusDetails),
                new SqlParameter(CommonSQLParameterNames.ICCID_LIST, iccidList != null ? string.Join(',', iccidList) : null),
            };
            if (serviceProviderId != null)
            {
                parameters.Add(new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId));
            }
            if (userName != null)
            {
                parameters.Add(new SqlParameter(CommonSQLParameterNames.USERNAME, userName));
            }
            sqlRetryPolicy.Execute(() =>
                 Amop.Core.Helpers.SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult(ParameterizedLog(context),
                    context.CentralDbConnectionString,
                    Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_NEW_SERVICE_ACTIVATION_UPDATE_MOBILITY_CHANGE,
                    parameters,
                    Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds));
        }

        public static async Task MarkProcessedForNewServiceActivationByICCIDAsync(KeySysLambdaContext context, long bulkChangeId, bool apiResult, string statusDetails, string iccid, string subscriberNumber = null, string deviceChangeStatus = null)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({bulkChangeId},{apiResult},{statusDetails},{iccid},{subscriberNumber}, {deviceChangeStatus})");

            try
            {
                using (var conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand(Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_SERVICE_ACTIVATION_UPDATE_MOBILITY_CHANGE_BY_ICCID, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                        cmd.Parameters.AddWithValue("@apiCallResult", apiResult ? 1 : 0);
                        cmd.Parameters.AddWithValue("@statusDetails", statusDetails);
                        cmd.Parameters.AddWithValue("@ICCID", iccid);
                        cmd.Parameters.AddWithValue("@subscriberNumber", subscriberNumber);
                        cmd.Parameters.AddWithValue("@deviceChangeStatus", deviceChangeStatus);
                        cmd.CommandTimeout = Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds;

                        var affectedRows = await cmd.ExecuteNonQueryAsync();
                        if (affectedRows >= 1)
                        {
                            LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.UPDATED_DEVICE_CHANGE_SUCCESSFULLY, LogCommonStrings.MOBILITY_DEVICE_CHANGE, iccid));
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, ex.Message));
                throw ex;
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
                throw ex;
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"{ex.Message} - {ex.StackTrace}");
                throw ex;
            }
        }

        public static async Task<DeviceChangeResult<CarrierRatePlanChange, string>> UpdateTelegenceDeviceCarrierRatePlan(KeySysLambdaContext context, CarrierRatePlanChange change, ISyncPolicy syncPolicy)
        {
            LogInfo(context, CommonConstants.SUB);
            try
            {
                var parameters = new List<SqlParameter>()
                {
                    new SqlParameter(CommonSQLParameterNames.PHONE_NUMBER, change.PhoneNumber),
                    new SqlParameter(CommonSQLParameterNames.CARRIER_RATE_PLAN, change.RatePlanName),
                    new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, change.ServiceProviderId),
                    new SqlParameter(CommonSQLParameterNames.OPTIMIZATION_GROUP, change.OptimizationGroup)
                };
                syncPolicy.Execute(() =>
                    Amop.Core.Helpers.SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult(ParameterizedLog(context),
                    context.CentralDbConnectionString,
                    Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_TELEGENCE_CARRIER_RATE_PLAN_CHANGE,
                    parameters,
                    Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds));

                return new DeviceChangeResult<CarrierRatePlanChange, string>()
                {
                    ActionText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_TELEGENCE_CARRIER_RATE_PLAN_CHANGE,
                    HasErrors = false,
                    RequestObject = change,
                    ResponseObject = CommonConstants.RESPONSE_OK
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.ERROR_WHEN_EXECUTING_STORED_PROCEDURE, logId, ex.Message, ex.StackTrace));
                return new DeviceChangeResult<CarrierRatePlanChange, string>()
                {
                    ActionText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_TELEGENCE_CARRIER_RATE_PLAN_CHANGE,
                    HasErrors = true,
                    RequestObject = change,
                    ResponseObject = string.Format(LogCommonStrings.ERROR_WHILE_EXECUTING_STORED_PROCEDURE_REF, logId)
                };
            }
        }

        public static async Task<DeviceChangeResult<CarrierRatePlanChange, string>> UpdateEBondingDeviceCarrierRatePlan(KeySysLambdaContext context, CarrierRatePlanChange change)
        {
            LogInfo(context, "SUB", "UpdateEBondingDeviceCarrierRatePlan()");
            try
            {
                await using var conn = new SqlConnection(context.CentralDbConnectionString);
                await using var cmd =
                    new SqlCommand("usp_DeviceBulkChange_eBonding_CarrierRatePlanChange", conn)
                    {
                        CommandType = CommandType.StoredProcedure
                    };
                cmd.Parameters.AddWithValue("@PhoneNumber", change.PhoneNumber);
                cmd.Parameters.AddWithValue("@CarrierRatePlan", change.RatePlanName);
                cmd.Parameters.AddWithValue("@ServiceProviderId", change.ServiceProviderId);
                cmd.Parameters.AddWithValue("@EffectiveDate", change.EffectiveDate);
                conn.Open();
                await cmd.ExecuteNonQueryAsync();

                return new DeviceChangeResult<CarrierRatePlanChange, string>()
                {
                    ActionText = "usp_DeviceBulkChange_eBonding_CarrierRatePlanChange",
                    HasErrors = false,
                    RequestObject = change,
                    ResponseObject = "OK"
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing usp_DeviceBulkChange_eBonding_CarrierRatePlanChange: {ex.Message} {ex.StackTrace}");
                return new DeviceChangeResult<CarrierRatePlanChange, string>()
                {
                    ActionText = "usp_DeviceBulkChange_eBonding_CarrierRatePlanChange",
                    HasErrors = true,
                    RequestObject = change,
                    ResponseObject = $"Error Executing Stored Procedure. Ref: {logId}"
                };
            }
        }

        private static bool QueueHasMoreItems(KeySysLambdaContext context, long bulkChangeId)
        {
            bool hasMoreItems;
            using (var connection = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "dbo.usp_DeviceBulkChange_UnprocessedChange_Exists";
                    cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                    connection.Open();

                    object itemExists = cmd.ExecuteScalar();

                    hasMoreItems = (bool)itemExists;
                    connection.Close();
                }
            }

            return hasMoreItems;
        }

        public static async Task<DeviceChangeResult<List<TelegenceActivationRequest>, ApiResponse>> UpdateTelegenceDeviceStatusAsync(
            IKeysysLogger logger, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, BulkChangeDetailRecord change,
            Base64Service base64Service, TelegenceAuthentication telegenceAuthentication,
            bool isProduction, List<TelegenceActivationRequest> request, string endpoint, string proxyUrl)
        {
            var apiResponse = new ApiResponse();
            if (telegenceAuthentication.WriteIsEnabled)
            {
                var decodedPassword = base64Service.Base64Decode(telegenceAuthentication.Password);

                using (var client = new HttpClient(new LambdaLoggingHandler()))
                {
                    Uri baseUrl = new Uri(telegenceAuthentication.SandboxUrl);
                    if (isProduction)
                    {
                        baseUrl = new Uri(telegenceAuthentication.ProductionUrl);
                    }

                    if (!string.IsNullOrWhiteSpace(proxyUrl))
                    {
                        var headerContent = new ExpandoObject() as IDictionary<string, object>;
                        headerContent.Add("app-id", telegenceAuthentication.ClientId);
                        headerContent.Add("app-secret", decodedPassword);
                        var headerContentString = JsonConvert.SerializeObject(headerContent);
                        var jsonContentString = JsonConvert.SerializeObject(request, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

                        var payload = new PayloadModel()
                        {
                            AuthenticationType = AuthenticationType.TELEGENCEAUTH,
                            Endpoint = endpoint,
                            HeaderContent = headerContentString,
                            JsonContent = jsonContentString,
                            Password = null,
                            Token = null,
                            Url = baseUrl.ToString(),
                            Username = null
                        };

                        var result = client.PostWithProxy(proxyUrl, payload, logger);
                        if (result.IsSuccessful)
                        {
                            logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = null,
                                HasErrors = false,
                                LogEntryDescription = "Update Telegence Device Status: Telegence API",
                                MobilityDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonContentString,
                                ResponseStatus = result.StatusCode,
                                ResponseText = result.ResponseMessage
                            });

                            apiResponse = new ApiResponse { IsSuccess = true, Response = result.ResponseMessage };
                            return new DeviceChangeResult<List<TelegenceActivationRequest>, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = false,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                        else
                        {
                            string responseBody = result.ResponseMessage;
                            logger.LogInfo("UpdateTelegenceDeviceStatus", $"Proxy call to {endpoint} failed.");
                            logger.LogInfo("Response Error", responseBody);

                            logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = $"Proxy call to {endpoint} failed.",
                                HasErrors = true,
                                LogEntryDescription = "Update Telegence Device Status: Telegence API",
                                MobilityDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonContentString,
                                ResponseStatus = result.StatusCode,
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody };
                            return new DeviceChangeResult<List<TelegenceActivationRequest>, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                    }
                    else
                    {
                        client.BaseAddress = baseUrl;
                        client.DefaultRequestHeaders.Add("app-id", telegenceAuthentication.ClientId);
                        client.DefaultRequestHeaders.Add("app-secret", decodedPassword);

                        var payloadAsJson = JsonConvert.SerializeObject(request);
                        var content = new StringContent(payloadAsJson, Encoding.UTF8, "application/json");

                        try
                        {
                            var response = await client.PostAsync(endpoint, content);
                            string responseBody = await response.Content.ReadAsStringAsync();
                            if (response.IsSuccessStatusCode)
                            {
                                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                                {
                                    BulkChangeId = bulkChange.Id,
                                    ErrorText = null,
                                    HasErrors = false,
                                    LogEntryDescription = "Update Telegence Device Status: Telegence API",
                                    MobilityDeviceChangeId = change.Id,
                                    ProcessBy = "AltaworxDeviceBulkChange",
                                    ProcessedDate = DateTime.UtcNow,
                                    RequestText = payloadAsJson,
                                    ResponseStatus = ((int)response.StatusCode).ToString(),
                                    ResponseText = responseBody
                                });

                                apiResponse = new ApiResponse { IsSuccess = true, Response = responseBody };
                                return new DeviceChangeResult<List<TelegenceActivationRequest>, ApiResponse>()
                                {
                                    ActionText = $"POST {client.BaseAddress}",
                                    HasErrors = false,
                                    RequestObject = request,
                                    ResponseObject = apiResponse
                                };
                            }
                            else
                            {
                                logger.LogInfo("UpdateTelegenceDeviceStatus", $"Call to {endpoint} failed.");
                                logger.LogInfo("Response Body", responseBody);
                                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                                {
                                    BulkChangeId = bulkChange.Id,
                                    ErrorText = $"Call to {endpoint} failed.",
                                    HasErrors = true,
                                    LogEntryDescription = "Update Telegence Device Status: Telegence API",
                                    MobilityDeviceChangeId = change.Id,
                                    ProcessBy = "AltaworxDeviceBulkChange",
                                    ProcessedDate = DateTime.UtcNow,
                                    RequestText = payloadAsJson,
                                    ResponseStatus = ((int)response.StatusCode).ToString(),
                                    ResponseText = responseBody
                                });

                                apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody };
                                return new DeviceChangeResult<List<TelegenceActivationRequest>, ApiResponse>()
                                {
                                    ActionText = $"POST {client.BaseAddress}",
                                    HasErrors = true,
                                    RequestObject = request,
                                    ResponseObject = apiResponse
                                };
                            }
                        }
                        catch (Exception e)
                        {
                            logger.LogInfo("UpdateTelegenceDeviceStatus", $"Call to {endpoint} failed.");
                            logger.LogInfo("ERROR", e.Message);

                            logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = $"Call to {endpoint} failed.",
                                HasErrors = true,
                                LogEntryDescription = "Update Telegence Device Status: Telegence API",
                                MobilityDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = payloadAsJson,
                                ResponseStatus = BulkChangeStatus.ERROR,
                                ResponseText = e.Message
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = e.Message };
                            return new DeviceChangeResult<List<TelegenceActivationRequest>, ApiResponse>()
                            {
                                ActionText = $"POST {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                    }
                }
            }
            else
            {
                logger.LogInfo("WARN", "Writes disabled for service provider");

                apiResponse = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" };
                return new DeviceChangeResult<List<TelegenceActivationRequest>, ApiResponse>()
                {
                    ActionText = $"Update Telegence Device Status: General",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = apiResponse
                };
            }
        }

        public static async Task<DeviceChangeResult<TelegenceSubscriberUpdateRequest, ApiResponse>>
            UpdateTelegenceSubscriberAsync(IKeysysLogger logger,
                DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, BulkChangeDetailRecord change,
                Base64Service base64Service, TelegenceAuthentication telegenceAuthentication, bool isProduction,
                TelegenceSubscriberUpdateRequest request, string subscriberNo, string endpoint, string proxyUrl)
        {
            var apiResponse = new ApiResponse();
            if (telegenceAuthentication.WriteIsEnabled)
            {
                var decodedPassword = base64Service.Base64Decode(telegenceAuthentication.Password);

                var subscriberUpdateURL = endpoint + subscriberNo;

                using (var client = new HttpClient(new LambdaLoggingHandler()))
                {
                    Uri baseUrl = new Uri(telegenceAuthentication.SandboxUrl);
                    if (isProduction)
                    {
                        baseUrl = new Uri(telegenceAuthentication.ProductionUrl);
                    }

                    if (!string.IsNullOrWhiteSpace(proxyUrl))
                    {
                        var headerContent = new ExpandoObject() as IDictionary<string, object>;
                        headerContent.Add("app-id", telegenceAuthentication.ClientId);
                        headerContent.Add("app-secret", decodedPassword);
                        var headerContentString = JsonConvert.SerializeObject(headerContent);
                        var jsonContentString = JsonConvert.SerializeObject(request, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

                        var payload = new PayloadModel
                        {
                            AuthenticationType = AuthenticationType.TELEGENCEAUTH,
                            Endpoint = subscriberUpdateURL,
                            HeaderContent = headerContentString,
                            JsonContent = jsonContentString,
                            Password = null,
                            Token = null,
                            Url = baseUrl.ToString(),
                            Username = null
                        };

                        var result = client.PatchWithProxy(proxyUrl, payload, logger);
                        if (result.IsSuccessful)
                        {
                            logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = null,
                                HasErrors = false,
                                LogEntryDescription = "Update Telegence Subscriber: Telegence API",
                                MobilityDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonContentString,
                                ResponseStatus = result.StatusCode,
                                ResponseText = result.ResponseMessage
                            });

                            apiResponse = new ApiResponse { IsSuccess = true, Response = result.ResponseMessage };
                            return new DeviceChangeResult<TelegenceSubscriberUpdateRequest, ApiResponse>()
                            {
                                ActionText = $"PATCH {client.BaseAddress}",
                                HasErrors = false,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                        else
                        {
                            string responseBody = result.ResponseMessage;
                            logger.LogInfo("UpdateTelegenceSubscriber", $"Proxy call to {endpoint} failed.");
                            logger.LogInfo("Response Error", responseBody);

                            logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = $"Proxy call to {endpoint} failed.",
                                HasErrors = true,
                                LogEntryDescription = "Update Telegence Subscriber: Telegence API",
                                MobilityDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = jsonContentString,
                                ResponseStatus = result.StatusCode,
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = responseBody };
                            return new DeviceChangeResult<TelegenceSubscriberUpdateRequest, ApiResponse>()
                            {
                                ActionText = $"PATCH {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                    }
                    else
                    {
                        client.BaseAddress = new Uri(baseUrl + subscriberUpdateURL);
                        client.DefaultRequestHeaders.Add("app-id", telegenceAuthentication.ClientId);
                        client.DefaultRequestHeaders.Add("app-secret", decodedPassword);

                        var payloadAsJson = JsonConvert.SerializeObject(request);
                        var content = new StringContent(payloadAsJson, Encoding.UTF8, "application/json");

                        try
                        {
                            var response = client.Patch(client.BaseAddress, content);
                            var responseBody = await response.Content.ReadAsStringAsync();

                            logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = null,
                                HasErrors = !response.IsSuccessStatusCode,
                                LogEntryDescription = "Update Telegence Subscriber: Telegence API",
                                MobilityDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = payloadAsJson,
                                ResponseStatus = ((int)response.StatusCode).ToString(),
                                ResponseText = responseBody
                            });

                            apiResponse = new ApiResponse
                            {
                                IsSuccess = response.IsSuccessStatusCode,
                                StatusCode = response.StatusCode,
                                Response = responseBody
                            };

                            return new DeviceChangeResult<TelegenceSubscriberUpdateRequest, ApiResponse>()
                            {
                                ActionText = $"PATCH {client.BaseAddress}",
                                HasErrors = !response.IsSuccessStatusCode,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                        catch (Exception e)
                        {
                            logger.LogInfo("UpdateTelegenceSubscriber", $"Call to {endpoint} failed.");
                            logger.LogInfo("ERROR", e.Message);

                            logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                            {
                                BulkChangeId = bulkChange.Id,
                                ErrorText = $"Call to {endpoint} failed.",
                                HasErrors = true,
                                LogEntryDescription = "Update Telegence Subscriber: Telegence API",
                                MobilityDeviceChangeId = change.Id,
                                ProcessBy = "AltaworxDeviceBulkChange",
                                ProcessedDate = DateTime.UtcNow,
                                RequestText = payloadAsJson,
                                ResponseStatus = BulkChangeStatus.ERROR,
                                ResponseText = e.Message
                            });

                            apiResponse = new ApiResponse { IsSuccess = false, Response = e.Message };
                            return new DeviceChangeResult<TelegenceSubscriberUpdateRequest, ApiResponse>()
                            {
                                ActionText = $"PATCH {client.BaseAddress}",
                                HasErrors = true,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                    }
                }
            }
            else
            {
                logger.LogInfo("WARN", "Writes disabled for service provider");

                apiResponse = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" };
                return new DeviceChangeResult<TelegenceSubscriberUpdateRequest, ApiResponse>()
                {
                    ActionText = $"Update Telegence Subscriber: General",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = apiResponse
                };
            }
        }

        private static async Task<int> GetThingspaceDeviceCountAsync(SqlConnection conn, string iccid, int serviceProviderId)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandText =
                    "SELECT COUNT(1) FROM Device WHERE IsDeleted = 0 AND ServiceProviderId = @ServiceProviderId AND ICCID = @ICCID";
                cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                cmd.Parameters.AddWithValue("@ICCID", iccid);

                object oDeviceCount = await cmd.ExecuteScalarAsync();
                if (oDeviceCount != null && oDeviceCount != DBNull.Value)
                {
                    return int.Parse(oDeviceCount.ToString());
                }
            }

            return 0;
        }

        private static async Task<int> GetThingspaceDeviceDetailCountAsync(SqlConnection conn, string iccid)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandText =
                    "SELECT COUNT(1) FROM ThingSpaceDeviceDetail WHERE IsDeleted = 0 AND ICCID = @ICCID";
                cmd.Parameters.AddWithValue("@ICCID", iccid);

                object oDeviceCount = await cmd.ExecuteScalarAsync();
                if (oDeviceCount != null && oDeviceCount != DBNull.Value)
                {
                    return int.Parse(oDeviceCount.ToString());
                }
            }

            return 0;
        }

        private static async Task<int> GetThingspaceDeviceUsageCountAsync(SqlConnection conn, string iccid)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandText =
                    "SELECT COUNT(1) FROM ThingSpaceDeviceUsage WHERE IsDeleted = 0 AND ICCID = @ICCID";
                cmd.Parameters.AddWithValue("@ICCID", iccid);

                object oDeviceCount = await cmd.ExecuteScalarAsync();
                if (oDeviceCount != null && oDeviceCount != DBNull.Value)
                {
                    return int.Parse(oDeviceCount.ToString());
                }
            }

            return 0;
        }

        public virtual TelegenceAPIAuthentication GetTelegenceApiAuthentication(string connectionString, int serviceProviderId)
        {
            var telegenceAuthenticationRepository =
                new TelegenceAuthenticationRepository(connectionString, new Base64Service());
            return telegenceAuthenticationRepository.GetTelegenceApiAuthentication(serviceProviderId);
        }

        private eBondingAuthentication GetEBondingAuthentication(string connectionString, int serviceProviderId)
        {
            var eBondingAuthenticationRepository = new AuthenticationRepository(connectionString);
            return eBondingAuthenticationRepository.GeteBondingAuthentication(serviceProviderId);
        }

        private RatePlanService GetEBondingRatePlanService(eBondingAuthentication eBondingAuthentication, bool isProd)
        {
            var eBondingUrl = isProd
                ? eBondingAuthentication.ProductionUrl
                : eBondingAuthentication.SandboxUrl;

            return new RatePlanService(new Uri($"{eBondingUrl}/soap/WsRouter"), eBondingAuthentication.Username, eBondingAuthentication.Password,
                eBondingAuthentication.AccountGroupId, eBondingAuthentication.AgreementId);
        }

        private IAsyncPolicy GetSqlTransientAsyncRetryPolicy(KeySysLambdaContext context)
        {
            var policyFactory = new PolicyFactory(context.logger);
            return policyFactory.GetSqlAsyncRetryPolicy(SQL_TRANSIENT_RETRY_MAX_COUNT);
        }

        private ISyncPolicy GetSqlTransientRetryPolicy(KeySysLambdaContext context)
        {
            var policyFactory = new PolicyFactory(context.logger);
            return policyFactory.GetSqlRetryPolicy(SQL_TRANSIENT_RETRY_MAX_COUNT);
        }

        private IAsyncPolicy GetHttpRetryPolicy(KeySysLambdaContext context)
        {
            var policyFactory = new PolicyFactory(context.logger);
            return policyFactory.GetHttpRetryPolicy(HTTP_RETRY_MAX_COUNT);
        }

        private async Task SendMessageToCheckThingSpaceDeviceNewActivate(KeySysLambdaContext context, int retryNumber, long bulkchangeId, int delaySeconds)
        {
            LogInfo(context, "SUB", "SendMessageToCheckThingSpaceDeviceNewActivate");
            LogInfo(context, "SUB", $"Retry Number: {retryNumber}");
            LogInfo(context, "SUB", $"Bulk Change Id: {bulkchangeId}");

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Get ThingSpace device after new activate device";
                LogInfo(context, "Sending message for", requestMsgBody);
                var request = new SendMessageRequest
                {
                    DelaySeconds = delaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "BulkChangeId", new MessageAttributeValue
                            { DataType = "String", StringValue = bulkchangeId.ToString()}
                        },
                        {
                            "IsRetryNewActivateThingSpaceDevice", new MessageAttributeValue
                            { DataType = "String", StringValue = true.ToString()}
                        },
                        {
                            "RetryNumber", new MessageAttributeValue
                                { DataType = "String", StringValue = retryNumber.ToString()}
                        }
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = DeviceBulkChangeQueueUrl
                };
                LogInfo(context, "STATUS", "SendMessageRequest is ready!");
                LogInfo(context, "MessageBody", request.MessageBody);
                LogInfo(context, "QueueURL", request.QueueUrl);
                var response = await client.SendMessageAsync(request);
                if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
                {
                    LogInfo(context, "EXCEPTION", $"Error enqueuing message to {DeviceBulkChangeQueueUrl}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                }
            }
        }
        private static ThingSpaceCallBackResponseLog GetThingSpaceCallBackResponseLog(KeySysLambdaContext context, string requestId, int tenantId, int serviceProviderId)
        {
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText =
                        "SELECT [RequestId], [APIStatus], [APIResponse], [TenantId], [ServiceProviderId] FROM [ThingSpaceCallBackResponseLog] WHERE [RequestId] = @RequestId AND TenantId = @TenantId AND [ServiceProviderId] = @ServiceProviderId";
                    cmd.Parameters.AddWithValue("@RequestId", requestId);
                    cmd.Parameters.AddWithValue("@TenantId", tenantId);
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.CommandTimeout = 800;
                    conn.Open();

                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        return ProcessedThingSpaceCallBackFromReader(rdr);
                    }
                }
            }
            return null;
        }
        private static ThingSpaceCallBackResponseLog ProcessedThingSpaceCallBackFromReader(IDataRecord rdr)
        {
            return new ThingSpaceCallBackResponseLog
            {
                RequestId = rdr["RequestId"] != DBNull.Value ? rdr["RequestId"].ToString() : string.Empty,
                APIStatus = rdr["APIStatus"] != DBNull.Value ? rdr["APIStatus"].ToString() : string.Empty,
                APIResponse = rdr["APIResponse"] != DBNull.Value ? rdr["APIResponse"].ToString() : string.Empty,
                TenantId = int.TryParse(rdr["TenantId"].ToString(), out var tenantId) ? tenantId : default,
                ServiceProviderId = int.TryParse(rdr["ServiceProviderId"].ToString(), out var serviceProviderId) ? serviceProviderId : default
            };
        }

        private static DeviceChangeResult<string, string> UpdateThingSpaceDeviceInformationCallbackFlow(KeySysLambdaContext context, string requestObject, string iccid, string ipAddress, long bulkChangeId, int serviceProviderId, string phoneNumber, string description, string userName)
        {
            try
            {
                using (var conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    conn.Open();

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = @"usp_DeviceBulkChange_ThingSpace_InformationChange";
                        cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                        cmd.Parameters.AddWithValue("@ICCID", iccid);
                        cmd.Parameters.AddWithValue("@phoneNumber", phoneNumber);
                        cmd.Parameters.AddWithValue("@ipAddress", string.IsNullOrEmpty(ipAddress) ? string.Empty : ipAddress);
                        cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                        cmd.Parameters.AddWithValue("@DeviceStatusId", THINGSPACE_DEVICESTATUSID_ACTIVE);
                        cmd.Parameters.AddWithValue("@username", string.IsNullOrEmpty(userName) ? string.Empty : userName);
                        cmd.Parameters.AddWithValue("@deviceDescription", string.IsNullOrEmpty(description) ? string.Empty : description);

                        cmd.ExecuteNonQuery();
                    }
                }

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "UPDATE Device",
                    HasErrors = false,
                    RequestObject = requestObject,
                    ResponseObject = "OK",
                    IsProcessed = true
                };
            }
            catch (Exception ex)
            {
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Error Executing Query: {ex.Message} {ex.StackTrace}");

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "UPDATE Device",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Error Executing Query. Ref: {logId}",
                    IsProcessed = true
                };
            }
        }
        private static DeviceChangeResult<string, string> UpdateThingSpaceDeviceInformation(KeySysLambdaContext context, string requestObject, string iccid, long bulkChangeId, int serviceProviderId, string deviceDescription, string userName)
        {
            {
                try
                {
                    using (var conn = new SqlConnection(context.CentralDbConnectionString))
                    {
                        conn.Open();

                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = @"UPDATE Device
                                            SET DeviceDescription = @deviceDescription,
                                            Username = @username,
                                            ModifiedBy = 'AltaworxJasperAWSUpdateDeviceStatus',
                                            ModifiedDate = GETUTCDATE()
                                            WHERE IsDeleted = 0 AND ServiceProviderId = @serviceProviderId 
                                            AND ICCID = @ICCID";
                            cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                            cmd.Parameters.AddWithValue("@ICCID", iccid);
                            cmd.Parameters.AddWithValue("@deviceDescription", deviceDescription);
                            cmd.Parameters.AddWithValue("@username", userName);
                            cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);

                            cmd.ExecuteNonQuery();
                        }
                    }

                    return new DeviceChangeResult<string, string>()
                    {
                        ActionText = "UPDATE Device",
                        HasErrors = false,
                        RequestObject = requestObject,
                        ResponseObject = "OK",
                        IsProcessed = false
                    };
                }
                catch (Exception ex)
                {
                    var logId = Guid.NewGuid();
                    LogInfo(context, "ERROR", $"{logId} Error Executing Query: {ex.Message} {ex.StackTrace}");

                    return new DeviceChangeResult<string, string>()
                    {
                        ActionText = "UPDATE Device",
                        HasErrors = true,
                        RequestObject = requestObject,
                        ResponseObject = $"Error Executing Query. Ref: {logId}"
                    };
                }
            }
        }
        private static ThingSpaceCallBackResponseLog CheckRequestIdExist(KeySysLambdaContext context, string requestId, int tenantId, int serviceProviderId)
        {
            var attempt = 0;
            var maxAttempt = 20;
            var delay = 5000;
            var isExist = false;
            var thingSpaceCallBackLog = new ThingSpaceCallBackResponseLog();
            do
            {
                Thread.Sleep(delay);
                attempt++;
                thingSpaceCallBackLog = GetThingSpaceCallBackResponseLog(context, requestId, tenantId, serviceProviderId);
                isExist = thingSpaceCallBackLog != null && thingSpaceCallBackLog.APIStatus.Equals("Success", StringComparison.InvariantCultureIgnoreCase);
            } while (!isExist && attempt < maxAttempt);
            return isExist ? thingSpaceCallBackLog : null;
        }
        private static async Task<string> CheckStatusThingSpaceDevice(ThingSpaceTokenResponse accessToken,
            ThingSpaceLoginResponse sessionToken, string requestId, string getStatusUrl, ThingSpaceAuthentication thingSpaceAuth)
        {
            var attempt = 0;
            var maxAttempt = 20;
            var delay = 5000;
            var isPending = true;
            var status = "";

            do
            {
                Thread.Sleep(delay);

                // get status 
                status = await ThingSpaceCommon.GetStatusRequest(thingSpaceAuth, accessToken, sessionToken, requestId, getStatusUrl);
                attempt++;
                isPending = string.IsNullOrEmpty(status) ? true : status.Equals(ThingSpaceRequestStatus.Pending);
            } while (isPending && attempt < maxAttempt);
            return status;
        }
        private static async Task<DeviceChangeResult<string, string>> OldFlowActivatedWithThingSpacePPUThingSpaceDeviceAsync(ThingSpaceStatusUpdateRequest request, string baseUrl, ThingSpaceTokenResponse accessToken,
            ThingSpaceLoginResponse sessionToken, KeySysLambdaContext context, int serviceProviderId, string requestId, string getStatusUrl, ThingSpaceAuthentication thingSpaceAuth, long bulkChangeId)
        {
            LogInfo(context, "INFO", $"Update Status Active By Old Flow.");
            var deviceDescription = "";
            var usernameOfDevice = "";
            var status = "";
            string requestObject = JsonConvert.SerializeObject(request);
            status = await CheckStatusThingSpaceDevice(accessToken, sessionToken, requestId, getStatusUrl, thingSpaceAuth);
            if (!string.IsNullOrEmpty(status) && (status.Equals(ThingSpaceRequestStatus.Success) || status.Equals(ThingSpaceRequestStatus.Pending)))
            {
                deviceDescription = JsonConvert.SerializeObject(request.thingSpacePPU);
                usernameOfDevice = $"{request.thingSpacePPU.FirstName} {request.thingSpacePPU.LastName}";
            }
            else
            {
                LogInfo(context, "INFO", $"Update Status Active and ThingSpacePPU Failed.");
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "GetThingSpaceDevice: ThingSpace API",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Update Status Active and ThingSpacePPU Failed."
                };
            }
            return UpdateThingSpaceDeviceInformation(context, requestObject, request.ICCID, bulkChangeId, serviceProviderId, deviceDescription, usernameOfDevice);
        }
        private static DeviceChangeResult<string, string> NewFlowActivatedWithThingSpacePPUThingSpaceDeviceAsync(ThingSpaceStatusUpdateRequest request, KeySysLambdaContext context, int serviceProviderId, string requestId, long bulkChangeId, ThingSpaceCallBackResponseLog thingSpaceCallBackLog)
        {
            LogInfo(context, "INFO", $"Update Status Active By New Flow use CallBack.");
            var deviceDescription = "";
            var usernameOfDevice = "";
            var status = "";
            string requestObject = JsonConvert.SerializeObject(request);
            var iccid = request.ICCID;
            var responseCallbackModel = JsonConvert.DeserializeObject<ThingSpaceCallBackResponse>(thingSpaceCallBackLog.APIResponse);
            status = responseCallbackModel?.Status;
            if (!string.IsNullOrEmpty(status) && (status.Equals(ThingSpaceRequestStatus.Success) || status.Equals(ThingSpaceRequestStatus.Pending)))
            {
                deviceDescription = JsonConvert.SerializeObject(request.thingSpacePPU);
                usernameOfDevice = $"{request.thingSpacePPU.FirstName} {request.thingSpacePPU.LastName}";
            }
            else
            {
                LogInfo(context, "INFO", $"Update Status Active and ThingSpacePPU Failed.");
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "GetThingSpaceDevice: ThingSpace API",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Update Status Active and ThingSpacePPU Failed ERROR {responseCallbackModel?.FaultResponse?.FaultString}."
                };
            }
            var ipAddress = responseCallbackModel?.DeviceResponse?.ActivateResponse?.IpAddress;
            usernameOfDevice = $"{request.thingSpacePPU.FirstName} {request.thingSpacePPU.LastName}";
            var phoneNumber = responseCallbackModel.DeviceResponse?.ActivateResponse?.DeviceIds.FirstOrDefault(x => x.Kind.Equals("msisdn", StringComparison.InvariantCultureIgnoreCase));
            if (phoneNumber == null || string.IsNullOrEmpty(phoneNumber.Id))
            {
                LogInfo(context, "WARNING", $"Newly activated ThingSpace device with ICCID '{iccid}' did not have an MSISDN");

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "GetThingSpaceDevice: ThingSpace API",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Newly activated ThingSpace device with ICCID '{iccid}' did not have an MSISDN",
                    IsProcessed = true
                };
            }
            return UpdateThingSpaceDeviceInformationCallbackFlow(context, requestObject, iccid, ipAddress, bulkChangeId, serviceProviderId, phoneNumber.Id, deviceDescription, usernameOfDevice);
        }
        private static async Task<DeviceChangeResult<string, string>> OldFlowUpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync(ThingSpaceStatusUpdateRequest request, string baseUrl,
            ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, KeySysLambdaContext context, int serviceProviderId, string accountName, string requestId, long bulkChangeId, int tenantId)
        {
            LogInfo(context, "WARNING", $"Use Old Flow To Update Fields On Newly Activated ThingSpace Device");
            var iccid = request.ICCID;
            string requestObject = $"ICCID: {iccid}";

            var attempt = 0;
            var maxAttempt = 20;
            var delay = 5000;

            LogInfo(context, "INFO", $"UpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync.");
            LogInfo(context, "INFO", $"Delay: {delay}s.");

            var connectionString = context.CentralDbConnectionString;
            var logger = context.logger;

            bool isActive;
            ThingSpaceServiceAmop.Models.DeviceResponse device;

            do
            {
                Thread.Sleep(delay);
                attempt++;
                device = await GetThingSpaceDeviceAsync(iccid, baseUrl, accessToken, sessionToken, logger);
                var carrierInformation = device?.carrierInformations?.FirstOrDefault();
                var state = carrierInformation?.state;

                isActive = state != null && state.Equals("active", StringComparison.InvariantCultureIgnoreCase);
            } while (!isActive && attempt < maxAttempt);

            if (!isActive)
            {
                var requestStatus = await GetThingSpaceRequestStatusAsync(accountName, requestId, baseUrl, accessToken, sessionToken, logger);

                //check fail
                var logId = Guid.NewGuid();
                LogInfo(context, "ERROR", $"{logId} Newly activated ThingSpace device with ICCID '{iccid}' was not active within the allotted retry time");

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "GetThingSpaceDevice: ThingSpace API",
                    HasErrors = requestStatus.status == ThingSpaceRequestStatus.Failure,
                    RequestObject = requestObject,
                    ResponseObject = requestStatus.status == ThingSpaceRequestStatus.Failure ?
                        $"Device failed to activate in ThingSpace. Log Ref: {logId}"
                        : $"Device is still being processed and have not activated in ThingSpace in a timely manner. Log Ref: {logId}",
                    IsProcessed = requestStatus.status == ThingSpaceRequestStatus.Failure ? true : false // pending active => delay 15p, then retry.
                };
            }

            var phoneNumber = device.deviceIds?.FirstOrDefault(id => id.kind.Equals("msisdn", StringComparison.InvariantCultureIgnoreCase));

            if (phoneNumber == null || string.IsNullOrEmpty(phoneNumber.id))
            {
                logger.LogInfo("WARNING", $"Newly activated ThingSpace device with ICCID '{iccid}' did not have an MSISDN");

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "GetThingSpaceDevice: ThingSpace API",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Newly activated ThingSpace device with ICCID '{iccid}' did not have an MSISDN",
                    IsProcessed = true
                };
            }

            requestObject += ", MSISDN: " + phoneNumber.id;

            return UpdateThingSpaceDeviceInformationCallbackFlow(context, requestObject, iccid, "", bulkChangeId, serviceProviderId, phoneNumber.id, "", "");
        }
        private static DeviceChangeResult<string, string> NewFlowUpdateFieldsOnNewlyActivatedThingSpaceDeviceAsync(ThingSpaceStatusUpdateRequest request, KeySysLambdaContext context, int serviceProviderId, string requestId, long bulkChangeId, ThingSpaceCallBackResponseLog thingSpaceCallBackLog)
        {
            LogInfo(context, "WARNING", $"Use New Flow To Update Fields On Newly Activated ThingSpace Device");
            var responseCallbackModel = JsonConvert.DeserializeObject<ThingSpaceCallBackResponse>(thingSpaceCallBackLog.APIResponse);
            var iccid = request.ICCID;
            string requestObject = $"ICCID: {iccid}";
            //check fail
            if (!thingSpaceCallBackLog.APIStatus.Contains(ThingSpaceRequestStatus.Success))
            {
                var logId = Guid.NewGuid();

                LogInfo(context, "ERROR", $"{logId} Newly activated ThingSpace device with ICCID '{iccid}' was not active within the allotted retry time");

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "GetThingSpaceDevice: ThingSpace API",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = responseCallbackModel?.FaultResponse?.FaultString,
                    IsProcessed = true
                };
            }
            var phoneNumber = responseCallbackModel.DeviceIds?.FirstOrDefault(x => x.Kind.Equals("msisdn", StringComparison.InvariantCultureIgnoreCase));
            var ipAddress = responseCallbackModel?.DeviceResponse?.ActivateResponse?.IpAddress;
            if (phoneNumber == null || string.IsNullOrEmpty(phoneNumber.Id))
            {
                LogInfo(context, "WARNING", $"Newly activated ThingSpace device with ICCID '{iccid}' did not have an MSISDN");

                return new DeviceChangeResult<string, string>()
                {
                    ActionText = "GetThingSpaceDevice: ThingSpace API",
                    HasErrors = true,
                    RequestObject = requestObject,
                    ResponseObject = $"Newly activated ThingSpace device with ICCID '{iccid}' did not have an MSISDN",
                    IsProcessed = true
                };
            }
            requestObject += ", MSISDN: " + phoneNumber.Id;
            return UpdateThingSpaceDeviceInformationCallbackFlow(context, requestObject, iccid, ipAddress, bulkChangeId, serviceProviderId, phoneNumber.Id, "", "");
        }

        private List<RevCustomerDefaultCustomerRatePlans> GetCustomerRatePlans(KeySysLambdaContext context, List<string> accountNumbers, int? tenantId)
        {
            LogInfo(context, CommonConstants.SUB, $"({string.Join(",", accountNumbers)})");
            var customerRatePlanDetails = new List<RevCustomerDefaultCustomerRatePlans>();
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = new SqlCommand(Amop.Core.Constants.SQLConstant.StoredProcedureName.UPDATE_DEVICE_STATUS_GET_CUSTOMER_RATE_PLANS, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue(CommonSQLParameterNames.ACCOUNT_NUMBERS, string.Join(",", accountNumbers));
                        command.Parameters.AddWithValue(CommonSQLParameterNames.TENANT_ID, tenantId);
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        connection.Open();

                        SqlDataReader reader = command.ExecuteReader();
                        while (reader.Read())
                        {
                            var defaultCustomerRatePlans = string.Empty;
                            var revCustomerId = string.Empty;
                            if (!reader.IsDBNull(reader.GetOrdinal(CommonColumnNames.DefaultCustomerRatePlans)))
                            {
                                defaultCustomerRatePlans = reader[CommonColumnNames.DefaultCustomerRatePlans].ToString();
                            }
                            if (!reader.IsDBNull(reader.GetOrdinal(CommonColumnNames.RevCustomerId)))
                            {
                                revCustomerId = reader[CommonColumnNames.RevCustomerId].ToString();
                            }
                            customerRatePlanDetails.Add(new RevCustomerDefaultCustomerRatePlans
                            {
                                DefaultCustomerRatePlans = defaultCustomerRatePlans,
                                RevCustomerId = revCustomerId
                            });
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, string.Join(". ", ex.Message, ex.ErrorCode, ex.Number, ex.StackTrace)));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, ex.Message);
            }

            return customerRatePlanDetails;
        }

        private List<RevServiceDetail> GetRevServices(KeySysLambdaContext context, List<string> iccids, int? tenantId, int serviceProviderId)
        {
            LogInfo(context, CommonConstants.SUB, $"({string.Join(",", iccids)})");
            var revServiceDetails = new List<RevServiceDetail>();
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = new SqlCommand(Amop.Core.Constants.SQLConstant.StoredProcedureName.UPDATE_DEVICE_STATUS_GET_REV_SERVICE, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue(CommonSQLParameterNames.ICCIDS, string.Join(",", iccids));
                        command.Parameters.AddWithValue(CommonSQLParameterNames.TENANT_ID, tenantId);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId);
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        connection.Open();

                        SqlDataReader reader = command.ExecuteReader();
                        while (reader.Read())
                        {
                            DateTime? activatedDate = null;
                            DateTime? disconnectedDate = null;
                            if (!reader.IsDBNull(reader.GetOrdinal(CommonColumnNames.ActivatedDate)))
                            {
                                activatedDate = DateTime.Parse(reader[CommonColumnNames.ActivatedDate].ToString());
                            }
                            if (!reader.IsDBNull(reader.GetOrdinal(CommonColumnNames.DisconnectedDate)))
                            {
                                disconnectedDate = DateTime.Parse(reader[CommonColumnNames.DisconnectedDate].ToString());
                            }
                            revServiceDetails.Add(new RevServiceDetail
                            {
                                Id = int.Parse(reader[CommonColumnNames.Id].ToString()),
                                ICCID = reader[CommonColumnNames.ICCID].ToString(),
                                TenantId = int.Parse(reader[CommonColumnNames.TenantId].ToString()),
                                RevServiceId = int.Parse(reader[CommonColumnNames.RevServiceId].ToString()),
                                ActivatedDate = activatedDate,
                                DisconnectedDate = disconnectedDate
                            });
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, string.Join(". ", ex.Message, ex.ErrorCode, ex.Number, ex.StackTrace)));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, ex.Message);
            }

            return revServiceDetails;
        }

        public void UpdateRevCustomer(KeySysLambdaContext context, string iccid, string accountNumber, int tenantId, int serviceProviderId, int newDeviceStatusId, string defaultCustomerRatePlans)
        {
            LogInfo(context, CommonConstants.SUB, $"({iccid}, {accountNumber}, {tenantId}");
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.DEVICE_BULK_CHANGE_UPDATE_REV_CUSTOMER;
                        command.Parameters.AddWithValue(CommonSQLParameterNames.ICCID, iccid);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.REV_CUSTOMER_ID, accountNumber);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.TENANT_ID, tenantId);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.NEW_DEVICE_STATUS_ID, newDeviceStatusId);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.DEFAULT_CUSTOMER_RATE_PLANS, defaultCustomerRatePlans);
                        command.Parameters.AddWithValue(CommonSQLParameterNames.PROCESSED_BY_PASCAL_CASE, CommonConstants.SYSTEM_USER);
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        connection.Open();

                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_EXECUTING_SQL_COMMAND, string.Join(". ", ex.Message, ex.ErrorCode, ex.Number, ex.StackTrace)));
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EXCEPTION_WHEN_CONNECTING_DATABASE, ex.Message));
            }
            catch (Exception ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, ex.Message);
            }
        }
    }
}
