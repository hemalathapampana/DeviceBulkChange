using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using AltaworxDeviceBulkChange.Constants;
using AltaworxDeviceBulkChange.Models;
using Amazon;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Models.DeviceBulkChange;
using Amop.Core.Models.Telegence;
using Amop.Core.Models.Telegence.Api;
using Amop.Core.Repositories;
using Amop.Core.Services.Telegence;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace AltaworxDeviceBulkChange
{
    public partial class Function
    {
        private static int MAX_IP_PROVISION_PER_LAMBDA_INSTANCE = 200;
        private static int MAX_IP_PROVISION_PER_REQUEST = 10;
        private static int DELAY_TELEGENCE_IP_PROVISION_MESSAGE = 5;//seconds
        private static int DELAY_TELEGENCE_IP_PROVISION_STATUS_CHECK = 15000;//ms

        private async Task<bool> ProcessTelegenceStaticIPProvisioning(KeySysLambdaContext context, BulkChange bulkChange, SQSEvent.SQSMessage message)
        {
            LogInfo(context, LogTypeConstant.Sub, $"ProcessTelegenceStaticIPProvisioning(..,{bulkChange.Id},{bulkChange.ServiceProviderId},...)");

            if (string.IsNullOrEmpty(TelegenceIPProvisioningURL))
            {
                LoadTelegenceStaticIPProvisioningEnv(context);
            }

            var sqlRetryPolicy = GetSqlTransientRetryPolicy(context);
            var logRepo = new DeviceBulkChangeLogRepository(context.CentralDbConnectionString, sqlRetryPolicy);

            int batchSize = MAX_IP_PROVISION_PER_LAMBDA_INSTANCE;

            //authentication info
            var telegenceApiAuthentication =
                GetTelegenceApiAuthentication(context.CentralDbConnectionString, bulkChange.ServiceProviderId);

            //get a batch of changes that have not processed ip provisioning step but finished the activation step
            var changes = GetBatchedWithAdditionalStepMobilityDeviceChanges(context, bulkChange.Id, batchSize, onlyUnprocessed: true);

            changes = changes.Where(change => !string.IsNullOrEmpty(change.DeviceIdentifier)).ToList();

            if (changes == null || changes.Count == 0)
            {
                // empty list to process meaning done
                // continue with the status check step
                LogInfo(context, LogTypeConstant.Info, $"No unprocessed changes found for new service activation {bulkChange.Id}");
                await EnqueueBatchedTelegenceIPProvisioningAsync(context, bulkChange.Id, DeviceBulkChangeQueueUrl, 5, TelegenceNewActivationStep.CheckIPProvisionStatus);
                return false;
            }

            if (telegenceApiAuthentication == null)
            {
                string errorMessage = $"Unable to get Telegence API Authentication for Service Provider: {bulkChange.ServiceProviderId}";

                LogInfo(context, LogTypeConstant.Exception, errorMessage);
                LogMobilityChange(logRepo, bulkChange, changes, "Static IP Provisioning: Telegence API Credentials", BulkChangeStatus.ERROR, true, errorMessage, $"ServiceProviderId: {bulkChange.ServiceProviderId}", errorMessage);
                // mark process error & second step error
                await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMessage, true, TelegenceAdditionalStepStatus.Error);
                return false;
            }

            if (_telegenceApiPostClient == null)
                _telegenceApiPostClient = new TelegenceAPIClient(telegenceApiAuthentication,
                    context.IsProduction, $"{ProxyUrl}/api/Proxy/Post", context.logger);

            //batch changes by groups of 10 
            var batchedChanges = changes.SplitCollection(MAX_IP_PROVISION_PER_REQUEST);
            try
            {
                foreach (var batch in batchedChanges)
                {
                    //process by batch of 10 
                    var errorMessage = await ProcessBatchedTelegenceStaticIPProvisioning(context, bulkChange, batch, logRepo, telegenceApiAuthentication, _telegenceApiPostClient);

                    //exception happened in middle of batch, marked error to all remaining and stop process
                    if (!string.IsNullOrEmpty(errorMessage))
                    {
                        LogInfo(context, LogTypeConstant.Info, "Error when procesing the IP provisioning. Stopping process...");
                        await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMessage, true, TelegenceAdditionalStepStatus.Error, JsonConvert.SerializeObject(new TelegenceIPProvisionStepDetails() { Error = errorMessage }));
                        return false;
                    }

                    //finished a batch and now check remaining time
                    if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                    {
                        LogInfo(context, LogTypeConstant.Sub, $"Requeue to continue process check IP provision status.");
                        // processing should continue, we just need to requeue
                        await EnqueueBatchedTelegenceIPProvisioningAsync(context, bulkChange.Id, DeviceBulkChangeQueueUrl, DELAY_TELEGENCE_IP_PROVISION_MESSAGE);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                // unexpected exception => stop immediately
                LogInfo(context, LogTypeConstant.Exception, $"Could not process Static IP Provionsing. {ex.Message}. Stack trace: {ex.StackTrace}");
                return false;
            }

            //finished the instance and continue with next set of changes
            LogInfo(context, LogTypeConstant.Info, $"Processed {batchSize} changes. Requeue to continue process next {batchSize} changes.");
            await EnqueueBatchedTelegenceIPProvisioningAsync(context, bulkChange.Id, DeviceBulkChangeQueueUrl, DELAY_TELEGENCE_IP_PROVISION_MESSAGE);

            //always return false so that this won't trigger the initial step retry logic 
            return false;
        }

        public async Task<string> ProcessBatchedTelegenceStaticIPProvisioning(KeySysLambdaContext context, BulkChange bulkChange, List<BulkChangeDetailRecord> changes, DeviceBulkChangeLogRepository logRepo, TelegenceAPIAuthentication auth, TelegenceAPIClient telegenceApiClient)
        {
            try
            {
                using (var httpClient = new HttpClient(new LambdaLoggingHandler()))
                {
                    var errorMsg = string.Empty;
                    var shouldEndProcess = false;
                    var refChangeRequest = JsonConvert.DeserializeObject<TelegenceActivationChangeRequest>(changes.FirstOrDefault()?.ChangeRequest);
                    var IPProvisioningOptions = refChangeRequest.StaticIPProvision;
                    var remainingChanges = new List<BulkChangeDetailRecord>();
                    // call api once to get 10 ips
                    var ipAddressesResponse = await GetIPAddressesResponseAsync(context, IPProvisioningOptions.PDPName, MAX_IP_PROVISION_PER_REQUEST, telegenceApiClient, httpClient);

                    if (ipAddressesResponse == null || ipAddressesResponse.HasErrors)
                    {
                        errorMsg = $"Error getting IP addresses using PDP name {IPProvisioningOptions.PDPName}";
                        var requestText = (ipAddressesResponse != null && ipAddressesResponse?.RequestObject != null) ?
                                            JsonConvert.SerializeObject(ipAddressesResponse?.RequestObject) : errorMsg;
                        var responseText = (ipAddressesResponse != null && ipAddressesResponse.ResponseObject != null) ?
                                            JsonConvert.SerializeObject(ipAddressesResponse?.ResponseObject) : errorMsg;
                        await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMsg, true,
                            TelegenceAdditionalStepStatus.Error, JsonConvert.SerializeObject(new TelegenceIPProvisionStepDetails() { Error = errorMsg }));
                        LogMobilityChange(logRepo, bulkChange, changes,
                            "Static IP Provisioning: Request IP Addresses",
                            BulkChangeStatus.ERROR, true, errorMsg,
                            requestText,
                            responseText);
                        return errorMsg;
                    }

                    //ip address type are based on PDP name so no need to check for it
                    var requestedIPs = ipAddressesResponse.ResponseObject.IPAddressRecords.ToList();

                    //no ip returned => just end the process
                    if (requestedIPs.Count <= 0)
                    {
                        errorMsg = $"No available IP address. {ipAddressesResponse.ResponseObject.Message}";
                        await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMsg, true,
                            TelegenceAdditionalStepStatus.APIError, JsonConvert.SerializeObject(new TelegenceIPProvisionStepDetails() { Error = errorMsg }));
                        LogMobilityChange(logRepo, bulkChange, changes,
                            "Static IP Provisioning: Request IP Addresses",
                            BulkChangeStatus.ERROR, true, errorMsg,
                            JsonConvert.SerializeObject(ipAddressesResponse.RequestObject),
                            JsonConvert.SerializeObject(ipAddressesResponse.ResponseObject));
                        return errorMsg;
                    }

                    if (requestedIPs.Count < changes.Count)
                    {
                        //pdp have less ip than needed => would not have enough for remaining sims => do remaining sims and mark stop process
                        shouldEndProcess = true;
                        remainingChanges = changes.Skip(requestedIPs.Count).ToList();

                        //removed those sims from the processing list
                        //subscriberNumbers = subscriberNumbers.Take(requestedIPs.Count).ToList();
                        changes = changes.Take(requestedIPs.Count).ToList();
                    }

                    // call api once to request provision for those 10 
                    var billingAccountId = refChangeRequest.TelegenceActivationRequest.BillingAccount.Id;

                    var submittedIPProvisionResult = await SubmitTelegenceIPProvisioningAsync(context, billingAccountId, changes.Select(change => change.DeviceIdentifier).ToList(), IPProvisioningOptions, requestedIPs, telegenceApiClient, httpClient);

                    if (submittedIPProvisionResult == null || submittedIPProvisionResult.HasErrors)
                    {
                        errorMsg = "Error submitting IP provisioning request.";
                        var requestText = (submittedIPProvisionResult != null && submittedIPProvisionResult?.RequestObject != null) ?
                                            JsonConvert.SerializeObject(submittedIPProvisionResult?.RequestObject) : errorMsg;
                        var responseText = (submittedIPProvisionResult != null && submittedIPProvisionResult.ResponseObject != null) ?
                                            JsonConvert.SerializeObject(submittedIPProvisionResult?.ResponseObject) : errorMsg;
                        await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMsg, true,
                            TelegenceAdditionalStepStatus.APIError, JsonConvert.SerializeObject(new TelegenceIPProvisionStepDetails() { Error = errorMsg }));
                        LogMobilityChange(logRepo, bulkChange, changes,
                            "Static IP Provisioning: Submit IP Provision Request",
                            BulkChangeStatus.ERROR, true, errorMsg,
                            requestText,
                            responseText);
                        return errorMsg;
                    }

                    //get the request id from message 
                    //example: "Your request has been received and will be submitted for processing. You may check the status with the subscriber-number or batch-id: 8081b1f0-d070-11eb-a2ea-a0cec8c59839"
                    var batchId = submittedIPProvisionResult.ResponseObject.TelegenceIPProvisionResponse.Message.Split(" ").Last();
                    Guid batchGuid;
                    bool isValidBatchId = Guid.TryParse(batchId, out batchGuid);
                    if (!isValidBatchId)
                    {
                        errorMsg = "Failed to retrieve batch id from response. Stopping process";
                        await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMsg, true,
                            TelegenceAdditionalStepStatus.APIError, JsonConvert.SerializeObject(new TelegenceIPProvisionStepDetails() { Error = errorMsg }));
                        LogMobilityChange(logRepo, bulkChange, changes,
                            "Static IP Provisioning: Request IP Addresses",
                            BulkChangeStatus.ERROR, true, errorMsg,
                            JsonConvert.SerializeObject(submittedIPProvisionResult.RequestObject),
                            JsonConvert.SerializeObject(submittedIPProvisionResult.ResponseObject.TelegenceIPProvisionResponse));
                        return errorMsg;
                    }

                    //save the batchid and other info into database
                    var additionalStepDetails = new TelegenceIPProvisionStepDetails()
                    {
                        BatchId = batchId,
                        RetryCount = 0 // only for the check request status step
                    };

                    await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, true, string.Empty,
                        TelegenceAdditionalStepStatus.Requested, JsonConvert.SerializeObject(additionalStepDetails), changes.Select(change => change.DeviceIdentifier).ToList(), ChangeStatus.PENDING, null);
                    LogMobilityChange(logRepo, bulkChange, changes, "Static IP Provisioning: Submit IP Provision Request",
                        BulkChangeStatus.PROCESSED, false, errorMsg,
                        JsonConvert.SerializeObject(submittedIPProvisionResult.RequestObject),
                        JsonConvert.SerializeObject(submittedIPProvisionResult.ResponseObject.TelegenceIPProvisionResponse));

                    // small delay to wait for request to complete
                    Thread.Sleep(DELAY_TELEGENCE_IP_PROVISION_STATUS_CHECK);
                    //get the request status

                    if (_telegenceApiGetClient == null)
                        _telegenceApiGetClient = new TelegenceAPIClient(auth,
                            context.IsProduction, $"{ProxyUrl}/api/Proxy/Get", context.logger);

                    var ipProvisionStatusResponse = await GetIPProvisionStatusResponseAsync(context, batchId, _telegenceApiGetClient, httpClient);

                    if (ipProvisionStatusResponse == null || ipProvisionStatusResponse.HasErrors)
                    {
                        //could not get response
                        errorMsg = "Error when checking status of IP provision request.";
                        additionalStepDetails.Error = errorMsg;
                        var responseText = (ipProvisionStatusResponse != null && ipProvisionStatusResponse?.ResponseObject != null) ?
                                            JsonConvert.SerializeObject(ipProvisionStatusResponse?.ResponseObject) : errorMsg;
                        await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, false, string.Empty,
                            TelegenceAdditionalStepStatus.APIError, JsonConvert.SerializeObject(additionalStepDetails),
                            changes.Select(change => change.DeviceIdentifier).ToList(), ChangeStatus.API_FAILED, null);
                        LogMobilityChange(logRepo, bulkChange, changes, "Static IP Provisioning: Check IP Provision Status",
                            BulkChangeStatus.ERROR, true, errorMsg,
                            JsonConvert.SerializeObject(additionalStepDetails),
                            responseText);
                        return errorMsg;
                    }

                    var responseStatuses = ipProvisionStatusResponse.ResponseObject.TelegenceIPProvisionStatusResponses;

                    // process the individual device provision statuses
                    var table = new DataTable();
                    table.Columns.Add("MSISDN");
                    table.Columns.Add("IPAddress");

                    foreach (var change in changes)
                    {
                        var IPProvisionInfo = responseStatuses.Where(res => res.SubscriberNumber == change.DeviceIdentifier).FirstOrDefault();
                        var newAdditionalStepDetails = new TelegenceIPProvisionStepDetails()
                        {
                            BatchId = batchId,
                            RetryCount = 0
                        };


                        if (!string.IsNullOrEmpty(IPProvisionInfo.BatchId))
                        {
                            LogInfo(context, LogTypeConstant.Info, $"Current status of request {batchId}: ProfileCheck - {IPProvisionInfo.ProfileCheck}, IPReservation - {IPProvisionInfo.IPReservation}, ProfileUpdate - {IPProvisionInfo.ProfileUpdate}");
                            //info found
                            if (IPProvisionInfo.ProfileCheck.Contains("Success")
                                && IPProvisionInfo.IPReservation.Contains("Reserved")
                                && IPProvisionInfo.ProfileUpdate.Contains("Complete"))
                            {
                                // if already success, mark processed additional step 
                                var ipAddress = IPProvisionInfo.IPAddressRecord.IPAddress;

                                if (!string.IsNullOrWhiteSpace(ipAddress))
                                {
                                    var dr = AddToDataRow(table, change.DeviceIdentifier, ipAddress);
                                    table.Rows.Add(dr);
                                }

                                await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, true, ipAddress,
                                    TelegenceAdditionalStepStatus.Processed, JsonConvert.SerializeObject(newAdditionalStepDetails),
                                    new List<string> { change.DeviceIdentifier }, ChangeStatus.PROCESSED, null);
                                LogMobilityChange(logRepo, bulkChange, changes,
                                    $"Static IP Provisioning: Check IP Provision Status. Success.",
                                    BulkChangeStatus.PROCESSED, false, errorMsg,
                                    JsonConvert.SerializeObject(newAdditionalStepDetails),
                                    JsonConvert.SerializeObject(IPProvisionInfo));
                            }
                            else
                            {
                                // if fail to get status after the delay, mark as requested, skip and continue submitting next set of sims
                                // only increase the api attemp count and will check this SIM again in next step
                                await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, true, string.Empty, TelegenceAdditionalStepStatus.Requested, JsonConvert.SerializeObject(newAdditionalStepDetails), new List<string> { change.DeviceIdentifier }, ChangeStatus.PENDING, null);
                                LogMobilityChange(logRepo, bulkChange, changes,
                                    $"Static IP Provisioning: Check IP Provision Status. Retry after {DELAY_TELEGENCE_CHECK_IP_PROVISION} seconds",
                                    BulkChangeStatus.PROCESSED, false, errorMsg,
                                    JsonConvert.SerializeObject(newAdditionalStepDetails),
                                    JsonConvert.SerializeObject(IPProvisionInfo));
                            }
                        }
                        else
                        {
                            //invalid batchId or the subscriberNumber not in this batch id (just to make sure)
                            errorMsg = $"Error when checking status of IP provision request. {IPProvisionInfo.Message}";
                            additionalStepDetails.Error = errorMsg;

                            LogInfo(context, "ERROR", errorMsg);
                            await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, false, string.Empty, TelegenceAdditionalStepStatus.APIError, JsonConvert.SerializeObject(additionalStepDetails), new List<string> { change.DeviceIdentifier }, ChangeStatus.API_FAILED, null);
                            LogMobilityChange(logRepo, bulkChange, new List<BulkChangeDetailRecord> { change },
                                "Static IP Provisioning: Check IP Provision Status",
                                BulkChangeStatus.ERROR, true, errorMsg,
                                JsonConvert.SerializeObject(submittedIPProvisionResult.RequestObject),
                                JsonConvert.SerializeObject(additionalStepDetails));
                        }
                    }
                    // check the flag if need to stop submitting ip midway

                    if (table.Rows.Count > 0)
                    {
                        BulkUpdateMobilityDeviceIpAddress(context, table);
                    }

                    if (shouldEndProcess)
                    {
                        LogInfo(context, LogTypeConstant.Info, "No remaining IP address. Stopping the requesting process and continue with checking request status.");
                        // mark the remaining sims as error 
                        errorMsg = $"Not enough available IP address to provision the remaining SIMs.";
                        additionalStepDetails.Error = errorMsg;
                        await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMsg, true, TelegenceAdditionalStepStatus.Error, JsonConvert.SerializeObject(additionalStepDetails));
                        LogMobilityChange(logRepo, bulkChange, changes, "Static IP Provisioning: Request IP Addresses", BulkChangeStatus.ERROR, true, errorMsg, JsonConvert.SerializeObject(ipAddressesResponse.RequestObject));
                    }
                }
            }
            catch (Exception ex)
            {
                //fail to run specific batch of 10
                var errorMsg = $"Error: {ex.Message} - {ex.StackTrace}";
                LogInfo(context, LogTypeConstant.Error, errorMsg);
                return errorMsg;
            }

            return string.Empty;
        }

        private async Task EnqueueBatchedTelegenceIPProvisioningAsync(KeySysLambdaContext context, long bulkChangeId,
          string queueUrl, int delaySeconds, TelegenceNewActivationStep activationStep = TelegenceNewActivationStep.RequestIPProvision)
        {
            LogInfo(context, LogTypeConstant.Sub, $"EnqueueBatchedTelegenceIPProvisioningAsync({bulkChangeId},'{queueUrl}',{delaySeconds})");

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                var messageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    {
                        "BulkChangeId",
                        new MessageAttributeValue {DataType = "String", StringValue = bulkChangeId.ToString()}
                    },
                    {
                        "TelegenceNewServiceActivationStep",
                        new MessageAttributeValue {DataType = "String", StringValue = ((int)activationStep).ToString()}
                    }
                };

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

        //get Telegence get IP request
        private async Task<DeviceChangeResult<TelegenceIPAdressesRequest, TelegenceIPAdressesResponse>> GetIPAddressesResponseAsync(KeySysLambdaContext context, string pdpName, int ipQuantity, TelegenceAPIClient telegenceApiClient, HttpClient httpClient)
        {
            LogInfo(context, LogTypeConstant.Sub, $"GetIPAddressesResponseAsync(..,{pdpName},{ipQuantity},..)");

            var request = new TelegenceIPAdressesRequest()
            {
                PdpName = pdpName,
                IPQuantity = ipQuantity
            };
            DeviceChangeResult<TelegenceIPAdressesRequest, TelegenceIPAdressesResponse> response =
                new DeviceChangeResult<TelegenceIPAdressesRequest, TelegenceIPAdressesResponse>()
                {
                    ActionText = $"POST {TelegenceIPAddressesURL}",
                    HasErrors = false,
                    RequestObject = request,
                    ResponseObject = new TelegenceIPAdressesResponse()
                };
            var httpRetryPolicy = GetHttpRetryPolicy(context);
            await httpRetryPolicy.ExecuteAsync(async () =>
            {
                var apiResponse = await telegenceApiClient.RequestIPAddresses(TelegenceIPAddressesURL, request, httpClient);
                if (apiResponse == null || apiResponse.HasErrors)
                {
                    LogInfo(context, LogTypeConstant.Error, $"Error getting IP addresses using PDP name {pdpName}");
                    response.HasErrors = true;
                    response.ResponseObject = apiResponse == null ? null : apiResponse.ResponseObject.TelegenceIPAdressesResponse;
                }
                else
                {
                    response.ResponseObject = apiResponse.ResponseObject.TelegenceIPAdressesResponse;
                }
            });

            return response;
        }

        //submit the API provisioning request for maximum of 10 devices
        private async Task<DeviceChangeResult<TelegenceIPProvisionRequest, TelegenceIPProvisionProxyResponse>> SubmitTelegenceIPProvisioningAsync(KeySysLambdaContext context, string billingAccount, List<string> subscriberNumbers, TelegenceIPProvision ipProvisionOptions, List<IPAddressRecord> ipAddresses, TelegenceAPIClient telegenceApiClient, HttpClient httpClient)
        {
            LogInfo(context, LogTypeConstant.Sub, $"SubmitTelegenceIPProvisioningAsync(..,{billingAccount},subscriberNumbers.Count():{subscriberNumbers.Count()},...,ipAddresses.Count():{ipAddresses.Count()}..)");

            var ipProvisionSubscribers = new List<IPProvisionSubscriber>();
            for (int i = 0; i < subscriberNumbers.Count; i++)
            {
                ipProvisionSubscribers.Add(new IPProvisionSubscriber()
                {
                    SubscriberNumber = subscriberNumbers[i],
                    IPAddressRecord = ipAddresses[i],
                    LteIndicator = ipProvisionOptions.LteIndicator,
                    DefaultAPNIndicator = ipProvisionOptions.DefaultAPN,
                    AccessPointName = ipProvisionOptions.StaticIPAPN,
                    EffectiveDate = DateTime.UtcNow.ToString("yyyy'-'MM'-'dd'Z'")
                });
            }

            var request = new TelegenceIPProvisionRequest()
            {
                PdpName = ipProvisionOptions.PDPName,
                BillingAccountNumber = billingAccount,
                Subscriber = ipProvisionSubscribers
            };

            DeviceChangeResult<TelegenceIPProvisionRequest, TelegenceIPProvisionProxyResponse> response =
                new DeviceChangeResult<TelegenceIPProvisionRequest, TelegenceIPProvisionProxyResponse>()
                {
                    ActionText = $"POST {TelegenceIPProvisioningURL}",
                    HasErrors = false,
                    RequestObject = request,
                    ResponseObject = new TelegenceIPProvisionProxyResponse()
                };
            var httpRetryPolicy = GetHttpRetryPolicy(context);
            await httpRetryPolicy.ExecuteAsync(async () =>
            {
                var apiResponse = await telegenceApiClient.SubmitIPProvisioningRequest($"{TelegenceIPProvisioningURL}", request, httpClient);
                if (apiResponse == null || apiResponse.HasErrors)
                {
                    LogInfo(context, LogTypeConstant.Error, "Error submitting IP provisioning request.");
                    response.HasErrors = true;
                    response.ResponseObject = apiResponse == null ? null : apiResponse.ResponseObject;
                }
                else
                {
                    response.ResponseObject = apiResponse.ResponseObject;
                }
            });

            return response;
        }

        public virtual async Task<DeviceChangeResult<string, TelegenceIPProvisionStatusProxyResponse>> GetIPProvisionStatusResponseAsync(KeySysLambdaContext context, string batchId, TelegenceAPIClient telegenceApiClient, HttpClient httpClient)
        {
            LogInfo(context, LogTypeConstant.Sub, $"GetIPProvisionStatusResponseAsync(..,{batchId},..)");
            DeviceChangeResult<string, TelegenceIPProvisionStatusProxyResponse> response =
                new DeviceChangeResult<string, TelegenceIPProvisionStatusProxyResponse>()
                {
                    ActionText = $"POST {TelegenceIPProvisionRequestStatusURL}",
                    HasErrors = false,
                    RequestObject = batchId,
                    ResponseObject = new TelegenceIPProvisionStatusProxyResponse()
                };

            var httpRetryPolicy = GetHttpRetryPolicy(context);
            await httpRetryPolicy.ExecuteAsync(async () =>
            {
                var apiResponse = await telegenceApiClient.CheckIPProvisioningStatus(TelegenceIPProvisionRequestStatusURL, batchId, string.Empty, httpClient);
                if (apiResponse == null || apiResponse.HasErrors)
                {
                    LogInfo(context, "ERROR", $"Error checking IP provisioning status for: {batchId}");
                    response.HasErrors = true;
                    response.ResponseObject = apiResponse == null ? null : apiResponse.ResponseObject;
                }
                else
                {
                    response = apiResponse;
                }
            });

            return response;
        }


        public virtual void LogTelegenceIPProvisionChange(DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, List<TelegenceBulkChangeDetailRecord> changes, string description, string bulkChangeStatus, bool hasErrors, string errorMessage = null, string requestText = null, string responseText = null)
        {
            foreach (var change in changes)
            {
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    MobilityDeviceChangeId = change.Id,
                    LogEntryDescription = description,
                    ResponseStatus = bulkChangeStatus,
                    HasErrors = hasErrors,
                    ErrorText = errorMessage,
                    RequestText = requestText ?? change.ChangeRequest,
                    ResponseText = responseText,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow
                });
            }
        }

        private void LogMobilityChange(DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, List<BulkChangeDetailRecord> changes, string description, string bulkChangeStatus, bool hasErrors, string errorMessage = null, string requestText = null, string responseText = null)
        {
            foreach (var change in changes)
            {
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    MobilityDeviceChangeId = change.Id,
                    LogEntryDescription = description,
                    ResponseStatus = bulkChangeStatus,
                    HasErrors = hasErrors,
                    ErrorText = errorMessage,
                    RequestText = requestText ?? change.ChangeRequest,
                    ResponseText = responseText ?? errorMessage,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow
                });
            }
        }

        //To update all in the same bulk change
        public static async Task MarkAllProcessedMobilityAdditionalStepAsync(KeySysLambdaContext context, long bulkChangeId, bool isSuccess, string errorMessage, bool onlyUnprocessed = true, string additionalStepStatus = null, string additionalStepDetails = null, string newStatusText = ChangeStatus.PROCESSED)
        {
            var statusText = isSuccess ? BulkChangeStatus.PROCESSED : BulkChangeStatus.ERROR;
            LogInfo(context, "SUB", $"MarkAllProcessedMobilityAdditionalStepAsync({bulkChangeId},{isSuccess},{errorMessage},{statusText})");
            await using var conn = new SqlConnection(context.CentralDbConnectionString);
            await using var cmd =
                new SqlCommand("usp_DeviceBulkChange_AdditionalStep_UpdateMobilityChangeByMSISDN", conn)
                {
                    CommandType = CommandType.StoredProcedure
                };
            cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
            cmd.Parameters.AddWithValue("@prevStatusText", BulkChangeStatus.PROCESSED);//skip changes that have error before additional step
            cmd.Parameters.AddWithValue("@IsSuccess", isSuccess ? 1 : 0);
            cmd.Parameters.AddWithValue("@newStatusText", newStatusText);
            cmd.Parameters.AddWithValue("@appendStatusDetails", errorMessage);
            cmd.Parameters.AddWithValue("@additionalStepStatus", additionalStepStatus ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@additionalStepDetails", additionalStepDetails ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@msisdnList", (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@onlyUnprocessed", onlyUnprocessed ? 1 : 0);
            conn.Open();
            await cmd.ExecuteNonQueryAsync();
        }

        //need new stored proc since iccids are not used in IP Provisioning process
        public virtual async Task MarkProcessedMobilityAdditionalStepByMSISDNAsync(KeySysLambdaContext context, long bulkChangeId, bool isSuccess, string ipAddress, string additionalStepStatus, string additionalStepDetails, List<string> msisdnList, string newStatusText = ChangeStatus.PROCESSED, string prevStatusText = ChangeStatus.PROCESSED, bool onlyUpdateUnprocessed = false)
        {
            var statusText = isSuccess ? BulkChangeStatus.PROCESSED : BulkChangeStatus.ERROR;
            LogInfo(context, CommonConstants.SUB, $"({bulkChangeId},{isSuccess},IP Address: {ipAddress},{statusText})");
            await using var conn = new SqlConnection(context.CentralDbConnectionString);
            await using var cmd = new SqlCommand(Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_ADDITIONAL_STEP_UPDATE_MOBILITY_CHANGE_BY_MSISDN, conn)
            {
                CommandType = CommandType.StoredProcedure,
                CommandTimeout = Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds
            };
            object msisdnListValue;
            if (msisdnList != null)
            {
                msisdnListValue = string.Join(',', msisdnList);
            }
            else
            {
                msisdnListValue = DBNull.Value;
            }
            object ipAddressValue;
            if (string.IsNullOrWhiteSpace(ipAddress))
            {
                ipAddressValue = DBNull.Value;
            }
            else
            {
                ipAddressValue = ipAddress;
            }
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.BULK_CHANGE_ID, bulkChangeId);
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.PREVIOUS_STATUS_TEXT, prevStatusText ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.IS_SUCCESS, Convert.ToInt32(isSuccess));
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.NEW_STATUS_TEXT, newStatusText);
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.IP_ADDRESS, ipAddressValue);
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.ADDITIONAL_STEP_STATUS, additionalStepStatus);
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.ADDITIONAL_STEP_DETAILS, additionalStepDetails);
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.MSISDN_LIST, msisdnListValue);
            cmd.Parameters.AddWithValue(CommonSQLParameterNames.ONLY_UNPROCESSED, Convert.ToInt32(onlyUpdateUnprocessed));
            conn.Open();
            await cmd.ExecuteNonQueryAsync();
        }

        private void LoadTelegenceStaticIPProvisioningEnv(KeySysLambdaContext context)
        {
            TelegenceIPAddressesURL = context.Context.ClientContext.Environment["TelegenceIPAddressesURL"];
            TelegenceIPProvisioningURL = context.Context.ClientContext.Environment["TelegenceIPProvisioningURL"];
            TelegenceIPProvisionRequestStatusURL = context.Context.ClientContext.Environment["TelegenceIPProvisionRequestStatusURL"];
        }
        public virtual void BulkUpdateMobilityDeviceIpAddress(KeySysLambdaContext context, DataTable dataTable)
        {
            LogInfo(context, "INFO", $"Update IPAddress for {dataTable.Rows.Count} devices");
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "dbo.usp_Update_MobilityDevice_IPAddress";
                        SqlParameter newrecordParam = cmd.Parameters.Add("@UpdatedValues", SqlDbType.Structured);
                        cmd.Parameters["@UpdatedValues"].Value = dataTable;
                        cmd.Parameters["@UpdatedValues"].TypeName = "dbo.UpdateMobilityDeviceIpAddress";
                        var returnParameter = cmd.Parameters.Add("@ReturnVal", SqlDbType.Int);
                        returnParameter.Direction = ParameterDirection.ReturnValue;
                        connection.Open();

                        cmd.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "ERROR", $"Error Executing UpdateIpAddress: {ex.Message} {ex.StackTrace}");
            }
        }
        private DataRow AddToDataRow(DataTable table, string MSISDN, string IpAddress)
        {
            var dr = table.NewRow();
            dr[0] = MSISDN;
            dr[1] = IpAddress;
            return dr;
        }
    }
}
