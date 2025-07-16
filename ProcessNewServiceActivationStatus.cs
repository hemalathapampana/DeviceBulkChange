using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Helpers;
using AltaworxDeviceBulkChange.Constants;
using Amop.Core.Models.DeviceBulkChange;
using Amop.Core.Models.Telegence;
using Amop.Core.Models.Telegence.Api;
using Amop.Core.Repositories;
using Amop.Core.Services.Http;
using Amop.Core.Services.Telegence;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using Polly;
using AltaworxDeviceBulkChange.Models;
using System.Net.Http;
using Amop.Core.Models.Revio;
using Amop.Core.Constants;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core;
using Amop.Core.Logger;

namespace AltaworxDeviceBulkChange
{
    public partial class Function
    {
        private async Task ProcessNewServiceActivationStatusAsync(KeySysLambdaContext context,
    long bulkChangeId, int serviceProviderId, string carrierRatePool, string carrierDataGroup, int newServiceActivationIteration, long additionBulkChangeId, int retryNumber)
        {
            LogInfo(context, LogTypeConstant.Sub, $"ProcessNewServiceActivationStatusAsync(..,{bulkChangeId},{serviceProviderId},{carrierRatePool},{carrierDataGroup})");

            var sqlRetryPolicy = GetSqlTransientRetryPolicy(context);
            var logRepo = new DeviceBulkChangeLogRepository(context.CentralDbConnectionString, sqlRetryPolicy);

            var response = await GetNewServiceActivationStatusResponseAsync(context, logRepo, bulkChangeId, serviceProviderId, carrierRatePool, carrierDataGroup, newServiceActivationIteration, additionBulkChangeId, retryNumber);
            //Mark the request as error again for cases before response status check
            if (!response.IsSuccess)
            {
                await MarkProcessedForNewServiceActivationAsync(context, bulkChangeId, response.IsSuccess, response.Response, response.ICCIDList);
                //Mark the request assign customer fail when active new service fail
                if (additionBulkChangeId > 0)
                {
                    var iccidList = new List<string>();
                    var additionBulkChangeDetails = GetDeviceChanges(context, additionBulkChangeId, PortalTypeMobility, int.MaxValue, false);
                    if (additionBulkChangeDetails != null && additionBulkChangeDetails.Count > 0)
                    {
                        iccidList.AddRange(additionBulkChangeDetails.Select(x => x.ICCID));
                        var message = string.Format(LogCommonStrings.ASSIGN_CUSTOMER_FAILED_BECAUSE_ACTIVE_NEW_SERVICE_FAILED, string.Join(',', iccidList));
                        await MarkProcessedForNewServiceActivationAsync(context, additionBulkChangeId, response.IsSuccess, message, iccidList);
                        await bulkChangeRepository.MarkBulkChangeStatusAsync(context, additionBulkChangeId, BulkChangeStatus.PROCESSED);
                    }
                }
            }
        }

        private async Task<TelegenceActivationApiResponse> GetNewServiceActivationStatusResponseAsync(KeySysLambdaContext context,
           DeviceBulkChangeLogRepository logRepo, long bulkChangeId,
           int serviceProviderId, string carrierRatePool, string carrierDataGroup, int newServiceActivationIteration, long additionBulkChangeId, int retryNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"(..,,{bulkChangeId},{serviceProviderId},{carrierRatePool},{carrierDataGroup},{additionBulkChangeId})");
            var sqlRetryPolicy = GetSqlTransientAsyncRetryPolicy(context);

            var bulkChange = GetBulkChange(context, bulkChangeId);
            var telegenceApiAuthentication =
                GetTelegenceApiAuthentication(context.CentralDbConnectionString, serviceProviderId);

            var changes = GetDeviceChanges(context, bulkChangeId, PortalTypeMobility, int.MaxValue, false);
            if (changes == null || changes.Count == 0)
            {
                LogInfo(context, LogTypeConstant.Warning, $"No changes found for bulk change: {bulkChangeId}");
            }

            // Each request to Telegence in 1 object in this object's list
            // The response is taken from statusDetails => if any MarkProcessedForNewServiceActivation... is called for a device
            // => statusDetails is updated and can't be mapped to TelegenceActivationResponse
            // => the device won't be included in this list
            var activationResponseList = await GetAllActivationResponseFromAMOPAsync(context, bulkChangeId, sqlRetryPolicy);
            if (activationResponseList == null)
            {
                string errorMessage = $"Telegence: No valid activation response for Bulk Change Id: {bulkChangeId}";
                LogInfo(context, LogTypeConstant.Error, errorMessage);
                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChangeId,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Get New Service Activation: AMOP DB",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    RequestText = $"BulkChangeId: {bulkChangeId}",
                    ResponseStatus = BulkChangeStatus.ERROR,
                    ResponseText = errorMessage
                });

                //Mark all as error
                return new TelegenceActivationApiResponse
                {
                    Response = errorMessage,
                    IsSuccess = false
                };
            }
            if (telegenceApiAuthentication == null)
            {
                string errorMessage = $"Unable to get Telegence API Authentication for Service Provider: {serviceProviderId}";
                LogInfo(context, LogTypeConstant.Error, errorMessage);
                var firstChange = changes.First();
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChangeId,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Get New Service Activation: Telegence API Credentials",
                    MobilityDeviceChangeId = firstChange.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    RequestText = $"ServiceProviderId: {serviceProviderId}",
                    ResponseStatus = BulkChangeStatus.ERROR,
                    ResponseText = errorMessage
                });

                //Mark all as error
                return new TelegenceActivationApiResponse
                {
                    Response = errorMessage,
                    IsSuccess = false
                };
            }

            var foundationAccountNumberList = await GetFoundationAccountNumbersAsync(context, sqlRetryPolicy);
            var telegenceNewServiceActivationStagingTable = DataTableHelper.GetTableSchemaFromServer(context, TELEGENCE_NEWACTIVATION_STAGING_TABLE);
            var featureStagingTable = new TelegenceDeviceFeatureSyncTable(context);

            //default to true to check for any request error
            bool isAllProcessed = true;
            var processedDevices = new List<TelegenceNewActivationProcessedDevice>();

            if (_telegenceApiGetClient == null)
                _telegenceApiGetClient = new TelegenceAPIClient(new SingletonHttpClientFactory(), new HttpRequestFactory(), telegenceApiAuthentication,
                    context.IsProduction, $"{ProxyUrl}/api/Proxy/Get", context.logger);
            using (var httpClient = new HttpClient(new LambdaLoggingHandler()))
            {
                //Init httpclient
                httpClient.BaseAddress = new Uri($"{ProxyUrl}/api/Proxy/Get");

                foreach (var activationResponse in activationResponseList)
                {
                    //get iccid list of devices in the request for later steps
                    var iccidList = new List<string>();
                    activationResponse.TelegenceActivationResponse?
                                            .ForEach(activation => iccidList
                                                    .Add(activation.Service.ServiceCharacteristic
                                                            .FirstOrDefault(sc => sc.Name == "sim")?.Value));

                    //at most 200 devices per request id
                    var activationStatusId = activationResponse.TelegenceActivationResponse.FirstOrDefault()?.Id;
                    LogVariableValue(context, nameof(activationStatusId), activationStatusId);

                    var firstChange = changes.FirstOrDefault();
                    if (string.IsNullOrEmpty(activationStatusId))
                    {
                        string errorMessage = $"Telegence: No valid activation response for Bulk Change Id: {bulkChangeId}";
                        LogInfo(context, LogTypeConstant.Error, errorMessage);
                        logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChangeId,
                            ErrorText = errorMessage,
                            HasErrors = true,
                            LogEntryDescription = "Parse Activation Status Id: AMOP DB",
                            MobilityDeviceChangeId = firstChange.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = $"Getting Activation request from AMOP for Bulk Change Id: {bulkChangeId}",
                            ResponseStatus = BulkChangeStatus.ERROR,
                            ResponseText = errorMessage
                        });
                        //Mark as error
                        return new TelegenceActivationApiResponse
                        {
                            ICCIDList = iccidList,
                            Response = errorMessage,
                            IsSuccess = false
                        };
                    }

                    //Activation status response mapped by its request for the batch of devices
                    var activationStatusResponse = await GetActivationStatusResponseAsync(context, activationStatusId, _telegenceApiGetClient, httpClient);
                    if (activationStatusResponse == null)
                    {
                        string errorMessage = $"Telegence: Unable to get Activation status for activation status Id {activationStatusId}";
                        LogInfo(context, LogTypeConstant.Error, errorMessage);
                        firstChange = changes.First();
                        logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                        {
                            BulkChangeId = bulkChangeId,
                            ErrorText = errorMessage,
                            HasErrors = true,
                            LogEntryDescription = "Fetch Activation Status: Telegence API",
                            MobilityDeviceChangeId = firstChange.Id,
                            ProcessBy = "AltaworxDeviceBulkChange",
                            ProcessedDate = DateTime.UtcNow,
                            RequestText = $"Getting Activation Status for activation status Id: {activationStatusId}",
                            ResponseStatus = BulkChangeStatus.ERROR,
                            ResponseText = errorMessage
                        });

                        return new TelegenceActivationApiResponse
                        {
                            ICCIDList = iccidList,
                            Response = errorMessage,
                            IsSuccess = false
                        };
                    }

                    bool isRequestProcessed = activationStatusResponse.ResponseObject.All(res =>
                                                res.Service.Status.ToLower() != TelegenceActivationServiceStatus.Pending
                                                && res.Service.Status.ToLower() != TelegenceActivationServiceStatus.Reserved);
                    if (!isRequestProcessed)
                    {
                        isAllProcessed = false;
                    }

                    LogVariableValue(context, nameof(isRequestProcessed), isRequestProcessed);
                    LogVariableValue(context, nameof(isAllProcessed), isAllProcessed);
                    var errorStatusList = TelegenceActivationErrorStatusList.Split(",");
                    //go through each device in the request and add any successful activation into a list
                    processedDevices.AddRange(await ProcessActivationStatusResponseAsync(context, logRepo, bulkChange, changes, serviceProviderId, carrierRatePool,
                        carrierDataGroup, activationStatusResponse.ResponseObject, errorStatusList, _telegenceApiGetClient, httpClient, telegenceNewServiceActivationStagingTable,
                        featureStagingTable, foundationAccountNumberList, newServiceActivationIteration, activationResponse, telegenceApiAuthentication, additionBulkChangeId));
                }
                if (!isAllProcessed)
                {
                    //retry another time
                    newServiceActivationIteration++;
                    LogInfo(context, LogTypeConstant.Status, $"Enqueue to process remaining changes. Iteration {newServiceActivationIteration}");
                    await EnqueueDeviceBulkChangesAsync(context, bulkChange.Id, DeviceBulkChangeQueueUrl, SQS_SHORT_DELAY_SECONDS, retryNumber, true, serviceProviderId, carrierRatePool, carrierDataGroup, newServiceActivationIteration, additionBulkChangeId);
                }
                else
                {
                    var processedICCIDs = new List<string>();
                    foreach (var device in processedDevices)
                    {
                        LogVariableValue(context, nameof(device.ICCID), device.ICCID);
                        processedICCIDs.Add(device.ICCID);
                        // Need to mark the activation change as processed & process customer rate plan/pool for each device
                        // if not the status details will be overwritten => no activationResponse to rerun this process
                        telegenceNewServiceActivationStagingTable.Rows.Add(device.telegenceNewActivationStagingRow);
                        foreach (var feature in device.DeviceMobilityFeatures)
                        {
                            featureStagingTable.DataTable.Rows.Add(feature);
                        }
                        await MarkProcessedForNewServiceActivationByICCIDAsync(context, bulkChange.Id, true, device.newStatusDetails, device.ICCID, device.subscriberNumber);
                    }

                    // Put device data into staging table
                    LogInfo(context, LogTypeConstant.Status, LogCommonStrings.SQL_BULK_COPY_START);
                    await TruncateActivationStagingAsync(context, serviceProviderId, sqlRetryPolicy);
                    SqlBulkCopy(context, context.CentralDbConnectionString, telegenceNewServiceActivationStagingTable, DatabaseTableNames.TelegenceNewServiceActivationStaging);
                    // Merge all device that are in staging
                    await LoadActivationFromStagingAsync(context, serviceProviderId, sqlRetryPolicy);
                    LogInfo(context, LogTypeConstant.Info, LogCommonStrings.NEWLY_ACTIVATED_SERVICES_ADDED_SUCCESSFULLY);

                    //Start add customer rate plan + rate pool process 
                    foreach (var device in processedDevices)
                    {
                        await ProcessCustomerRateplanAfterNewService(context, logRepo, bulkChange, device.change, device.ICCID, device.subscriberNumber, device.change.Status);
                    }
                    // Put the mobility feature data into staging table
                    LogInfo(context, LogTypeConstant.Status, LogCommonStrings.SQL_BULK_COPY_START_FOR_MOBILITY_FEATURES);
                    await TruncateDeviceMobilityFeatureStagingAsync(context, sqlRetryPolicy);
                    SqlBulkCopy(context, context.CentralDbConnectionString, featureStagingTable.DataTable, DatabaseTableNames.TelegenceDeviceMobilityFeatureStaging);
                    // Merge all feature mapping from staging
                    await UpdateDeviceFeaturesFromStaging(context, sqlRetryPolicy, bulkChange.ServiceProviderId);
                    LogInfo(context, LogTypeConstant.Info, LogCommonStrings.MOBILITY_FEATURES_FOR_NEWLY_ACTIVATED_SERVICES_ADDED_SUCCESSFULLY);

                    // If any change have IP provisioning, enqueue new instance
                    if (changes.Any(change => change.ChangeRequest.Contains("StaticIPProvision") && !change.ChangeRequest.Contains("StaticIPProvision\":null")))
                    {
                        await EnqueueBatchedTelegenceIPProvisioningAsync(context, bulkChangeId, DeviceBulkChangeQueueUrl, DELAY_TELEGENCE_IP_PROVISION_MESSAGE, TelegenceNewActivationStep.RequestIPProvision);
                    }

                    // Assign customer
                    if (additionBulkChangeId > 0)
                    {
                        LogInfo(context, LogTypeConstant.Status, $"Addition BulkChange For Assign Customer {additionBulkChangeId}");
                        if (processedDevices != null && processedDevices.Count > 0)
                        {
                            await ProcessAssignCustomerByUploadFile(context, additionBulkChangeId, processedDevices, retryNumber);
                        }
                        else
                        {
                            // Mark processed Assign customer if no device activated
                            LogInfo(context, LogTypeConstant.Info, LogCommonStrings.NO_DEVICES_ACTIVATED);
                            await bulkChangeRepository.MarkBulkChangeStatusAsync(context, additionBulkChangeId, BulkChangeStatus.PROCESSED);
                        }
                    }
                }

                return new TelegenceActivationApiResponse
                {
                    IsSuccess = true
                };
            }
        }

        private async Task ProcessAssignCustomerByUploadFile(KeySysLambdaContext context, long bulkChangeId, List<TelegenceNewActivationProcessedDevice> processedDevices, int retryNumber)
        {
            LogInfo(context, LogTypeConstant.Status, "Start For Assign Customer");
            var bulkChangeDetails = GetDeviceChanges(context, bulkChangeId, PortalTypeMobility, int.MaxValue, false);
            foreach (var change in bulkChangeDetails)
            {
                var processDevice = processedDevices.FirstOrDefault(x => x.ICCID == change.ICCID);
                if (processDevice != null)
                {
                    var subscriberNumber = processDevice.subscriberNumber;
                    // ChangeRequest
                    var changeRequest = JsonConvert.DeserializeObject<BulkChangeAssociateCustomer>(change.ChangeRequest);
                    changeRequest.Number = subscriberNumber;
                    UpdateModelMobilityDevice(context, change.Id, subscriberNumber, JsonConvert.SerializeObject(changeRequest));
                }
                else
                {
                    var message = $"Assign Customer Fail For Device: {change.ICCID} Because Active New Service Failed";
                    await MarkProcessedForMobilityDeviceChangeAsync(context, change.Id, true, message);
                }
            }
            //Queue Assign Customer
            LogInfo(context, LogTypeConstant.Info, $"Queue Assign Customer For BulkChangeId : {bulkChangeId}");
            await EnqueueDeviceBulkChangesAsync(context, bulkChangeId, DeviceBulkChangeQueueUrl, SQS_SHORT_DELAY_SECONDS, retryNumber);
        }
        private void UpdateModelMobilityDevice(KeySysLambdaContext context, long mobilityDeviceChangeId, string phoneNumber, string changeRequest)
        {
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandText = Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_UPDATE_MODEL_MOBILITY_DEVICE_CHANGE;
                        command.Parameters.AddWithValue("@id", mobilityDeviceChangeId);
                        command.Parameters.AddWithValue("@phoneNumber", phoneNumber);
                        command.Parameters.AddWithValue("@changeRequest", changeRequest);
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
                        connection.Open();

                        var affectedRows = command.ExecuteNonQuery();
                        if (affectedRows <= Amop.Core.Constants.SQLConstant.NoStatementSQLExecuteDetected)
                        {
                            throw new Exception(LogCommonStrings.ERROR_WHILE_TRYING_TO_UPDATE_DATA_INTO_MOBILITY_DEVICE_CHANGE);
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
        }

        private async Task<List<TelegenceNewActivationProcessedDevice>> ProcessActivationStatusResponseAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes, int serviceProviderId, string carrierRatePool, string carrierDataGroup,
            List<TelegenceActivationResponse> activationStatusResponse, string[] errorStatusList, TelegenceAPIClient telegenceApiClient, HttpClient httpClient,
            DataTable telegenceNewServiceActivationStagingTable, TelegenceDeviceFeatureSyncTable featureStagingTable, List<TelegenceFANBAN> foundationAccountNumberList,
            int newServiceActivationIteration, TelegenceActivationProxyResponse actionResponse, TelegenceAPIAuthentication telegenceApiAuthentication, long additionBulkChangeId)
        {
            if (_telegenceApiPatchClient == null)
                _telegenceApiPatchClient = new TelegenceAPIClient(new SingletonHttpClientFactory(), new HttpRequestFactory(), telegenceApiAuthentication,
                    context.IsProduction, $"{ProxyUrl}/api/Proxy/Patch", context.logger);

            var dataRows = new List<DataRow>();
            var processedDevices = new List<TelegenceNewActivationProcessedDevice>();
            //Go through each number and check their status
            //Rechecked processed changes so that all active services are added into staging table
            LogInfo(context, LogTypeConstant.Sub,
                $"ProcessActivationStatusResponseAsync(..,..,{bulkChange.Id},..,{serviceProviderId},{carrierRatePool},{carrierDataGroup},..)");

            // Get username from bulk change request
            var username = string.Empty;
            var firstChange = changes.FirstOrDefault();
            if (firstChange != null)
            {
                var telegenceChangeRequest = JsonConvert.DeserializeObject<TelegenceActivationChangeRequest>(firstChange.ChangeRequest);
                var relatedParty = telegenceChangeRequest.TelegenceActivationRequest.RelatedParty;
                var firstName = relatedParty.FirstOrDefault()?.FirstName;
                var lastName = relatedParty.FirstOrDefault()?.LastName;
                if (firstName != null && lastName != null)
                {
                    username = firstName + " " + lastName;
                }
            }

            foreach (var response in activationStatusResponse)
            {
                var iccid = response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "sim")?.Value;
                var subscriberNumber = response.Service.SubscriberNumber;
                var change = changes.FirstOrDefault(x => x.ICCID == iccid);
                bool isError = false;

                if (change == null)
                {
                    LogInfo(context, LogTypeConstant.Error,
                        $"Could not find matching change request for ICCID: {iccid}. Defaulting to first change for logging.");
                    change = changes.First();
                }
                //Skipping changes that already failed the first request
                if (change.Status == ChangeStatus.API_FAILED)
                {
                    LogInfo(context, "INFO", $"Change request for ICCID: {iccid} is already checked. Skipping to next change.");
                    continue;
                }
                LogInfo(context, "INFO", $"Activation Status for {response.Service.SubscriberNumber} is: {response.InteractionStatus}");
                LogInfo(context, "INFO", $"Service Status for {response.Service.SubscriberNumber} is: {response.Service.Status}");

                // If error ( rejected / reserved ) => the change is logged
                if (errorStatusList.Contains(response.InteractionStatus.ToLower())
                    || errorStatusList.Contains(response.Service.Status.ToLower()))
                {
                    var message = $"Telegence: Device Activation {response.Service.Status.ToLower()} for SIM: {iccid}";
                    //check error message and add it to the error text
                    if (!string.IsNullOrEmpty(response.Service.Error))
                    {
                        message = $"{message}{Environment.NewLine}{response.Service.Error}";
                    }
                    LogInfo(context, "INFO", message);
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = message,
                        HasErrors = true,
                        LogEntryDescription = "Parse Telegence Response: Validation",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        RequestText = JsonConvert.SerializeObject(response),
                        ResponseStatus = BulkChangeStatus.ERROR,
                        ResponseText = message
                    });
                    //Only mark as error at 6th try or rejected
                    if (response.Service.Status.ToLower() == TelegenceActivationServiceStatus.Rejected
                        || newServiceActivationIteration >= NEW_SERVICE_ACTIVATION_MAX_COUNT)
                    {
                        await MarkProcessedForNewServiceActivationByICCIDAsync(context, bulkChange.Id, false, message, iccid, subscriberNumber);

                        await MarkProcessedForNewServiceActivationByICCIDAsync(context, additionBulkChangeId, false, message, iccid, subscriberNumber, BulkChangeStatus.ERROR);
                    }
                    continue;
                }

                //Try checking phone number (in case of Pending status)
                if (string.IsNullOrEmpty(subscriberNumber))
                {
                    LogInfo(context, LogTypeConstant.Info, $"No subscriber number is provided for ICCID: {iccid}. Skipping to next device.");
                    continue;
                }
                //Update change to show subscriberNumber
                await MarkProcessedForNewServiceActivationByICCIDAsync(context, bulkChange.Id, true, null, iccid, subscriberNumber);

                TelegenceDeviceDetailsProxyResponse deviceDetailResponse = null;
                try
                {
                    deviceDetailResponse = await GetTelegenceDeviceDetailResponseAsync(context, telegenceApiClient, httpClient, subscriberNumber);
                }
                catch (Exception ex)
                {
                    isError = true;
                    LogInfo(context, LogTypeConstant.Error, $"GetTelegenceDeviceDetailResponseAsync Call Failed: {ex.Message} {ex.StackTrace}");
                }

                string deviceDetailsMessage;

                if (deviceDetailResponse == null || deviceDetailResponse?.TelegenceDeviceDetailResponse == null)
                {
                    isError = true;
                    deviceDetailsMessage = $"Unable to get device details for {subscriberNumber}.";
                    LogInfo(context, LogTypeConstant.Error, deviceDetailsMessage);

                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = deviceDetailsMessage,
                        HasErrors = true,
                        LogEntryDescription = "Fetch Device Details: Telegence API",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        RequestText = JsonConvert.SerializeObject(response),
                        ResponseStatus = BulkChangeStatus.ERROR,
                        ResponseText = deviceDetailsMessage
                    });
                }
                else if (!deviceDetailResponse.IsSuccessful)
                {
                    deviceDetailsMessage =
                        $"Unable to get device details for {subscriberNumber}. {deviceDetailResponse.TelegenceDeviceDetailResponse.ErrorDescription}";

                    isError = true;
                    LogInfo(context, LogTypeConstant.Error, deviceDetailsMessage);

                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = deviceDetailsMessage,
                        HasErrors = true,
                        LogEntryDescription = LogCommonStrings.DEVICE_DETAILS_TELEGENCE_API,
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = LogCommonStrings.ALTAWORX_DEVICE_BULK_CHANGE,
                        ProcessedDate = DateTime.UtcNow,
                        RequestText = JsonConvert.SerializeObject(response),
                        ResponseStatus = BulkChangeStatus.ERROR,
                        ResponseText = deviceDetailsMessage
                    });
                }
                else
                {
                    deviceDetailsMessage = username;

                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = null,
                        HasErrors = false,
                        LogEntryDescription = LogCommonStrings.DEVICE_DETAILS_TELEGENCE_API,
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = LogCommonStrings.ALTAWORX_DEVICE_BULK_CHANGE,
                        ProcessedDate = DateTime.UtcNow,
                        RequestText = JsonConvert.SerializeObject(response),
                        ResponseStatus = BulkChangeStatus.PROCESSED,
                        ResponseText = CommonConstants.OK
                    });
                }

                if (response.Service.Status.ToLower() == TelegenceActivationServiceStatus.Active
                    && !isError)
                {
                    // create a data row
                    var deviceDataRow = await AddToDataRow(context, telegenceNewServiceActivationStagingTable, telegenceApiClient, response, serviceProviderId, foundationAccountNumberList, carrierRatePool, carrierDataGroup, username, logRepo, bulkChange.Id, change.Id);
                    if (deviceDataRow == null)
                    {
                        continue;
                    }

                    var featureDataRows = new List<DataRow>();
                    foreach (var feature in GetDeviceOfferingCodes(response.Service))
                    {
                        featureDataRows.Add(featureStagingTable.AddRow(subscriberNumber.Trim(), feature.Trim()));
                    }
                    // use this model to return needed data
                    processedDevices.Add(new TelegenceNewActivationProcessedDevice()
                    {
                        telegenceNewActivationStagingRow = deviceDataRow,
                        DeviceMobilityFeatures = featureDataRows,
                        change = change,
                        ICCID = iccid,
                        subscriberNumber = subscriberNumber,
                        newStatusDetails = $"ICCID: {iccid} {response.InteractionStatus}. Date Completed: {response.InteractionDateComplete}. Username: {deviceDetailsMessage}. "
                    });
                }

                //remove feature
                var telegenceRequest = JsonConvert.DeserializeObject<TelegenceActivationChangeRequest>(change.ChangeRequest);
                var removeOfferingCodes = telegenceRequest.TelegenceActivationRequest.Service.ServiceCharacteristic.Where(x => x.Name.Equals(Common.CommonString.REMOVE_SOC_CODE_STRING)).ToList();
                DeviceChangeResult<TelegenceConfigurationRequest, TelegenceDeviceDetailsProxyResponse> apiResultRemoveOfferingCode = null;
                //get device id 
                var sqlCommand = "SELECT Top 1 Id FROM TelegenceDevice WHERE SubscriberNumber = @subscriberNumber AND IsActive = 1 AND IsDeleted = 0";
                await using var conn = new SqlConnection(context.CentralDbConnectionString);
                await using var cmd = new SqlCommand(sqlCommand, conn) { CommandType = CommandType.Text };
                cmd.Parameters.AddWithValue("@subscriberNumber", subscriberNumber);
                conn.Open();
                var resultQuery = await cmd.ExecuteScalarAsync();
                var deviceId = 0;
                if (response != null && !string.IsNullOrEmpty(response.ToString()) && !Equals(resultQuery, null))
                {
                    deviceId = (int)resultQuery;
                }

                if (removeOfferingCodes != null && removeOfferingCodes.Count > 0 && deviceId != 0)
                {
                    var telegenceConfigurationRequest = new TelegenceConfigurationRequest()
                    {
                        serviceCharacteristic = removeOfferingCodes,
                        effectiveDate = string.Empty
                    };

                    apiResultRemoveOfferingCode = await _telegenceApiPatchClient.UpdateTelegenceMobilityConfigurationAsync(telegenceConfigurationRequest, TelegenceSubscriberUpdateURL + subscriberNumber);
                    var isSuccessfulRemoveSOCCode = !apiResultRemoveOfferingCode?.HasErrors ?? false;

                    //remove feature code in db
                    if (isSuccessfulRemoveSOCCode)
                    {
                        var soccode = string.Join(",", removeOfferingCodes.Select(x => x.Value));
                        LogInfo(context, LogTypeConstant.Info, $"Remove SOCCode: usp_DeleteTelegenceMobilityFeature ({deviceId}, {soccode})");
                        //delete feature assign for telegence device
                        string sqlProcDelete = "usp_DeleteTelegenceMobilityFeature";

                        await using var connProcDelete = new SqlConnection(context.CentralDbConnectionString);
                        await using var cmdProcDelete = new SqlCommand(sqlProcDelete, connProcDelete) { CommandType = CommandType.StoredProcedure };
                        connProcDelete.Open();
                        cmdProcDelete.Parameters.AddWithValue("@deviceId", deviceId);
                        cmdProcDelete.Parameters.AddWithValue("@SOCCode", soccode);
                        await cmdProcDelete.ExecuteNonQueryAsync();
                    }

                    //create bulkchange log remove SOCCode 
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = apiResultRemoveOfferingCode.HasErrors ? JsonConvert.SerializeObject(apiResultRemoveOfferingCode.ResponseObject.TelegenceDeviceDetailResponse) : null,
                        HasErrors = apiResultRemoveOfferingCode.HasErrors,
                        LogEntryDescription = "Telegence Update Mobility Configuration: Telegence API",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = isSuccessfulRemoveSOCCode ? BulkChangeStatus.PENDING : BulkChangeStatus.ERROR,
                        RequestText = apiResultRemoveOfferingCode.ActionText + Environment.NewLine + JsonConvert.SerializeObject(apiResultRemoveOfferingCode.RequestObject),
                        ResponseText = apiResultRemoveOfferingCode.ResponseObject != null ? JsonConvert.SerializeObject(apiResultRemoveOfferingCode.ResponseObject) : string.Empty
                    });
                }
            }
            return processedDevices;
        }

        private async Task ProcessCustomerRateplanAfterNewService(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, BulkChangeDetailRecord change, string iccid, string subscriberNumber, string currentChangeStatus)
        {
            bool isValidated;
            string errorMessage = "";
            var telegenceChangeRequest =
                    JsonConvert.DeserializeObject<TelegenceActivationChangeRequest>(change.ChangeRequest);
            isValidated = telegenceChangeRequest.AddCustomerRatePlan;
            if (telegenceChangeRequest.AddCustomerRatePlan)
            {
                // id 0 is considered as null
                int? customerRatePlanIdToSubmit = null;
                if (telegenceChangeRequest.CustomerRatePlan != null &&
                    telegenceChangeRequest.CustomerRatePlan != 0)
                {
                    if (telegenceChangeRequest.CustomerRatePlan > 0)
                    {
                        customerRatePlanIdToSubmit = telegenceChangeRequest.CustomerRatePlan;
                    }
                    else
                    {
                        LogInfo(context, LogTypeConstant.Warning, $"Customer Rate Plan Id not valid: {telegenceChangeRequest.CustomerRatePlan}");
                        isValidated = false;
                        errorMessage += $"Customer Rate Plan/Pool not set. Customer Rate Plan Id not valid: {telegenceChangeRequest.CustomerRatePlan}";
                    }
                }

                // id 0 is considered as null
                int? customerRatePoolIdToSubmit = null;
                if (telegenceChangeRequest.CustomerRatePool != null &&
                    telegenceChangeRequest.CustomerRatePool != 0)
                {
                    if (telegenceChangeRequest.CustomerRatePool > 0)
                    {
                        customerRatePoolIdToSubmit = telegenceChangeRequest.CustomerRatePool;
                    }
                    else
                    {
                        LogInfo(context, LogTypeConstant.Warning, $"Customer Rate Pool Id not valid: {telegenceChangeRequest.CustomerRatePool}");
                        isValidated = false;
                        errorMessage += $"Customer Rate Plan/Pool not set. Customer Rate Pool Id not valid: {telegenceChangeRequest.CustomerRatePool}";
                    }
                }

                if (isValidated)
                {
                    context.logger.LogInfo(LogTypeConstant.Info, $"Processing Customer Rate Plan update {change.MSISDN}");
                    var ratePlanChangeResult = await ProcessCustomerRatePlanChangeBySubNumberAsync(bulkChange.Id, subscriberNumber,
                        customerRatePlanIdToSubmit, customerRatePoolIdToSubmit, DateTime.Now, null,
                        context.CentralDbConnectionString, context.logger);
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = ratePlanChangeResult.HasErrors ? ratePlanChangeResult.ResponseObject : null,
                        HasErrors = ratePlanChangeResult.HasErrors,
                        LogEntryDescription = "Activate New Service: Update AMOP Customer Rate Plan",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = ratePlanChangeResult.HasErrors ? BulkChangeStatus.ERROR : BulkChangeStatus.PROCESSED,
                        RequestText = ratePlanChangeResult.ActionText + Environment.NewLine + ratePlanChangeResult.RequestObject,
                        ResponseText = ratePlanChangeResult.ResponseObject
                    });
                    if (ratePlanChangeResult.HasErrors)
                    {
                        isValidated = false;
                        errorMessage = ratePlanChangeResult.ResponseObject;
                    }
                }
                else
                {
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChange.Id,
                        ErrorText = errorMessage,
                        HasErrors = true,
                        LogEntryDescription = "Activate New Service: Update AMOP Customer Rate Plan",
                        MobilityDeviceChangeId = change.Id,
                        ProcessBy = "AltaworxDeviceBulkChange",
                        ProcessedDate = DateTime.UtcNow,
                        ResponseStatus = BulkChangeStatus.ERROR,
                        RequestText = "usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDeviceByNumber" + Environment.NewLine + $"customerRatePlanId: {customerRatePoolIdToSubmit}, customerRatePoolId: {customerRatePoolIdToSubmit}",
                        ResponseText = errorMessage
                    });
                }
                await MarkProcessedForNewServiceActivationByICCIDAsync(context, bulkChange.Id, isValidated,
                        JsonConvert.SerializeObject(errorMessage), iccid, subscriberNumber);
            }
            else
            {
                //logging
                LogInfo(context, LogTypeConstant.Info, $"Skipping add customer rate plan for device {iccid}");
                logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = "",
                    HasErrors = false,
                    LogEntryDescription = "Add Customer Rate Plan",
                    MobilityDeviceChangeId = change.Id,
                    ProcessBy = "AltaworxDeviceBulkChange",
                    ProcessedDate = DateTime.UtcNow,
                    RequestText = "Adding customer rate plan",
                    ResponseStatus = BulkChangeStatus.PROCESSED,
                    ResponseText = $"Skipping Add Customer Rate Plan for device {iccid}"
                });
            }
        }

        private async Task<DeviceChangeResult<string, List<TelegenceActivationResponse>>> GetActivationStatusResponseAsync(KeySysLambdaContext context, string activationStatusId, TelegenceAPIClient telegenceApiClient, HttpClient httpClient)
        {
            LogInfo(context, LogTypeConstant.Sub, $"GetActivationStatusResponseAsync(..,{activationStatusId},..)");
            DeviceChangeResult<string, List<TelegenceActivationResponse>> response =
                new DeviceChangeResult<string, List<TelegenceActivationResponse>>()
                {
                    ResponseObject = new List<TelegenceActivationResponse>()
                };
            var httpRetryPolicy = GetHttpRetryPolicy(context);
            await httpRetryPolicy.ExecuteAsync(async () =>
            {
                var apiResponse = await telegenceApiClient.CheckActivationStatus($"{TelegenceDeviceStatusUpdateURL}/{activationStatusId}", httpClient);
                if (apiResponse == null || apiResponse.HasErrors)
                {
                    LogInfo(context, LogTypeConstant.Error, $"Error checking activation status for: {activationStatusId}");
                    response = null;
                }
                else
                {
                    response.ResponseObject = apiResponse.ResponseObject.TelegenceActivationResponse;
                }
            });

            return response;
        }

        private async Task<TelegenceDeviceDetailsProxyResponse> GetTelegenceDeviceDetailResponseAsync(KeySysLambdaContext context,
            TelegenceAPIClient telegenceApiClient, HttpClient httpClient, string phoneNumber)
        {
            LogInfo(context, LogTypeConstant.Sub, $"GetTelegenceDeviceDetailResponseAsync(..,..,{phoneNumber})");
            var response = new TelegenceDeviceDetailsProxyResponse();
            var httpRetryPolicy = GetHttpRetryPolicy(context);
            await httpRetryPolicy.ExecuteAsync(async () =>
            {
                var deviceDetailsEndpoint = $"{TelegenceSubscriberUpdateURL}{phoneNumber}";
                var apiResponse = await telegenceApiClient.GetDeviceDetails(deviceDetailsEndpoint, httpClient);
                if (apiResponse.HasErrors)
                {
                    LogInfo(context, LogTypeConstant.Error, $"Error getting device details for: {phoneNumber}");
                }
                response = apiResponse.ResponseObject;
            });

            return response;
        }
        //Legacy code
        private async Task<TelegenceActivationResponse> GetActivationResponseFromAMOPAsync(KeySysLambdaContext context,
            long bulkChangeId, IAsyncPolicy retryPolicy)
        {
            LogInfo(context, LogTypeConstant.Sub, "GetActivationResponseFromAMOPAsync()");
            TelegenceActivationResponse activationResponse = null;
            await retryPolicy.ExecuteAsync(async () =>
            {
                var sqlCommand = "SELECT TOP 1 StatusDetails FROM [dbo].[Mobility_DeviceChange] WHERE BulkChangeId = @BulkChangeId AND IsProcessed = 1 AND IsActive = 1 AND IsDeleted = 0";
                await using var conn = new SqlConnection(context.CentralDbConnectionString);
                await using var cmd = new SqlCommand(sqlCommand, conn) { CommandType = CommandType.Text };
                cmd.Parameters.AddWithValue("@BulkChangeId", bulkChangeId);
                conn.Open();
                var response = await cmd.ExecuteScalarAsync();
                if (response != null && !string.IsNullOrEmpty(response.ToString()))
                {
                    var result = JsonConvert.DeserializeObject<List<TelegenceActivationResponse>>(response.ToString());
                    activationResponse = result.FirstOrDefault();
                }
            });
            return activationResponse;
        }
        private async Task<List<TelegenceActivationProxyResponse>> GetAllActivationResponseFromAMOPAsync(KeySysLambdaContext context,
         long bulkChangeId, IAsyncPolicy retryPolicy)
        {
            LogInfo(context, LogTypeConstant.Sub, "GetActivationResponseFromAMOPAsync()");
            List<TelegenceActivationProxyResponse> activationResponses = new List<TelegenceActivationProxyResponse>();
            await retryPolicy.ExecuteAsync(async () =>
            {
                var sqlCommand = "SELECT mdc.StatusDetails FROM[dbo].[Mobility_DeviceChange] mdc INNER JOIN(SELECT StatusDetails, MIN(id) as id FROM[dbo].[Mobility_DeviceChange] WHERE BulkChangeId = @BulkChangeId GROUP BY StatusDetails) as distictRows on mdc.StatusDetails = distictRows.StatusDetails and mdc.id = distictRows.id WHERE BulkChangeId = @BulkChangeId AND IsProcessed = 1 AND IsActive = 1 AND IsDeleted = 0";
                await using var conn = new SqlConnection(context.CentralDbConnectionString);
                await using var cmd = new SqlCommand(sqlCommand, conn) { CommandType = CommandType.Text };
                cmd.Parameters.AddWithValue("@BulkChangeId", bulkChangeId);
                conn.Open();
                SqlDataReader rdr = await cmd.ExecuteReaderAsync();
                {
                    if (rdr.HasRows)
                    {
                        while (rdr.Read())
                        {
                            if (rdr.GetString("StatusDetails") != null && !string.IsNullOrEmpty(rdr[0].ToString()))
                            {
                                if (rdr[0].ToString().TryParseJson(out List<TelegenceActivationResponse> result))
                                {
                                    var singleResponse = new TelegenceActivationProxyResponse
                                    {
                                        IsSuccessful = true,
                                        TelegenceActivationResponse = result
                                    };
                                    activationResponses.Add(singleResponse);
                                }
                            }
                        }
                    }
                }
            });
            return activationResponses;
        }
        private async Task<List<TelegenceFANBAN>> GetFoundationAccountNumbersAsync(KeySysLambdaContext context, IAsyncPolicy retryPolicy)
        {
            var foundationAccountNumberList = new List<TelegenceFANBAN>();
            await retryPolicy.ExecuteAsync(async () =>
            {
                var sqlCommand = "SELECT DISTINCT(BillingAccountNumber), FoundationAccountNumber FROM [dbo].[TelegenceDevice]";
                await using var conn = new SqlConnection(context.CentralDbConnectionString);
                await using var cmd = new SqlCommand(sqlCommand, conn);
                cmd.CommandType = CommandType.Text;
                conn.Open();
                var rdr = await cmd.ExecuteReaderAsync();
                while (rdr.Read())
                {
                    var ban = rdr["BillingAccountNumber"].ToString();
                    var fan = rdr["FoundationAccountNumber"].ToString();
                    foundationAccountNumberList.Add(new TelegenceFANBAN { FoundationAccountNo = fan, BillingAccountNo = ban });
                }
            });
            return foundationAccountNumberList;
        }
        //Legacy code
        private DataTable GetTelegenceNewServiceActivationStagingTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("FoundationAccountNumber");
            table.Columns.Add("BillingAccountNumber");
            table.Columns.Add("SubscriberNumber");
            table.Columns.Add("SubscriberActivatedDate");
            table.Columns.Add("SubscriberNumberStatus");
            table.Columns.Add("SingleUserCode");
            table.Columns.Add("ServiceZipCode");
            table.Columns.Add("ICCID");
            table.Columns.Add("IMEI");
            table.Columns.Add("ServiceProviderId");
            table.Columns.Add("CarrierRatePool");
            table.Columns.Add("CarrierDataGroup");
            table.Columns.Add("Username");
            return table;
        }

        private async Task<DataRow> AddToDataRow(KeySysLambdaContext context, DataTable table, TelegenceAPIClient telegenceApiClient, TelegenceActivationResponse response, int serviceProviderId,
            List<TelegenceFANBAN> foundationAccountNumberList, string carrierRatePool, string carrierDataGroup, string username, DeviceBulkChangeLogRepository logRepo, long bulkChangeId, long changeId)
        {
            var dataRow = table.NewRow();
            var foundationAccountNumber = foundationAccountNumberList.FirstOrDefault(fan => fan.BillingAccountNo == response.BillingAccount.Id)?.FoundationAccountNo;
            foundationAccountNumber = await IsFoundationAccountNumberValidAsync(context, telegenceApiClient, response, logRepo, bulkChangeId, changeId, foundationAccountNumber);
            if (!string.IsNullOrEmpty(foundationAccountNumber))
            {
                dataRow[CommonColumnNames.FoundationAccountNumber] = foundationAccountNumber;
                dataRow[CommonColumnNames.BillingAccountNumber] = response.BillingAccount.Id;
                dataRow[CommonColumnNames.SubscriberNumber] = response.Service.SubscriberNumber;
                dataRow[CommonColumnNames.SubscriberActivatedDate] = DateTime.UtcNow;
                dataRow[CommonColumnNames.SubscriberNumberStatus] = response.Service.Status;
                dataRow[CommonColumnNames.SingleUserCode] = response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "singleUserCode")?.Value;
                dataRow[CommonColumnNames.ServiceZipCode] = response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "serviceZipCode")?.Value;
                dataRow[CommonColumnNames.ICCID] = response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "sim")?.Value;
                dataRow[CommonColumnNames.IMEI] = response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "IMEI")?.Value;
                dataRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
                dataRow[CommonColumnNames.CarrierRatePool] = carrierRatePool;
                dataRow[CommonColumnNames.CarrierDataGroup] = carrierDataGroup;
                dataRow[CommonColumnNames.Username] = username;
                return dataRow;
            }

            return null;
        }

        private async Task<string> IsFoundationAccountNumberValidAsync(KeySysLambdaContext context, TelegenceAPIClient telegenceApiClient, TelegenceActivationResponse response,
            DeviceBulkChangeLogRepository logRepo, long bulkChangeId, long changeId, string foundationAccountNumber)
        {
            if (string.IsNullOrWhiteSpace(foundationAccountNumber))
            {
                var banFromAPI = await telegenceApiClient.GetBillingAccountNumber(response.BillingAccount.Id, URLConstants.TELEGENCE_BAN_DETAIL_GET_URL);
                if (!string.IsNullOrWhiteSpace(banFromAPI?.BusinessAccount?.Fan))
                {
                    return banFromAPI.BusinessAccount.Fan;
                }
                else
                {
                    logRepo.AddMobilityLogEntry(new CreateMobilityDeviceBulkChangeLog()
                    {
                        BulkChangeId = bulkChangeId,
                        ErrorText = null,
                        HasErrors = true,
                        LogEntryDescription = LogCommonStrings.GET_BILLING_ACCOUNT_NUMBER_FROM_TELEGENCE_API,
                        MobilityDeviceChangeId = changeId,
                        ProcessBy = context.Context.FunctionName,
                        ProcessedDate = DateTime.UtcNow,
                        RequestText = JsonConvert.SerializeObject(response),
                        ResponseStatus = BulkChangeStatus.API_FAILED,
                        ResponseText = string.Format(LogCommonStrings.BILLING_ACCOUNT_NUMBER_NOT_FOUND, response.BillingAccount.Id)
                    });
                    return string.Empty;
                }
            }
            else
            {
                return foundationAccountNumber;
            }
        }

        private async Task LoadActivationFromStagingAsync(KeySysLambdaContext context, int serviceProviderId, IAsyncPolicy retryPolicy)
        {
            LogInfo(context, "SUB", $"LoadActivationFromStagingAsync()");

            await retryPolicy.ExecuteAsync(async () =>
            {
                await using SqlConnection conn = new SqlConnection(context.CentralDbConnectionString);
                await using SqlCommand cmd = new SqlCommand("dbo.usp_Telegence_Update_NewServiceActivations_FromStaging", conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                conn.Open();
                await cmd.ExecuteNonQueryAsync();
            });
        }

        private async Task TruncateActivationStagingAsync(KeySysLambdaContext context, int serviceProviderId, IAsyncPolicy retryPolicy)
        {
            LogInfo(context, "SUB", $"TruncateActivationStagingAsync()");
            await retryPolicy.ExecuteAsync(async () =>
            {
                var sqlCommand = "DELETE FROM [dbo].[TelegenceNewServiceActivation_Staging] WHERE ServiceProviderId = @ServiceProviderId";
                await using var conn = new SqlConnection(context.CentralDbConnectionString);
                await using var cmd = new SqlCommand(sqlCommand, conn) { CommandType = CommandType.Text };
                cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                conn.Open();
                await cmd.ExecuteNonQueryAsync();
            });

        }
        private static IEnumerable<string> GetDeviceOfferingCodes(Amop.Core.Models.Telegence.Api.Service service)
        {
            if (service?.ServiceCharacteristic == null)
            {
                return new List<string>();
            }

            return service.ServiceCharacteristic.Where(x => x.Name.StartsWith("offeringCode")).Select(x => x.Value).ToList();
        }

        private async Task TruncateDeviceMobilityFeatureStagingAsync(KeySysLambdaContext context, IAsyncPolicy retryPolicy)
        {
            await retryPolicy.ExecuteAsync(async () =>
            {
                string sqlCommand = "TRUNCATE TABLE [dbo].[TelegenceDeviceMobilityFeature_Staging]";

                await using var conn = new SqlConnection(context.CentralDbConnectionString);
                await using var cmd = new SqlCommand(sqlCommand, conn) { CommandType = CommandType.Text };
                conn.Open();
                await cmd.ExecuteNonQueryAsync();
            });
        }

        private async Task UpdateDeviceFeaturesFromStaging(KeySysLambdaContext context, IAsyncPolicy retryPolicy, int serviceProviderId)
        {
            await retryPolicy.ExecuteAsync(async () =>
            {
                var parameters = new List<SqlParameter>()
                {
                    new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId)
                };
                Amop.Core.Helpers.SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult(ParameterizedLog(context),
                       context.CentralDbConnectionString,
                       Amop.Core.Constants.SQLConstant.StoredProcedureName.BULK_CHANGE_TELEGENCE_DEVICE_MOBILITY_FEATURE_FROM_STAGING,
                       parameters,
                       commandTimeout: Amop.Core.Constants.SQLConstant.ShortTimeoutSeconds);
            });
        }
    }
}
