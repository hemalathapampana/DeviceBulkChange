using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Models;
using Altaworx.ThingSpace.Core;
using Amop.Core.Constants;
using Amop.Core.Models;
using Amop.Core.Models.DeviceBulkChange;
using Amop.Core.Models.Telegence.Api;
using Amop.Core.Repositories;
using Amop.Core.Repositories.Revio;
using Amop.Core.Services.Base64Service;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using Polly;
using static Amop.Core.Models.ThingSpace.ThingSpaceRequest;
using Amop.Core.Enumerations;
using Amop.Core.Models.ThingSpace;
using Amop.Core.Services.Http;
using Amop.Core.Logger;

namespace AltaworxDeviceBulkChange
{
    public partial class Function
    {
        private async Task<bool> ProcessChangeEquipmentAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange)
        {
            LogInfo(context, "SUB", $"ProcessChangeEquipmentAsync({bulkChange.Id})");

            var changes = GetDeviceChanges(context, bulkChange.Id, bulkChange.PortalTypeId, Int32.MaxValue);
            LogInfo(context, "INFO", $"Have {changes.Count} devices need to be updated.)");
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
                context.logger.LogInfo("WARN", $"No unprocessed changes found for status change {bulkChange.Id}");
                return true;
            }

            //Http Retry Policy
            var httpRetryPolicy = GetHttpRetryPolicy(context);

            //Sql Retry Policy
            var sqlRetryPolicy = GetSqlTransientAsyncRetryPolicy(context);
            switch (bulkChange.IntegrationId)
            {
                case (int)IntegrationType.Telegence:
                    result = await ProcessTelegenceChangeEquipmentAsync(context, logRepo, bulkChange, changes,
                        httpRetryPolicy, sqlRetryPolicy);
                    break;
                case (int)IntegrationType.ThingSpace:
                    result = await ProcessThingSpaceChangeIdentifierAsync(context, logRepo, bulkChange, changes, httpRetryPolicy, sqlRetryPolicy);
                    break;
                default:
                    throw new Exception(LogCommonStrings.INTEGRATION_TYPE_IS_UNSUPPORTED);
            }

            return result;
        }

        private async Task<bool> ProcessThingSpaceChangeIdentifierAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository bulkChangeLogRepository,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> deviceChanges, IAsyncPolicy httpRetryPolicy, IAsyncPolicy sqlRetryPolicy)
        {
            LogInfo(context, CommonConstants.SUB, $"({bulkChange.Id})");

            var processedBy = context.Context.FunctionName;
            var thingSpaceAuthentication = ThingSpaceCommon.GetThingspaceAuthenticationInformation(context.CentralDbConnectionString, bulkChange.ServiceProviderId);
            var accessToken = ThingSpaceCommon.GetAccessToken(thingSpaceAuthentication);
            if (accessToken != null)
            {
                var sessionToken = ThingSpaceCommon.GetSessionToken(thingSpaceAuthentication, accessToken);
                if (sessionToken == null)
                {
                    var change = deviceChanges.FirstOrDefault();
                    if (change != null)
                    {
                        string errorMessage = LogCommonStrings.SESSION_TOKEN_REQUEST_FAILED_FOR_THINGSPACE;
                        bulkChangeLogRepository.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(bulkChange.Id, errorMessage, change.Id, processedBy, BulkChangeStatus.ERROR, change.ChangeRequest, true, errorMessage));
                    }
                    return false;
                }
                var httpClientFactory = new SingletonHttpClientFactory();
                var httpRequestFactory = new HttpRequestFactory();
                foreach (var deviceChange in deviceChanges)
                {
                    if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                    {
                        break;
                    }
                    var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<BulkChangeUpdateIdentifier>>(deviceChange.ChangeRequest);
                    var deviceChangeRequest = changeRequest.Request;
                    var thingSpaceChangeIdentifierRequest = BuildThingSpaceChangeIdentifierRequest(changeRequest.Request);
                    var apiResult = await ThingSpaceCommon.PutUpdateIdentifierAsync(thingSpaceAuthentication, thingSpaceChangeIdentifierRequest, accessToken, sessionToken, ThingSpaceChangeIdentifierPath, context.logger, httpClientFactory, httpRequestFactory);

                    if (!apiResult.HasErrors)
                    {
                        bulkChangeLogRepository.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(apiResult, bulkChange.Id, deviceChange.Id, $"{LogCommonStrings.DEVICE_CHANGE_IDENTIFIER}{LogCommonStrings.SUCCESFULLY_CALL_API}"));

                        var thingSpaceCallBackLog = GetThingSpaceCallbackLog(context, apiResult.ResponseObject);

                        if (thingSpaceCallBackLog != null)
                        {
                            if (thingSpaceCallBackLog.APIStatus.ToLower().Equals(CommonConstants.SUCCESS.ToLower(), StringComparison.InvariantCultureIgnoreCase))
                            {
                                var resultUpdateIdentifier = UpdateIdentifierForThingSpace(context, deviceChange, deviceChangeRequest);
                                bulkChangeLogRepository.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(resultUpdateIdentifier, bulkChange.Id, deviceChange.Id, resultUpdateIdentifier.ResponseObject));

                                var resultUpdateCustomerRatePlan = new DeviceChangeResult<string, string>();
                                if (deviceChangeRequest.AddCustomerRatePlan && (!string.IsNullOrWhiteSpace(deviceChangeRequest.CustomerRatePlan) || !string.IsNullOrWhiteSpace(deviceChangeRequest.CustomerRatePool)))
                                {
                                    resultUpdateCustomerRatePlan = await UpdateCustomerRatePlan(context, bulkChangeLogRepository, bulkChange.Id, deviceChange, deviceChangeRequest);
                                    bulkChangeLogRepository.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(resultUpdateCustomerRatePlan, bulkChange.Id, deviceChange.Id, LogCommonStrings.ASSOCIATE_CUSTOMER_UPDATE_CUSTOMER_RATE_PLAN));
                                }

                                if (resultUpdateCustomerRatePlan.HasErrors)
                                {
                                    apiResult.HasErrors = resultUpdateCustomerRatePlan.HasErrors;
                                    apiResult.ResponseObject = resultUpdateCustomerRatePlan.ResponseObject;
                                }

                                await MarkProcessedForM2MDeviceChangeAsync(context, deviceChange.Id, true, $"{LogCommonStrings.DEVICE_CHANGE_IDENTIFIER} {LogCommonStrings.SUCCESSFUL}");
                            }
                            else
                            {
                                await MarkProcessedForM2MDeviceChangeAsync(context, deviceChange.Id, false, thingSpaceCallBackLog.APIResponse);
                            }
                        }
                        else
                        {
                            await EnqueueDeviceBulkChangesAsync(context, bulkChange.Id, DeviceBulkChangeQueueUrl, CommonConstants.DELAY_IN_SECONDS_THREE_MINUTES, 0, isRetryUpdateIdentifier: true, m2mDeviceChangeId: deviceChange.Id, requestId: apiResult.ResponseObject);
                            await MarkProcessedForM2MDeviceChangeAsync(context, deviceChange.Id, true, LogCommonStrings.WAITING_FOR_CARRIER_CHANGE_IDENTIFIER, isDeviceChangeProcessing: true);
                        }
                    }
                    else
                    {
                        var logMessage = string.Format(LogCommonStrings.EMPTY_API_RESPONSE_ERROR, apiResult.ActionText);
                        bulkChangeLogRepository.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(apiResult, bulkChange.Id, deviceChange.Id, logMessage));
                        await MarkProcessedForM2MDeviceChangeAsync(context, deviceChange.Id, false, logMessage);
                    }
                }

                return false;
            }
            else
            {
                var change = deviceChanges.FirstOrDefault();
                if (change != null)
                {
                    string errorMessage = LogCommonStrings.ACCESS_TOKEN_REQUEST_FAILED_FOR_THINGSPACE;
                    bulkChangeLogRepository.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(bulkChange.Id, errorMessage, change.Id, processedBy, BulkChangeStatus.ERROR, change.ChangeRequest, true, errorMessage));
                }
                return false;
            }
        }

        private async Task RetryUpdateIdentifierProcess(KeySysLambdaContext context, long bulkChangeId, SqsValues sqsValues)
        {
            var m2mDeviceChangeId = sqsValues.M2MDeviceChangeId;
            LogInfo(context, CommonConstants.SUB, $"({bulkChangeId}, {m2mDeviceChangeId}, {sqsValues.RetryNumber})");

            var sqlRetryPolicy = GetSqlTransientRetryPolicy(context);
            var logRepo = new DeviceBulkChangeLogRepository(context.CentralDbConnectionString, sqlRetryPolicy);
            var changes = GetDeviceChanges(context, bulkChangeId, 0, Int32.MaxValue, false);
            var m2mDeviceChange = changes.FirstOrDefault(x => x.Id == m2mDeviceChangeId);
            if (m2mDeviceChange != null)
            {
                var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<BulkChangeUpdateIdentifier>>(m2mDeviceChange.ChangeRequest);
                var deviceChangeRequest = changeRequest.Request;
                var thingSpaceCallBackLog = GetThingSpaceCallbackLog(context, sqsValues.RequestId);

                if (thingSpaceCallBackLog != null)
                {
                    if (thingSpaceCallBackLog.APIStatus.ToLower().Equals(CommonConstants.SUCCESS.ToLower(), StringComparison.InvariantCultureIgnoreCase))
                    {
                        var resultUpdateIdentifier = UpdateIdentifierForThingSpace(context, m2mDeviceChange, deviceChangeRequest);
                        logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(resultUpdateIdentifier, bulkChangeId, m2mDeviceChangeId, resultUpdateIdentifier.ResponseObject));
                        var resultUpdateCustomerRatePlan = new DeviceChangeResult<string, string>();
                        if (deviceChangeRequest.AddCustomerRatePlan && (!string.IsNullOrWhiteSpace(deviceChangeRequest.CustomerRatePlan) || !string.IsNullOrWhiteSpace(deviceChangeRequest.CustomerRatePool)))
                        {
                            resultUpdateCustomerRatePlan = await UpdateCustomerRatePlan(context, logRepo, bulkChangeId, m2mDeviceChange, deviceChangeRequest);
                            logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog(resultUpdateCustomerRatePlan, bulkChangeId, m2mDeviceChangeId, LogCommonStrings.ASSOCIATE_CUSTOMER_UPDATE_CUSTOMER_RATE_PLAN));
                        }
                        await MarkProcessedForM2MDeviceChangeAsync(context, m2mDeviceChangeId, true, $"{LogCommonStrings.DEVICE_CHANGE_IDENTIFIER} {LogCommonStrings.SUCCESSFUL}");
                    }
                    else
                    {
                        await MarkProcessedForM2MDeviceChangeAsync(context, m2mDeviceChangeId, false, thingSpaceCallBackLog.APIResponse);
                    }
                }
                else
                {
                    await EnqueueDeviceBulkChangesAsync(context, bulkChangeId, DeviceBulkChangeQueueUrl, CommonConstants.DELAY_IN_SECONDS_THREE_MINUTES, sqsValues.RetryNumber + 1, isRetryUpdateIdentifier: true, m2mDeviceChangeId: m2mDeviceChangeId, requestId: sqsValues.RequestId);
                    await MarkProcessedForM2MDeviceChangeAsync(context, m2mDeviceChangeId, true, LogCommonStrings.WAITING_FOR_CARRIER_CHANGE_IDENTIFIER, isDeviceChangeProcessing: true);
                }
            }
            else
            {
                LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.M2M_DEVICE_CHANGE_NOT_EXIST, m2mDeviceChangeId));
            }
        }

        private ThingSpaceCallBackResponseLog GetThingSpaceCallbackLog(KeySysLambdaContext context, string requestId)
        {
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.REQUEST_ID, requestId),
            };

            var thingSpaceCallBackResponseLog = Amop.Core.Helpers.SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(context),
                            context.CentralDbConnectionString,
                            SQLConstant.StoredProcedureName.GET_THINGSPACE_CALLBACK_RESPONSE_LOG,
                            (dataReader) => ProcessedThingSpaceCallBackFromReader(dataReader),
                            parameters,
                            commandTimeout: SQLConstant.ShortTimeoutSeconds);

            return thingSpaceCallBackResponseLog.FirstOrDefault();
        }

        private DeviceChangeResult<string, string> UpdateIdentifierForThingSpace(KeySysLambdaContext context, BulkChangeDetailRecord deviceChange, BulkChangeUpdateIdentifier changeRequest)
        {
            LogInfo(context, CommonConstants.SUB, "");
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID, deviceChange.ServiceProviderId),
                new SqlParameter(CommonSQLParameterNames.OLD_ICCID, changeRequest.OldICCID),
                new SqlParameter(CommonSQLParameterNames.OLD_IMEI, changeRequest.OldIMEI),
                new SqlParameter(CommonSQLParameterNames.PROCESSED_BY, context.Context.FunctionName)
            };

            if (changeRequest.IdentifierType == (int)IdentifierTypeEnum.ICCID)
            {
                parameters.Add(new SqlParameter(CommonSQLParameterNames.NEW_ICCID, changeRequest.NewICCID));
            }
            else
            {
                parameters.Add(new SqlParameter(CommonSQLParameterNames.NEW_IMEI, changeRequest.NewIMEI));
            }

            var affectedRows = Amop.Core.Helpers.SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult(ParameterizedLog(context),
                            context.CentralDbConnectionString,
                            SQLConstant.StoredProcedureName.UPDATE_IDENTIFIER_FOR_THINGSPACE,
                            parameters,
                            commandTimeout: SQLConstant.ShortTimeoutSeconds);

            var responseMessage = string.Format(LogCommonStrings.ROWS_AFFECTED_WHEN_EXECUTING_STORED_PROCEDURE, "No", SQLConstant.StoredProcedureName.UPDATE_IDENTIFIER_FOR_THINGSPACE);
            if (affectedRows > 0)
            {
                responseMessage = string.Format(LogCommonStrings.SUCCESSULLY_UPDATE, LogCommonStrings.DATABASE);
            }
            return new DeviceChangeResult<string, string>()
            {
                ActionText = SQLConstant.StoredProcedureName.UPDATE_IDENTIFIER_FOR_THINGSPACE,
                HasErrors = false,
                RequestObject = null,
                ResponseObject = responseMessage
            };
        }

        private async Task<DeviceChangeResult<string, string>> UpdateCustomerRatePlan(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo, long bulkChangeId, BulkChangeDetailRecord change, BulkChangeUpdateIdentifier deviceChangeRequest)
        {
            LogInfo(context, CommonConstants.SUB, "");
            var sqlRetryPolicyCustomerRatePlan = GetSqlTransientRetryPolicy(context);
            int? customerRatePlanIdToSubmit = null;
            int customerRatePlanId = 0;
            if (int.TryParse(deviceChangeRequest.CustomerRatePlan, out customerRatePlanId))
            {
                customerRatePlanIdToSubmit = customerRatePlanId;
            }

            int? customerRatePoolIdToSubmit = null;
            int customerRatePoolId = 0;
            if (int.TryParse(deviceChangeRequest.CustomerRatePool, out customerRatePoolId))
            {
                customerRatePoolIdToSubmit = customerRatePoolId;
            }
            var resultUpdateCustomerRatePlan = await ProcessCustomerRatePlanChangeForDevicesAsync(bulkChangeId, customerRatePlanIdToSubmit, null, customerRatePoolIdToSubmit, context.CentralDbConnectionString, context.logger, sqlRetryPolicyCustomerRatePlan);
            return resultUpdateCustomerRatePlan;
        }

        private string BuildThingSpaceChangeIdentifierRequest(BulkChangeUpdateIdentifier request)
        {
            var deviceKind = CommonColumnNames.ICCID.ToLower();
            var changeType = CommonConstants.THINGSPACE_CHANGE_TYPE_ICCID;
            var oldDeviceIdentifier = request.OldICCID;
            var newDeviceIdentifier = request.NewICCID;

            if (request.IdentifierType == IdentifierTypeEnum.IMEI)
            {
                deviceKind = CommonColumnNames.IMEI.ToLower();
                changeType = CommonConstants.THINGSPACE_CHANGE_TYPE_IMEI;
                oldDeviceIdentifier = request.OldIMEI;
                newDeviceIdentifier = request.NewIMEI;
            }
            var thingSpaceChangeIdentifierRequest = new ThingSpaceChangeIdentifierRequest()
            {
                DeviceIds = new List<DeviceId>()
                {
                    new DeviceId()
                    {
                        Id = oldDeviceIdentifier,
                        Kind = deviceKind
                    }
                },
                DeviceIdsTo = new List<DeviceId>()
                {
                    new DeviceId()
                    {
                        Id = newDeviceIdentifier,
                        Kind = deviceKind
                    }
                },
                Change4gOption = changeType
            };

            return JsonConvert.SerializeObject(thingSpaceChangeIdentifierRequest, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        }

        private async Task<bool> ProcessTelegenceChangeEquipmentAsync(KeySysLambdaContext context, DeviceBulkChangeLogRepository logRepo,
            BulkChange bulkChange, ICollection<BulkChangeDetailRecord> changes, IAsyncPolicy httpRetryPolicy, IAsyncPolicy sqlRetryPolicy)
        {
            LogInfo(context, "SUB", $"ProcessTelegenceChangeEquipmentAsync({bulkChange.Id})");

            var telegenceAuthenticationInfo =
                TelegenceCommon.GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, bulkChange.ServiceProviderId);
            if (telegenceAuthenticationInfo != null)
            {
                if (telegenceAuthenticationInfo.WriteIsEnabled)
                {
                    var telegenceAuthentication = telegenceAuthenticationInfo;
                    foreach (var change in changes)
                    {
                        var apiResult = new ApiResponse();
                        if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                        {
                            // processing should continue, we just need to requeue
                            return true;
                        }

                        var changeRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<dynamic>>(change.ChangeRequest);
                        var updateRequest = JsonConvert.DeserializeObject<StatusUpdateRequest<TelegenceUpdateICCIDorIMEIRequest>>(change.ChangeRequest).Request;
                        var apiUpdateResult = await UpdateTelegenceChangeEquipmentAsync(context, logRepo, bulkChange, change,
                            new Base64Service(), telegenceAuthentication, context.IsProduction, updateRequest,
                            change.DeviceIdentifier, TelegenceSubscriberUpdateURL, ProxyUrl);
                        apiResult = apiUpdateResult.ResponseObject;

                        if (apiResult.IsSuccess)
                        {
                            //update iccid and imei
                            var newICCID = updateRequest.ServiceCharacteristic.Where(x => x.Name == "sim").Select(x => x.Value).FirstOrDefault();
                            var newIMEI = updateRequest.ServiceCharacteristic.Where(x => x.Name == "IMEI").Select(x => x.Value).FirstOrDefault();
                            await UpdateEquipmentMobility(context, newICCID, newIMEI, bulkChange.ServiceProviderId, change.DeviceIdentifier);

                            var requestChangeCusRP = changeRequest.RevService;
                            if (requestChangeCusRP != null && requestChangeCusRP.AddCustomerRatePlan)
                            {
                                LogInfo(context, "INFO", $"Add Customer Rate Plan.");
                                int? customerRatePlanIdToSubmit = null;
                                int customerRatePlanId = 0;
                                if (!string.IsNullOrWhiteSpace(requestChangeCusRP.CustomerRatePlan) && !int.TryParse(requestChangeCusRP.CustomerRatePlan, out customerRatePlanId))
                                {
                                    context.logger.LogInfo("WARN", $"Customer Rate Plan Id not valid: {requestChangeCusRP.CustomerRatePlan}");
                                    apiResult.IsSuccess = false;
                                    apiResult.Response += $"Associate Customer Successful. Customer Rate Plan/Pool not set. Customer Rate Plan Id not valid: {requestChangeCusRP.CustomerRatePlan}";
                                }
                                else
                                {
                                    customerRatePlanIdToSubmit = customerRatePlanId;
                                }

                                int? customerRatePoolIdToSubmit = null;
                                int customerRatePoolId = 0;
                                if (bulkChange.PortalTypeId != PortalTypeM2M && !string.IsNullOrWhiteSpace(requestChangeCusRP.CustomerRatePool) && !int.TryParse(requestChangeCusRP.CustomerRatePool, out customerRatePoolId))
                                {
                                    context.logger.LogInfo("WARN", $"Customer Rate Pool Id not valid: {requestChangeCusRP.CustomerRatePool}");
                                    apiResult.IsSuccess = false;
                                    apiResult.Response += $"Associate Customer Successful. Customer Rate Plan/Pool not set. Customer Rate Pool Id not valid: {requestChangeCusRP.CustomerRatePool}";
                                }
                                else
                                {
                                    customerRatePoolIdToSubmit = customerRatePoolId;
                                }

                                if (apiResult.IsSuccess)
                                {
                                    var sqlRetryPolicyCustomerRatePlan = GetSqlTransientRetryPolicy(context);
                                    context.logger.LogInfo("INFO", $"Processing Customer Rate Plan update {requestChangeCusRP.ICCID}");
                                    var ratePlanChangeResult = await ProcessCustomerRatePlanChangeAsync(bulkChange.Id,
                                        customerRatePlanIdToSubmit, requestChangeCusRP.EffectiveDate, null, customerRatePoolIdToSubmit,
                                        context.CentralDbConnectionString, context.logger, sqlRetryPolicyCustomerRatePlan);

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

                                    if (ratePlanChangeResult.HasErrors)
                                    {
                                        apiResult.IsSuccess = false;
                                        apiResult.Response = ratePlanChangeResult.ResponseObject;
                                    }
                                }
                            }
                        }

                        await MarkProcessedForChangeEquipment(context, change.Id, apiResult?.IsSuccess ?? false, apiResult?.Response, PortalTypeMobility);
                    }

                    return true;
                }
                else
                {
                    LogInfo(context, "WARN", "Writes disabled for this service provider.");
                    return false;
                }
            }
            else
            {
                var change = changes.FirstOrDefault();

                string errorMessage = $"Error Sending {bulkChange.Id}: Failed to get Telegence Authentication Information.";
                LogInfo(context, "ERROR", errorMessage);
                logRepo.AddM2MLogEntry(new CreateM2MDeviceBulkChangeLog()
                {
                    BulkChangeId = bulkChange.Id,
                    ErrorText = errorMessage,
                    HasErrors = true,
                    LogEntryDescription = "Telegence Update ICCID or IMEI: Telegence API",
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

        public static async Task<DeviceChangeResult<TelegenceUpdateICCIDorIMEIRequest, ApiResponse>>
            UpdateTelegenceChangeEquipmentAsync(KeySysLambdaContext context,
                DeviceBulkChangeLogRepository logRepo, BulkChange bulkChange, BulkChangeDetailRecord change,
                Base64Service base64Service, TelegenceAuthentication telegenceAuthentication, bool isProduction,
                TelegenceUpdateICCIDorIMEIRequest request, string subscriberNo, string endpoint, string proxyUrl)
        {
            LogInfo(context, "SUB", $"UpdateTelegenceChangeEquipmentAsync({bulkChange.Id})");

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

                        var payload = new Altaworx.AWS.Core.Models.PayloadModel
                        {
                            AuthenticationType = Altaworx.AWS.Core.Helpers.AuthenticationType.TELEGENCEAUTH,
                            Endpoint = subscriberUpdateURL,
                            HeaderContent = headerContentString,
                            JsonContent = jsonContentString,
                            Password = null,
                            Token = null,
                            Url = baseUrl.ToString(),
                            Username = null
                        };

                        var result = client.PatchWithProxy(proxyUrl, payload, context.logger);
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
                            return new DeviceChangeResult<TelegenceUpdateICCIDorIMEIRequest, ApiResponse>()
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
                            context.logger.LogInfo("UpdateTelegenceSubscriber", $"Proxy call to {endpoint} failed.");
                            context.logger.LogInfo("Response Error", responseBody);

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
                            return new DeviceChangeResult<TelegenceUpdateICCIDorIMEIRequest, ApiResponse>()
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

                            return new DeviceChangeResult<TelegenceUpdateICCIDorIMEIRequest, ApiResponse>()
                            {
                                ActionText = $"PATCH {client.BaseAddress}",
                                HasErrors = !response.IsSuccessStatusCode,
                                RequestObject = request,
                                ResponseObject = apiResponse
                            };
                        }
                        catch (Exception e)
                        {
                            context.logger.LogInfo("UpdateTelegenceSubscriber", $"Call to {endpoint} failed.");
                            context.logger.LogInfo("ERROR", e.Message);

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
                            return new DeviceChangeResult<TelegenceUpdateICCIDorIMEIRequest, ApiResponse>()
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
                context.logger.LogInfo("WARN", "Writes disabled for service provider");

                apiResponse = new ApiResponse { IsSuccess = false, Response = "Writes disabled for service provider" };
                return new DeviceChangeResult<TelegenceUpdateICCIDorIMEIRequest, ApiResponse>()
                {
                    ActionText = $"Update Telegence Subscriber: General",
                    HasErrors = true,
                    RequestObject = request,
                    ResponseObject = apiResponse
                };
            }
        }

        private static async Task MarkProcessedForChangeEquipment(KeySysLambdaContext context, long changeId, bool apiResult, string statusDetails, int portalType)
        {
            context.logger.LogInfo("SUB", "MarkProcessedForChangeEquipment()");
            var storedProc = "usp_DeviceBulkChangeUpdateEquipmentMobility";

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = storedProc;

                    cmd.Parameters.AddWithValue("@ChangeId", changeId);
                    cmd.Parameters.AddWithValue("@apiCallResult", apiResult ? 1 : 0);
                    cmd.Parameters.AddWithValue("@statusDetails", statusDetails);
                    cmd.CommandTimeout = 800;
                    conn.Open();

                    await cmd.ExecuteNonQueryAsync();
                }
                conn.Close();
            }
        }

        private static async Task UpdateEquipmentMobility(KeySysLambdaContext context, string iccid, string imei, int serviceProviderId, string msisdn)
        {
            context.logger.LogInfo("SUB", $"UpdateEquipmentMobility({iccid}, {imei}, {serviceProviderId}, {msisdn})");
            var storedProc = "usp_UpdateEquipmentMobility";

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = storedProc;

                    cmd.Parameters.AddWithValue("@iccid", iccid);
                    cmd.Parameters.AddWithValue("@imei", imei);
                    cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                    cmd.Parameters.AddWithValue("@msisdn", msisdn);
                    cmd.CommandTimeout = 1800;
                    conn.Open();

                    await cmd.ExecuteNonQueryAsync();
                }
                conn.Close();
            }
        }
    }
}
