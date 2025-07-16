using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Models;
using AltaworxDeviceBulkChange.Constants;
using AltaworxDeviceBulkChange.Models;
using Amazon.Lambda.SQSEvents;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Models.Telegence;
using Amop.Core.Repositories;
using Amop.Core.Services.Telegence;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace AltaworxDeviceBulkChange
{
    public partial class Function
    {

        private static int MAX_CHECK_IP_CHANGES_PER_LAMBDA_INSTANCE = 200;
        private static int MAX_TELEGENCE_CHECK_IP_RETRY = 3;
        private static int DELAY_TELEGENCE_CHECK_IP_PROVISION = 300;

        private async Task<bool> ProcessTelegenceCheckIPProvision(KeySysLambdaContext context, BulkChange bulkChange, SQSEvent.SQSMessage message)
        {
            LogInfo(context, "SUB", $"ProcessTelegenceStaticIPProvisioning(..,{bulkChange.Id},{bulkChange.ServiceProviderId},...)");
            var sqlRetryPolicy = GetSqlTransientRetryPolicy(context);
            var logRepo = new DeviceBulkChangeLogRepository(context.CentralDbConnectionString, sqlRetryPolicy);

            if (string.IsNullOrEmpty(TelegenceIPProvisioningURL))
            {
                LoadTelegenceStaticIPProvisioningEnv(context);
            }

            int batchSize = MAX_CHECK_IP_CHANGES_PER_LAMBDA_INSTANCE;

            //authentication info
            var telegenceApiAuthentication =
                GetTelegenceApiAuthentication(context.CentralDbConnectionString, bulkChange.ServiceProviderId);

            //get a batch of changes that have not processed ip provisioning step but finished the activation step
            var changes = GetBatchedTelegenceIPProvisionChanges(context, bulkChange.Id, batchSize,
                additionalStatusToFilterBy: new List<string> { TelegenceAdditionalStepStatus.Requested },
                onlyUnprocessed: false, statusText: ChangeStatus.PENDING);

            changes = changes.Where(change => !string.IsNullOrEmpty(change.DeviceIdentifier)
                                    && !string.IsNullOrEmpty(change.AdditionalStepDetails)
                                    && change.IPProvisionDetails?.RetryCount < MAX_TELEGENCE_CHECK_IP_RETRY).ToList();


            if (changes == null || changes.Count == 0)
            {
                // empty list to process meaning done
                // continue with the status check step
                LogInfo(context, "INFO", $"All changes are processed for {bulkChange.Id}");
                return false;
            }

            if (telegenceApiAuthentication == null)
            {
                string errorMessage = $"Unable to get Telegence API Authentication for Service Provider: {bulkChange.ServiceProviderId}";

                LogInfo(context, "EXCEPTION", errorMessage);
                LogTelegenceIPProvisionChange(logRepo, bulkChange, changes, "Check Provisioning Status: Telegence API Credentials", BulkChangeStatus.ERROR, true, errorMessage, $"ServiceProviderId: {bulkChange.ServiceProviderId}", errorMessage);
                // mark process error & second step error
                await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMessage, true, TelegenceAdditionalStepStatus.Error);
                return false;
            }

            if (_telegenceApiGetClient == null)
                _telegenceApiGetClient = new TelegenceAPIClient(telegenceApiAuthentication,
                    context.IsProduction, $"{ProxyUrl}/api/Proxy/Get", context.logger);

            //batch changes by the batch id, always <= 10 for each group
            var batchedChanges = changes.GroupBy(change => change.IPProvisionDetails.BatchId);
            try
            {
                foreach (var batch in batchedChanges)
                {
                    //process by batch of at most 10, all with same batch id
                    var errorMessage = await ProcessBatchedTelegenceCheckIPProvision(context, bulkChange, batch.ToList(), logRepo, telegenceApiAuthentication, _telegenceApiGetClient, batch.Key);

                    //exception happened in middle of batch, marked error to all remaining and stop process
                    if (!string.IsNullOrEmpty(errorMessage))
                    {
                        LogInfo(context, "INFO", "Error when checking IP provision status. Stopping process...");
                        await MarkAllProcessedMobilityAdditionalStepAsync(context, bulkChange.Id, false, errorMessage, true, TelegenceAdditionalStepStatus.Error, JsonConvert.SerializeObject(new TelegenceIPProvisionStepDetails() { Error = errorMessage }));
                        return false;
                    }

                    //finished a batch and now check remaining time
                    if (context.Context.RemainingTime.TotalSeconds < RemainingTimeCutoff)
                    {
                        LogInfo(context, "SUB", $"Requeue to continue process check IP provision status.");
                        // processing should continue, we just need to requeue
                        await EnqueueBatchedTelegenceIPProvisioningAsync(context, bulkChange.Id, DeviceBulkChangeQueueUrl, DELAY_TELEGENCE_CHECK_IP_PROVISION, TelegenceNewActivationStep.CheckIPProvisionStatus);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                // unexpected exception => stop immediately
                LogInfo(context, "EXCEPTION", $"Error when checking IP provision status. {ex.Message}. Stack trace: {ex.StackTrace}");
                return false;
            }

            //finished the instance and continue with next set of changes
            LogInfo(context, "INFO", $"Processed {batchSize} changes. Requeue to continue process next {batchSize} changes.");
            await EnqueueBatchedTelegenceIPProvisioningAsync(context, bulkChange.Id, DeviceBulkChangeQueueUrl, DELAY_TELEGENCE_CHECK_IP_PROVISION, TelegenceNewActivationStep.CheckIPProvisionStatus);

            //always return false so that this won't trigger the initial step retry logic 
            return false;
        }

        public async Task<string> ProcessBatchedTelegenceCheckIPProvision(KeySysLambdaContext context, BulkChange bulkChange, List<TelegenceBulkChangeDetailRecord> changes, DeviceBulkChangeLogRepository logRepo, TelegenceAPIAuthentication auth, TelegenceAPIClient telegenceApiClient, string batchId)
        {
            LogInfo(context, CommonConstants.SUB, $"(...,changes.Count: {changes.Count},...,batchId: {batchId})");
            try
            {
                using (var httpClient = new HttpClient(new LambdaLoggingHandler()))
                {
                    var errorMsg = string.Empty;
                    var ipProvisionStatusResponse = await GetIPProvisionStatusResponseAsync(context, batchId, telegenceApiClient, httpClient);


                    // Save the batchId and related info into database
                    var additionalStepDetails = new TelegenceIPProvisionStepDetails()
                    {
                        BatchId = batchId,
                        // Set as max retry if common check fail
                        RetryCount = CommonConstants.MAX_TELEGENCE_CHECK_IP_RETRY
                    };

                    if (ipProvisionStatusResponse == null || ipProvisionStatusResponse.HasErrors || ipProvisionStatusResponse.ResponseObject == null)
                    {
                        //could not get response
                        errorMsg = "Error when checking status of IP provision request.";
                        additionalStepDetails.Error = errorMsg;
                        var responseText = (ipProvisionStatusResponse != null && ipProvisionStatusResponse.ResponseObject != null) ?
                                            JsonConvert.SerializeObject(ipProvisionStatusResponse.ResponseObject) : errorMsg;
                        await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, false, errorMsg,
                            TelegenceAdditionalStepStatus.APIError, JsonConvert.SerializeObject(additionalStepDetails),
                            changes.Select(change => change.DeviceIdentifier).ToList(), ChangeStatus.API_FAILED, ChangeStatus.PENDING);
                        LogTelegenceIPProvisionChange(logRepo, bulkChange, changes,
                            "Static IP Provisioning: Check IP Provision Status",
                            BulkChangeStatus.ERROR, true, errorMsg,
                            JsonConvert.SerializeObject(additionalStepDetails),
                            responseText);
                        return errorMsg;
                    }

                    var responseStatuses = ipProvisionStatusResponse.ResponseObject.TelegenceIPProvisionStatusResponses;

                    // Keep a list to later mark as error
                    var changesWithMaxRetries = new List<TelegenceBulkChangeDetailRecord>();

                    // Process the individual device provision statuses
                    var table = new DataTable();
                    table.Columns.Add(CommonColumnNames.MSISDN);
                    table.Columns.Add(CommonColumnNames.IPAddressTableColumn);

                    foreach (var change in changes)
                    {
                        var ipProvisionInfo = responseStatuses.FirstOrDefault(res => res.SubscriberNumber == change.DeviceIdentifier);
                        var newAdditionalStepDetails = new TelegenceIPProvisionStepDetails()
                        {
                            BatchId = batchId,
                            RetryCount = change.IPProvisionDetails.RetryCount
                        };

                        // Found matching IP Provisioning request
                        if (ipProvisionInfo?.BatchId != null)
                        {
                            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.CURRENT_TELEGENCE_SERVICE_ACTIVATION_STATUS, batchId, ipProvisionInfo.ProfileCheck, ipProvisionInfo.IPReservation, ipProvisionInfo.ProfileUpdate));

                            newAdditionalStepDetails.RetryCount = change.IPProvisionDetails.RetryCount + 1;
                            // IP Provisioning only complete if all three statuses are success (based on Telegence API documentation) 
                            if (ipProvisionInfo.ProfileCheck.Contains(CommonConstants.TELEGENCE_IP_PROVISIONING_SUCCESS)
                                && ipProvisionInfo.IPReservation.Contains(CommonConstants.TELEGENCE_IP_PROVISIONING_RESERVED)
                                && ipProvisionInfo.ProfileUpdate.Contains(CommonConstants.TELEGENCE_IP_PROVISIONING_COMPLETE))
                            {
                                // If already success, mark processed additional step 
                                var ipAddress = ipProvisionInfo.IPAddressRecord.IPAddress;

                                if (!string.IsNullOrWhiteSpace(ipAddress))
                                {
                                    var dr = AddToDataRow(table, change.DeviceIdentifier, ipAddress);
                                    table.Rows.Add(dr);
                                }

                                await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, true, ipAddress,
                                    TelegenceAdditionalStepStatus.Processed, JsonConvert.SerializeObject(newAdditionalStepDetails),
                                    new List<string> { change.DeviceIdentifier }, ChangeStatus.PROCESSED, null);
                                LogTelegenceIPProvisionChange(logRepo, bulkChange, new List<TelegenceBulkChangeDetailRecord> { change },
                                    string.Format(LogCommonStrings.TELEGENCE_STATIC_IP_PROVISIONING_RETRY, DELAY_TELEGENCE_CHECK_IP_PROVISION),
                                    BulkChangeStatus.PROCESSED, false, errorMsg,
                                    JsonConvert.SerializeObject(newAdditionalStepDetails),
                                    JsonConvert.SerializeObject(ipProvisionInfo));
                            }
                            else
                            {
                                if (newAdditionalStepDetails.RetryCount <= CommonConstants.MAX_TELEGENCE_CHECK_IP_RETRY)
                                {
                                    // if fail to get status after the delay, mark as requested, skip and continue submitting next set of sims
                                    // only increase the api attempt count and will check this SIM again in next step
                                    await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, true, string.Empty, TelegenceAdditionalStepStatus.Requested, JsonConvert.SerializeObject(newAdditionalStepDetails), new List<string> { change.DeviceIdentifier }, ChangeStatus.PENDING, null);
                                    LogTelegenceIPProvisionChange(logRepo, bulkChange, new List<TelegenceBulkChangeDetailRecord> { change },
                                        string.Format(LogCommonStrings.TELEGENCE_STATIC_IP_PROVISIONING_RETRY, DELAY_TELEGENCE_CHECK_IP_PROVISION),
                                        BulkChangeStatus.PROCESSED, false, errorMsg,
                                        JsonConvert.SerializeObject(newAdditionalStepDetails),
                                        JsonConvert.SerializeObject(ipProvisionInfo));
                                }
                                else
                                {
                                    changesWithMaxRetries.Add(change);
                                }
                            }
                        }
                        else
                        {
                            // Invalid batchId or the SubscriberNumber not in this batch
                            errorMsg = string.Format(LogCommonStrings.ERROR_WHEN_CHECKING_IP_PROVISIONING_STATUS, ipProvisionInfo?.Message);
                            additionalStepDetails.Error = errorMsg;

                            LogInfo(context, CommonConstants.ERROR, errorMsg);
                            await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, false, string.Empty, TelegenceAdditionalStepStatus.APIError, JsonConvert.SerializeObject(additionalStepDetails), new List<string> { change.DeviceIdentifier }, ChangeStatus.API_FAILED, ChangeStatus.PENDING);
                            LogTelegenceIPProvisionChange(logRepo, bulkChange, new List<TelegenceBulkChangeDetailRecord> { change },
                                LogCommonStrings.TELEGENCE_STATIC_IP_PROVISIONING_CHECK_STATUS,
                                BulkChangeStatus.ERROR, true, errorMsg,
                                JsonConvert.SerializeObject(additionalStepDetails),
                                JsonConvert.SerializeObject(ipProvisionStatusResponse));
                        }
                    }

                    if (table.Rows.Count > 0)
                    {
                        BulkUpdateMobilityDeviceIpAddress(context, table);
                    }

                    if (changesWithMaxRetries.Count > 0)
                    {
                        await MarkedErrorTelegenceCheckIPIfMaxRetry(context, bulkChange, changesWithMaxRetries, logRepo, auth, telegenceApiClient, batchId);
                    }
                }
            }
            catch (Exception ex)
            {
                //fail to run specific batch of 10
                var errorMsg = $"Error: {ex.Message} - {ex.StackTrace}";
                LogInfo(context, "ERROR", errorMsg);
                return errorMsg;
            }
            //almost always return false since error messages are not in the document
            return string.Empty;
        }

        private static List<TelegenceBulkChangeDetailRecord> GetBatchedTelegenceIPProvisionChanges(KeySysLambdaContext context, long bulkChangeId,
            int batchSize = 100, List<string> additionalStatusToFilterBy = null, bool onlyUnprocessed = true, string statusText = ChangeStatus.PROCESSED)
        {
            context.logger.LogInfo("SUB", $"GetBatchedTelegenceIPProvisionChanges({nameof(context)},{bulkChangeId},{batchSize}");
            string procName = "usp_DeviceBulkChange_GetPagedProcessedMobilityChanges";

            List<TelegenceBulkChangeDetailRecord> records = new List<TelegenceBulkChangeDetailRecord>();
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = procName;
                    cmd.Parameters.AddWithValue("@bulkChangeId", bulkChangeId);
                    cmd.Parameters.AddWithValue("@batchSize", batchSize);
                    cmd.Parameters.AddWithValue("@statusText", statusText);
                    cmd.Parameters.AddWithValue("@additionalStepFilterStatus", additionalStatusToFilterBy != null ? string.Join(',', additionalStatusToFilterBy) : (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@onlyUnprocessed", onlyUnprocessed);
                    conn.Open();

                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var deviceRecord = ProcessedTelegenceCheckIPDeviceRecordFromReader(rdr);
                        records.Add(deviceRecord);
                    }
                }
            }

            return records;
        }

        private static TelegenceBulkChangeDetailRecord ProcessedTelegenceCheckIPDeviceRecordFromReader(IDataRecord rdr)
        {
            return new TelegenceBulkChangeDetailRecord
            {
                Id = long.TryParse(rdr["Id"].ToString(), out var detailId) ? detailId : default,
                MSISDN = rdr["MSISDN"] != DBNull.Value ? rdr["MSISDN"].ToString() : string.Empty,
                DeviceIdentifier = rdr["DeviceIdentifier"] != DBNull.Value ? rdr["DeviceIdentifier"].ToString() : string.Empty,
                BulkChangeId = long.TryParse(rdr["BulkChangeId"].ToString(), out var bulkChangeId) ? bulkChangeId : default,
                Status = rdr["Status"].ToString(),
                AdditionalStepStatus = rdr["AdditionalStepStatus"] != DBNull.Value ? rdr["AdditionalStepStatus"].ToString() : null,
                AdditionalStepDetails = rdr["AdditionalStepDetails"] != DBNull.Value ? rdr["AdditionalStepDetails"].ToString() : null,
                ServiceProviderId = int.TryParse(rdr["ServiceProviderId"].ToString(), out var serviceProviderId)
                    ? serviceProviderId
                    : default,
                IntegrationId = int.TryParse(rdr["IntegrationId"].ToString(), out var integrationId) ? integrationId : default,
                TenantId = int.TryParse(rdr["TenantId"].ToString(), out var tenantId) ? tenantId : default,
                ChangeRequestTypeId = int.TryParse(rdr["ChangeRequestTypeId"].ToString(), out var changeRequestTypeId)
                    ? changeRequestTypeId
                    : default,
                ChangeRequestType = rdr["ChangeRequestType"] != DBNull.Value ? rdr["ChangeRequestType"].ToString() : null,
                ChangeRequest = rdr["ChangeRequest"] != DBNull.Value ? rdr["ChangeRequest"].ToString() : null,
                ICCID = rdr["ICCID"] != DBNull.Value ? rdr["ICCID"].ToString() : null,
                DeviceId = rdr["DeviceId"] != DBNull.Value ? int.Parse(rdr["DeviceId"].ToString()) : default,
                IPProvisionDetails = rdr["AdditionalStepDetails"] != DBNull.Value ?
                            JsonConvert.DeserializeObject<TelegenceIPProvisionStepDetails>(rdr["AdditionalStepDetails"].ToString()) : null
            };
        }

        public virtual async Task MarkedErrorTelegenceCheckIPIfMaxRetry(KeySysLambdaContext context, BulkChange bulkChange, List<TelegenceBulkChangeDetailRecord> changes, DeviceBulkChangeLogRepository logRepo, TelegenceAPIAuthentication auth, TelegenceAPIClient telegenceApiClient, string batchId)
        {
            LogInfo(context, "SUB", $"MarkedErrorTelegenceCheckIPIfMaxRetry(...,changes.Count: {changes.Count},...,batchId: {batchId})");
            var errorMsg = "Reach max Retries for checking IP Provisioning status.";
            var additionalStepDetails = new TelegenceIPProvisionStepDetails()
            {
                BatchId = batchId,
                RetryCount = MAX_TELEGENCE_CHECK_IP_RETRY,
                Error = errorMsg
            };
            await MarkProcessedMobilityAdditionalStepByMSISDNAsync(context, bulkChange.Id, false, string.Empty,
                TelegenceAdditionalStepStatus.APIError, JsonConvert.SerializeObject(additionalStepDetails),
                changes.Select(change => change.DeviceIdentifier).ToList(), ChangeStatus.API_FAILED, ChangeStatus.PENDING);
            LogTelegenceIPProvisionChange(logRepo, bulkChange, changes, "Static IP Provisioning: Check IP Provision Status",
                BulkChangeStatus.ERROR, true, errorMsg, JsonConvert.SerializeObject(additionalStepDetails), errorMsg);

        }
    }
}
