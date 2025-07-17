# Database Table and Field Mapping Analysis

## Overview
This document provides a comprehensive analysis of database tables and field mappings identified in the Altaworx Device Bulk Change repository, with particular focus on the sources of IMEI and ICCID data.

## IMEI and ICCID Source Analysis

### Primary Sources:

#### 1. ServiceCharacteristic Collections (API Response Data)
- **ICCID**: Retrieved from `ServiceCharacteristic` where `Name == "sim"`
- **IMEI**: Retrieved from `ServiceCharacteristic` where `Name == "IMEI"`
- **Source Files**: 
  - `ProcessNewServiceActivationStatus.cs` (lines 931-932)
  - `ProcessChangeICCIDorIMEI.cs` (lines 362-363)
  - `AltaworxDeviceBulkChange.cs` (lines 3144, 3169)

#### 2. Direct Table Fields
- **ICCID**: Used as primary device identifier in multiple tables
- **IMEI**: Stored alongside ICCID in device-related tables

## Database Tables Identified

### Core Device Tables

#### 1. **Device** Table
- **Fields**: ServiceProviderId, ICCID, IMEI, DeviceStatusId, Status, CarrierRatePlanId, RatePlan, ProviderDateAdded, CreatedBy, CreatedDate, IsActive, IsDeleted, CommunicationPlan
- **Primary Key**: Not explicitly shown, likely ICCID or composite key
- **IMEI/ICCID Usage**: Both stored as core fields
- **Source**: `AltaworxDeviceBulkChange.cs` line 4771

#### 2. **ThingSpaceDevice** Table
- **Fields**: ICCID, Status, RatePlan, CreatedBy, CreatedDate, IsActive, IsDeleted, DeviceStatusId, ServiceProviderId
- **Primary Key**: Likely ICCID
- **IMEI/ICCID Usage**: ICCID as primary identifier
- **Source**: `AltaworxDeviceBulkChange.cs` line 4890

#### 3. **ThingSpaceDeviceUsage** Table
- **Fields**: ICCID, IMEI, Status, RatePlan, CreatedBy, CreatedDate, IsActive, IsDeleted, DeviceStatusId
- **Primary Key**: Likely ICCID or composite key
- **IMEI/ICCID Usage**: Both fields present for usage tracking
- **Source**: `AltaworxDeviceBulkChange.cs` line 4813

#### 4. **ThingSpaceDeviceDetail** Table
- **Fields**: ICCID, CreatedBy, CreatedDate, IsActive, IsDeleted, AccountNumber, ThingSpaceDateAdded
- **Primary Key**: Likely ICCID
- **IMEI/ICCID Usage**: ICCID as primary identifier
- **Source**: `AltaworxDeviceBulkChange.cs` line 4853

### Carrier-Specific Tables

#### 5. **JasperCarrierRatePlan** Table
- **Fields**: id, IsDeleted, ServiceProviderId, RatePlanCode
- **Primary Key**: id
- **IMEI/ICCID Usage**: Referenced indirectly through rate plan relationships
- **Source**: `AltaworxDeviceBulkChange.cs` line 4638

### Telegence Platform Tables

#### 6. **TelegenceDevice** Table
- **Fields**: Id, SubscriberNumber, IsActive, IsDeleted, BillingAccountNumber, FoundationAccountNumber
- **Primary Key**: Id
- **IMEI/ICCID Usage**: Related through SubscriberNumber (MSISDN)
- **Source**: `ProcessNewServiceActivationStatus.cs` lines 607, 881

#### 7. **TelegenceNewServiceActivation_Staging** Table
- **Purpose**: Staging table for new service activations
- **Fields**: ServiceProviderId (confirmed), other fields inferred from usage
- **IMEI/ICCID Usage**: Part of staging process for device activation
- **Source**: `ProcessNewServiceActivationStatus.cs` line 997

#### 8. **TelegenceDeviceMobilityFeature_Staging** Table
- **Purpose**: Staging table for device mobility features
- **IMEI/ICCID Usage**: Related to device feature management
- **Source**: `ProcessNewServiceActivationStatus.cs` line 1020

### Change Management Tables

#### 9. **Mobility_DeviceChange** Table
- **Fields**: Id, BulkChangeId, IsProcessed, IsActive, IsDeleted, StatusDetails, ICCID (inferred), TenantId, ServiceProviderId
- **Primary Key**: Id
- **IMEI/ICCID Usage**: ICCID used for device identification in change tracking
- **Source**: `ProcessNewServiceActivationStatus.cs` lines 826, 847

### Callback and Logging Tables

#### 10. **ThingSpaceCallBackResponseLog** Table
- **Fields**: RequestId, APIStatus, APIResponse, TenantId, ServiceProviderId
- **Primary Key**: Likely RequestId + TenantId + ServiceProviderId
- **IMEI/ICCID Usage**: Indirect through API responses
- **Source**: `AltaworxDeviceBulkChange.cs` line 6378

## Stored Procedures Identified

### Device Management Procedures
1. **usp_DeviceBulkChange_AddToDeviceInventory**
   - **Purpose**: Add devices to inventory
   - **IMEI/ICCID Usage**: Processes device records with identifiers

2. **usp_UpdateEquipmentMobility**
   - **Parameters**: @iccid, @imei, @serviceProviderId, @msisdn
   - **Purpose**: Update equipment mobility records
   - **IMEI/ICCID Usage**: Direct parameters for updating device information
   - **Source**: `ProcessChangeICCIDorIMEI.cs` line 695

3. **usp_DeviceBulkChangeUpdateEquipmentMobility**
   - **Purpose**: Bulk update equipment mobility changes
   - **IMEI/ICCID Usage**: Related to equipment change processing
   - **Source**: `ProcessChangeICCIDorIMEI.cs` line 671

### Service Management Procedures
4. **usp_RevService_Create_Service**
   - **Purpose**: Create Rev services
   - **IMEI/ICCID Usage**: Part of service creation workflow

5. **usp_RevService_Create_ServiceProduct**
   - **Purpose**: Create service products
   - **IMEI/ICCID Usage**: Related to device service products

### Customer and Rate Plan Procedures
6. **usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDevices**
   - **Purpose**: Update devices for customer rate plan changes
   - **IMEI/ICCID Usage**: Updates devices based on rate plan changes

7. **usp_DeviceBulkChange_CustomerRatePlanChange_UpdateDeviceByNumber**
   - **Purpose**: Update device by number for rate plan changes
   - **IMEI/ICCID Usage**: Updates specific device records

### IP Provisioning Procedures
8. **usp_Update_MobilityDevice_IPAddress**
   - **Purpose**: Update IP addresses for mobility devices
   - **IMEI/ICCID Usage**: Related to device IP provisioning

9. **usp_DeviceBulkChange_AdditionalStep_UpdateMobilityChangeByMSISDN**
   - **Purpose**: Update mobility changes by MSISDN
   - **IMEI/ICCID Usage**: Updates through MSISDN relationship

## Field Mapping Sources

### From API Responses (ServiceCharacteristic)
- **ICCID**: `response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "sim")?.Value`
- **IMEI**: `response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "IMEI")?.Value`
- **SingleUserCode**: `response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "singleUserCode")?.Value`
- **ServiceZipCode**: `response.Service.ServiceCharacteristic.FirstOrDefault(sc => sc.Name == "serviceZipCode")?.Value`

### From Direct API Response Properties
- **BillingAccountNumber**: `response.BillingAccount.Id`
- **SubscriberNumber**: `response.Service.SubscriberNumber`
- **SubscriberNumberStatus**: `response.Service.Status`

### From Database Lookups
- **FoundationAccountNumber**: Retrieved from TelegenceDevice table
- **CarrierRatePlan**: Retrieved through various rate plan tables
- **DeviceStatus**: Retrieved through DeviceStatusId references

## Key Relationships

1. **ICCID-IMEI Relationship**: Always processed together in device operations
2. **ServiceCharacteristic Mapping**: Primary source for device identifiers from API responses
3. **Cross-Platform Synchronization**: ICCID/IMEI synchronized across ThingSpace, Jasper, and Telegence platforms
4. **Change Tracking**: All device changes tracked through Mobility_DeviceChange table
5. **Staging Process**: New activations processed through staging tables before final commit

## Data Flow Summary

1. **Inbound**: API responses provide ICCID/IMEI through ServiceCharacteristic collections
2. **Processing**: Device identifiers processed through platform-specific services
3. **Storage**: Multiple device tables maintain ICCID/IMEI relationships
4. **Change Management**: All modifications tracked through change management tables
5. **Cross-Platform**: Data synchronized across multiple carrier platforms (ThingSpace, Jasper, Telegence)