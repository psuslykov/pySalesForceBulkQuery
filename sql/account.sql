Select Id,
   IsDeleted,
   MasterRecordId,
   Name,
   Type,
   RecordTypeId,
   ParentId,
   BillingStreet,
   BillingCity,
   BillingState,
   BillingPostalCode,
   BillingCountry,
   ShippingStreet,
   ShippingCity,
   ShippingState,
   ShippingPostalCode,
   ShippingCountry,
   Phone,
   Fax,
   AccountNumber,
   Website,
   Sic,
   Industry,
   AnnualRevenue,
   NumberOfEmployees,
   Ownership,
   TickerSymbol,
   Description,
   Rating,
   Site,
   OwnerId,
   CreatedDate,
   CreatedById,
   LastModifiedDate,
   LastModifiedById,
   SystemModstamp,
   LastActivityDate,
   Latitude__c,
   Longitude__c,
   PSG_Language__c,
   Submarket__c,
   PSG_Strategic_Account__c,
   Expedia_Hotel_Id__c,
   Market__c,
   External_Account_Id__c,
   QL2_Id__c,
   ETP_Eligible__c,
   Destination_Management_Priority__c,
   Region__c,
   Super_Region__c,
   Number_of_rooms__c,
   Venere_Id__c,
   Brand__c,
   Chain__c,
   Closest_Airport__c,
   IAN_Id__c,
   Market_Manager_Email__c,
   Market_Manager_Name__c,
   Market_Manager_Phone__c,
   Vendor_Id__c,
   ESR_Structure_Type__c,
   PSG_Record_Type__c,
   Status__c,
   Venere_Contract_Status__c,
   Venere_Structure_Type__c,
   HIMS_Stop_Sell__c,
   PSG_Market_Id__c,
   PSG_Region_Id__c,
   PSG_Submarket_Id__c,
   Venere_Status__c,
   Destination_Management__c,
   DS_Admintool_VNID__c,
   Vendor_Code__c,
   Assigned_Star_Rating__c,
   Star_Rating_Review_ID__c,
   Star_Rating_Structure_Type__c,
   Contract_Types__c,
   H_S_Contact__c,
   Request_Venere_ID_Date__c,
   Request_Venere_ID__c,
   PSG_Status__c,
   Contract_Manager_2009__c,
   Contract_Manager_2010__c,
   Contract_Manager_2011__c,
   Contract_Manager_Upside_2011__c,
   Contract_Manager_Upside_2012__c,
   EQC_Addendum_CM__c,
   Stop_Sell_Reason__c,
   Contract_Term__c,
   Docusign_ID__c,
   ESR_Merchant_Venere_ID__c,
   MM_Territory__c,
   Stop_Sell_Start_Date__c,
   Contract_Solicitation_2011__c,
   Contract_Solicitation_2012__c,
   EAN_Id__c,
   Hotwire_Id__c,
   EXPE_Last_day_of_inventory__c,
   PSG_Lead_Account__c,
   Contracted_Name__c,
   Health_Safety_Gas_Status__c,
   Health_Safety_Status__c,
   Expedia_Virtual_Card__c,
   ESR_or_GDS__c,
   ARI_Enabled__c,
   Active_Connections__c,
   Booking_Delivery_Method__c,
   Fallback_Method__c,
   Fax_to_Email_Address__c,
   Wholesaler_Bed_bank_representation__c,
   of_Accounts__c,
   View_on_Expedia__c,
   On_Expedia_Pay__c,
   Venere_Last_Date_Inventory__c,
   ExpediaPay_Last_Used_Date__c,
   ExpediaPay_Status__c,
   Contract_Manager_2003__c,
   Contract_Manager_2004__c,
   Contract_Manager_2005__c,
   Contract_Manager_2006__c,
   Contract_Manager_2007__c,
   Contract_Manager_2008__c,
   Price_Level__c,
   Venere_Commission__c,
   Venere_EEM_Availability_Policy__c,
   Venere_EEM_Hotel_Login__c,
   Venere_EEM_Last_Contract_Signed_Date__c,
   Venere_Hotel_Email__c,
   Venere_Last_Activation_Date__c,
   Venere_Page_Status__c,
   inserted__c,
   Destination_Management_Group__c,
   Fallback_Exception_Hotel__c,
   Fallback_Notes__c,
   View_on_Hotels_com__c,
   ELong_Id__c,
   Direct_Debit_Type__c, 
   Onboarding_Begin__c,
   Onboarding_Complete__c,
   Onboarding_Status__c,
   Hotel_Tier__c,
   Parent_Account_Name__c,
   Synchronous_Booking__c,
   Business_Model_Impact__c,
   Suspended_Reason_Description__c,
   Suspended_Reason__c,
   Suspended_Update_Date__c,
   Contract_Manager_DX__c,
   Contract_Solicitation_2013__c,
   Docusign_Completed_Date__c,
   Sub_Industry__c,
   Venere_Migration_Date__c,
   Booking_Expires__c,
   Contract_Type__c,
   Cost_Price_Expires__c,
   DX_Account_Status__c,
   Evergreen_Items__c,
   Evergreen_Percent__c,
   Insurance_Certificate__c,
   Inventory_Expires__c,
   Items__c,
   Offerings__c,
   Partner_Type__c,
   Restricted_Key_Words__c,
   Seasonal_Offers__c,
   Service_Level__c,
   Who_Will_Solicit_Contract__c,
   Business_Model_Indicator__c,
   Contract_Manager_Upside_2013__c,
   Billing_Contact__c,
   Collection_Language__c,
   Collections_Agent__c,
   Last_Collection_Call__c,
   Last_Payment_Amount__c,
   Lodging_Ops_Email_Address__c,
   Lodging_Ops_Phone_Number__c,
   Payment_Method__c,
   Temporary_Collections_Agent__c,
   Collection_15_day_Bucket__c,
   Collection_30_day_Bucket__c,
   Collection_60_day_Bucket__c,
   Collection_Current_Bucket__c,
   Last_Payment_Date__c,
   Total_Amount_Invoiced__c,
   Total_Amount_Owed__c,
   Total_Amount_Past_Due__c,
   Collections_Dunning_Active_Subject__c,
   Collections_Dunning_Suspended_Subject__c,
   Collections_PTP_Confirmation_Subject__c,
   Oracle_Id__c,
   Collections_Reactivation_Subject__c,
   Collections_Reminder_Subject__c,
   Collections_Suspension_Notification__c,
   Collections_Suspension_Warning__c,
   Partner_Pay__c,
   Data_Source__c,
   Contract_Solicitation_2014__c,
   Venere_Migration_Start__c,
   Venere_Migration_Complete__c,
   Venere_RFS_On__c,
   Venere_Touch_Property__c
From Account
Where SystemModStamp >= [START_DATETIME] and SystemModStamp < [END_DATETIME]