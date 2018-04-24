Select Id,
   SystemModstamp,
   Blocker__c,
   Closed_Inventory_Type__c,
   isCloned__c,
   Did_You_Seek_MM_Approval__c,
   MM_Response__c
From Case Where SystemModStamp >= [START_DATETIME] and SystemModStamp < [END_DATETIME]