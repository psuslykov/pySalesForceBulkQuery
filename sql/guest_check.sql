Select Id,
   IsDeleted,
   SystemModstamp
From Guest__c
Where SystemModStamp >= 2001-01-01T00:00:00.000Z and SystemModStamp < 2015-06-02T00:00:00.000Z
