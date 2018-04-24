Select Id,
   ProcessedbyMachineLearning__c,
   High_Impact_Event__c
From Case
Where SystemModStamp >= 2017-01-27T01:00:00.000Z and SystemModStamp < 2017-03-02T00:00:00.000Z