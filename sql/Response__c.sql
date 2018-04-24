select 
   id,
   ownerid,
   isdeleted,
   name,
   createddate,
   systemmodstamp,
   recordtypeid,
   account__c,
   case__c,
   contact__c,
   filing_user__c,
   guest__c,
   status__c,
   was_your_issue_resolved__c,
   case_wellness_review_link__c,
   surveygizmo_reference__c,
   surveygizmo_return_link__c,
   case_language__c,
   case_subject__c
from Response__c