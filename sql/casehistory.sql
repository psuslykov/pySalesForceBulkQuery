Select Id,
   IsDeleted,
   CaseId,
   CreatedById,
   CreatedDate,
   Field,
   OldValue,
   NewValue
From CaseHistory
Where CreatedDate >= [START_DATETIME] and CreatedDate < [END_DATETIME]