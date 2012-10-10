package uk.ac.ebi.age.transaction;

public interface TransactionalDB
{
 ReadLock getReadLock();
 UpgradableReadLock getUpgradableReadLock();

 void releaseLock( ReadLock l );
 
 Transaction startTransaction();
 Transaction startTransaction( UpgradableReadLock rl ) throws TransactionException;

 void commitTransaction( Transaction t ) throws TransactionException;
 void prepareTransaction( Transaction t ) throws TransactionException;
 void rollbackTransaction( Transaction t ) throws TransactionException;
}
