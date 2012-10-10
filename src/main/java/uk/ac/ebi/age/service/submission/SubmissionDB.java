package uk.ac.ebi.age.service.submission;

import java.io.File;
import java.util.List;

import uk.ac.ebi.age.ext.submission.HistoryEntry;
import uk.ac.ebi.age.ext.submission.SubmissionDBException;
import uk.ac.ebi.age.ext.submission.SubmissionMeta;
import uk.ac.ebi.age.ext.submission.SubmissionQuery;
import uk.ac.ebi.age.ext.submission.SubmissionReport;
import uk.ac.ebi.age.transaction.TransactionalDB;
import uk.ac.ebi.mg.rwarbiter.TokenFactory;

public abstract class SubmissionDB implements TransactionalDB 
{
 private static SubmissionDB instance;
 
 
 public static SubmissionDB getInstance()
 {
  return instance;
 }

 public static void setInstance( SubmissionDB db )
 {
  instance=db;
 }

 public abstract void init();

 public abstract void storeSubmission( SDBTransaction t, SubmissionMeta sMeta, SubmissionMeta origSbm, String updateDescr) throws SubmissionDBException;

 public abstract void shutdown();

 public abstract SubmissionReport getSubmissions(SDBReadLock rl, SubmissionQuery q) throws SubmissionDBException;

 public abstract SubmissionMeta getSubmission(SDBReadLock rl,String id) throws SubmissionDBException;

 public abstract boolean hasSubmission(SDBReadLock rl,String id) throws SubmissionDBException;

 public abstract void storeAttachment( SDBTransaction t, String submId, String fileId, long modificationTime, File aux)  throws SubmissionDBException;

 public abstract File getAttachment(SDBReadLock rl,String clustId, String fileId, long ver);

 public abstract File getDocument(SDBReadLock rl,String clustId, String fileId, long ts);

 public abstract List<HistoryEntry> getHistory(SDBReadLock rl,String sbmId) throws SubmissionDBException;

 public abstract boolean removeSubmission(SDBTransaction t, String sbmID) throws SubmissionDBException;

 public abstract boolean  restoreSubmission(SDBTransaction t, String id) throws SubmissionDBException;

 public abstract boolean tranklucateSubmission(SDBTransaction t, String sbmID) throws SubmissionDBException;

 protected static class TokFactory implements TokenFactory<SDBReadLock,SDBTransaction,SDBUpgReadLock>
 {
  public TokFactory()
  {
  }

  @Override
  public SDBReadLock createReadToken()
  {
   return new SDBReadLock();
  }

  @Override
  public SDBTransaction createWriteToken()
  {
   return new SDBTransaction();
  }

  @Override
  public SDBUpgReadLock createUpgradableReadToken()
  {
   return new SDBUpgReadLock();
  }
  
 }

}
