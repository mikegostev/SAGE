package uk.ac.ebi.age.service.submission;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.transaction.file.FileResourceManager;
import org.apache.commons.transaction.file.ResourceManagerException;

import uk.ac.ebi.age.ext.submission.AttachmentDiff;
import uk.ac.ebi.age.ext.submission.DataModuleDiff;
import uk.ac.ebi.age.ext.submission.DataModuleMeta;
import uk.ac.ebi.age.ext.submission.Factory;
import uk.ac.ebi.age.ext.submission.FileAttachmentMeta;
import uk.ac.ebi.age.ext.submission.HistoryEntry;
import uk.ac.ebi.age.ext.submission.Status;
import uk.ac.ebi.age.ext.submission.SubmissionDBException;
import uk.ac.ebi.age.ext.submission.SubmissionDiff;
import uk.ac.ebi.age.ext.submission.SubmissionMeta;
import uk.ac.ebi.age.ext.submission.SubmissionQuery;
import uk.ac.ebi.age.ext.submission.SubmissionQuery.Selector;
import uk.ac.ebi.age.ext.submission.SubmissionReport;
import uk.ac.ebi.age.transaction.InconsistentStateException;
import uk.ac.ebi.age.transaction.InvalidStateException;
import uk.ac.ebi.age.transaction.ReadLock;
import uk.ac.ebi.age.transaction.Transaction;
import uk.ac.ebi.age.transaction.TransactionException;
import uk.ac.ebi.age.transaction.UpgradableReadLock;
import uk.ac.ebi.mg.filedepot.FileDepot;
import uk.ac.ebi.mg.rwarbiter.InvalidTokenException;
import uk.ac.ebi.mg.rwarbiter.UpgradableRWArbiter;

import com.pri.util.M2Pcodec;
import com.pri.util.Pair;
import com.pri.util.StringUtils;

public class H2SubmissionDB extends SubmissionDB
{
 private static final int REQUEST_LIMIT = 100;

 private static final String submissionDB = "SUBMISSIONDB";
 private static final String submissionTable = "SUBMISSION";
 private static final String moduleTable = "MODULE";
 private static final String attachmentTable = "ATTACHMENT";
 private static final String historyTable = "HISTORY";

 private static final String selectSubmissionIDSQL = "SELECT id FROM "+submissionDB+"."+submissionTable
 +" WHERE id=?";
 
 private static final String selectSubmissionSQL = "SELECT * FROM "+submissionDB+"."+submissionTable
 +" WHERE id='";
 
 private static final String selectModuleBySubmissionSQL = "SELECT * FROM "+submissionDB+"."+moduleTable
 +" WHERE submid=?";
 
 private static final String selectAttachmentBySubmissionSQL = "SELECT * FROM "+submissionDB+"."+attachmentTable
 +" WHERE submid=?";

 private static final String selectHistoryBySubmissionSQL = "SELECT * FROM "+submissionDB+"."+historyTable
 +" WHERE id=? ORDER BY mtime";
 
 private static final String deleteSubmissionSQL = "DELETE FROM "+submissionDB+"."+submissionTable
 +" WHERE id=?";

 private static final String deleteSubmissionHistorySQL = "DELETE FROM "+submissionDB+"."+historyTable
 +" WHERE id=?";
 
 private static final String insertSubmissionSQL = "INSERT INTO "+submissionDB+"."+submissionTable
 +" (id,desc,ctime,mtime,creator,modifier,ft_desc) VALUES (?,?,?,?,?,?,?)";
 
 private static final String insertModuleSQL = "INSERT INTO "+submissionDB+"."+moduleTable
 +" (id,submid,desc,ctime,mtime,creator,modifier,docver) VALUES (?,?,?,?,?,?,?,?)";
 
 private static final String insertAttachmentSQL = "INSERT INTO "+submissionDB+"."+attachmentTable
 +" (id,submid,desc,ctime,mtime,creator,modifier,filever,isglobal) VALUES (?,?,?,?,?,?,?,?,?)";
 
 private static final String insertHistorySQL = "INSERT INTO "+submissionDB+"."+historyTable
 +" (id,mtime,modifier,descr,diff) VALUES (?,?,?,?,?)";

 private static final String switchSubmissionRemovedSQL = 
 "UPDATE "+submissionDB+"."+submissionTable+" SET removed=? WHERE id=?";
 
 
 private static final String h2DbPath = "h2db";
 private static final String docDepotPath = "docs";
 private static final String attDepotPath = "att";
 
 private static final Charset docCharset = Charset.forName("UTF-8");
 
 private Connection permConn;
 private final AtomicBoolean permConnFree = new AtomicBoolean( true );
 
 private FileDepot docDepot;
 private FileDepot attachmentDepot;
 
 private final UpgradableRWArbiter<SDBReadLock,SDBTransaction,SDBUpgReadLock> arbiter = new UpgradableRWArbiter<>( new TokFactory() );
 
 private String connectionString;

 private FileResourceManager txManager;

 private String docDepotRelPath;
 private String attDepotRelPath;
 
 private String transactionId;
 
 private static StringUtils.ReplacePair likePairs[] = new StringUtils.ReplacePair[]
                 {
                  new StringUtils.ReplacePair('\'',"''"), 
                  new StringUtils.ReplacePair('*',"%") 
                 };
 
 public H2SubmissionDB( FileResourceManager frm, String submRelPath )
 {
  try
  {
   Class.forName("org.h2.Driver");
   
   if( submRelPath.length() > 0 && ( submRelPath.charAt(0) == '/' || submRelPath.charAt(0) == '\\' ) )
    submRelPath = submRelPath.substring(1);

   int pos = submRelPath.length()-1;
   if( pos >= 0 &&  submRelPath.charAt(pos) != '/' && submRelPath.charAt(0) != '\\'  )
    submRelPath = submRelPath+'/';
   
   docDepotRelPath=submRelPath+docDepotPath+'/';
   attDepotRelPath=submRelPath+attDepotPath+'/';
   
   File sbmDbRoot = new  File( frm.getStoreDir() , submRelPath );
   
   connectionString = "jdbc:h2:"+new File(sbmDbRoot,h2DbPath).getAbsolutePath();
   
   permConn = DriverManager.getConnection(connectionString, "sa", "");
   permConn.setAutoCommit(false);
   
   initSubmissionDb();
   
   docDepot = new FileDepot( new File(sbmDbRoot,docDepotPath), true );
   attachmentDepot = new FileDepot( new File(sbmDbRoot,attDepotPath), true );
  }
  catch(Exception e)
  {
   e.printStackTrace();
   
   throw new RuntimeException("Database initialization error: "+e.getMessage(),e);
  }

 }
 
 private Connection createConnection() throws SQLException
 {
  if( permConnFree.compareAndSet(true, false) )
   return permConn;
 
  Connection conn = DriverManager.getConnection(connectionString, "sa", "");
  conn.setAutoCommit(false);

  return conn;
 }
 
 private void releaseConnection( Connection conn ) throws SQLException
 {
  if( permConn == conn )
  {
   permConnFree.set(true);
   return;
  }
  
  conn.close();
 }
 
 private void initSubmissionDb() throws SQLException
 {
  Connection conn = createConnection();
  
  try
  {
   
   Statement stmt = conn.createStatement();
   
   stmt.executeUpdate("CREATE SCHEMA IF NOT EXISTS "+submissionDB);
 
   stmt.executeUpdate("CREATE TABLE IF NOT EXISTS "+submissionDB+'.'+submissionTable+" ("+
     "id VARCHAR PRIMARY KEY, desc VARCHAR, ctime BIGINT NOT NULL, mtime BIGINT NOT NULL, creator VARCHAR NOT NULL, modifier VARCHAR NOT NULL," +
     " removed BOOLEAN NOT NULL DEFAULT FALSE, FT_DESC VARCHAR)");
 
   stmt.executeUpdate("CREATE INDEX IF NOT EXISTS ctimeIdx ON "+submissionDB+'.'+submissionTable+"(ctime)");
   stmt.executeUpdate("CREATE INDEX IF NOT EXISTS mtimeIdx ON "+submissionDB+'.'+submissionTable+"(mtime)");
   stmt.executeUpdate("CREATE INDEX IF NOT EXISTS creatorIdx ON "+submissionDB+'.'+submissionTable+"(creator)");
   stmt.executeUpdate("CREATE INDEX IF NOT EXISTS modifierIdx ON "+submissionDB+'.'+submissionTable+"(modifier)");
   stmt.executeUpdate("CREATE INDEX IF NOT EXISTS removedIdx ON "+submissionDB+'.'+submissionTable+"(removed)");
 
   stmt.executeUpdate("CREATE TABLE IF NOT EXISTS "+submissionDB+'.'+moduleTable+" ("+
     "id VARCHAR PRIMARY KEY, submid VARCHAR NOT NULL, desc VARCHAR, ctime BIGINT NOT NULL, mtime BIGINT NOT NULL," +
     " creator VARCHAR NOT NULL, modifier VARCHAR NOT NULL, docver BIGINT NOT NULL," +
     " FOREIGN KEY(submid) REFERENCES "
     +submissionDB+'.'+submissionTable+"(id) ON DELETE CASCADE )");
 
   stmt.executeUpdate("CREATE TABLE IF NOT EXISTS "+submissionDB+'.'+attachmentTable+" ("+
     "id VARCHAR, submid VARCHAR NOT NULL, desc VARCHAR, ctime BIGINT NOT NULL, mtime BIGINT NOT NULL," +
     " creator VARCHAR NOT NULL, modifier VARCHAR NOT NULL, filever BIGINT NOT NULL, isglobal BOOL NOT NULL," +
     " PRIMARY KEY (id,submid), FOREIGN KEY (submid) REFERENCES "
     +submissionDB+'.'+submissionTable+"(id) ON DELETE CASCADE )");
 
   stmt.executeUpdate("CREATE TABLE IF NOT EXISTS "+submissionDB+'.'+historyTable+" ("+
     "id VARCHAR NOT NULL, mtime BIGINT NOT NULL, modifier VARCHAR NOT NULL, descr VARCHAR, diff BINARY, data BINARY," +
     " PRIMARY KEY (id,mtime) )");
 
   conn.commit();
   
   stmt.executeUpdate("CREATE ALIAS IF NOT EXISTS FTL_INIT FOR \"org.h2.fulltext.FullTextLucene.init\"");
   stmt.executeUpdate("CALL FTL_INIT()");
 
   try
   {
    stmt.executeUpdate("CALL FTL_CREATE_INDEX('"+submissionDB+"', '"+submissionTable+"', 'FT_DESC')");
   }
   catch (SQLException e)
   {
//    System.out.println( "Duplicate index error code: "+e.getErrorCode() );
    
    if( e.getErrorCode() != 23505 )
     e.printStackTrace();
   }
   
   stmt.close();
  }
  finally
  {
   releaseConnection(conn);
  }
 }
 
 private String getTxId() throws ResourceManagerException
 {
  if( transactionId != null )
   return transactionId;
  
  transactionId = txManager.generatedUniqueTxId();
  
  txManager.startTransaction(transactionId);
  
  return transactionId;
 }
 
 @Override
 public void storeSubmission( SDBTransaction t, SubmissionMeta sMeta, SubmissionMeta oldSbm, String updateDescr) throws SubmissionDBException
 {
  if(!t.isActive())
   throw new InvalidStateException();
  
  SubmissionDiff diff = null;
  
  diff = calculateDiff(sMeta,oldSbm);
  
  if( oldSbm == null && updateDescr == null  )
   updateDescr = "Initial submission";
  
  StringBuilder sb = new StringBuilder(1000);

  sb.append(sMeta.getDescription());

  if(sMeta.getDataModules() != null)
  {
   for(DataModuleMeta dmm : sMeta.getDataModules())
    sb.append(' ').append(dmm.getDescription());
  }

  if(sMeta.getAttachments() != null)
  {
   for(FileAttachmentMeta dmm : sMeta.getAttachments())
    sb.append(' ').append(dmm.getDescription());
  }

  Connection conn = t.getConnection();
 
  try
  {
   if( conn == null )
    t.setConnection( conn = createConnection() );
   
   PreparedStatement pstsmt = null;

   if( oldSbm != null )
   {
    pstsmt = conn.prepareStatement(deleteSubmissionSQL);
    
    pstsmt.setString(1, oldSbm.getId());
    
    pstsmt.executeUpdate();
    pstsmt.close();
   }

//   ByteArrayOutputStream baosData = new ByteArrayOutputStream();
//   ObjectOutputStream oos = new ObjectOutputStream( baosData );
//   oos.writeObject(oldSbm);
//   oos.close();

   
   //(id,mtime,modifier,data)
   pstsmt = conn.prepareStatement(insertHistorySQL);
   pstsmt.setString(1, sMeta.getId());
   pstsmt.setLong(2, sMeta.getModificationTime());
   pstsmt.setString(3, sMeta.getModifier());
   pstsmt.setString(4, updateDescr);
   
   if( diff != null )
   {
    ByteArrayOutputStream baosDiff = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream( baosDiff );
    oos.writeObject(diff);
    oos.close();
    
    pstsmt.setBytes(5, baosDiff.toByteArray());
   }
   else
    pstsmt.setNull(5, Types.BINARY);
   
   pstsmt.executeUpdate();
   pstsmt.close();

   
   // (id,desc,ctime,mtime,creator,modifier,ft_desc)
   pstsmt = conn.prepareStatement(insertSubmissionSQL);
   pstsmt.setString(1, sMeta.getId());
   pstsmt.setString(2, sMeta.getDescription());
   pstsmt.setLong(3, sMeta.getSubmissionTime());
   pstsmt.setLong(4, sMeta.getModificationTime());
   pstsmt.setString(5, sMeta.getSubmitter());
   pstsmt.setString(6, sMeta.getModifier());
   pstsmt.setString(7, sb.toString());

   pstsmt.executeUpdate();
   pstsmt.close();

   if(sMeta.getDataModules() != null)
   {
    //(id,submid,desc,ctime,mtime,creator,modifier,docver)
    pstsmt = conn.prepareStatement(insertModuleSQL);
    for(DataModuleMeta dmm : sMeta.getDataModules())
    {
     pstsmt.setString(1, dmm.getId());
     pstsmt.setString(2, sMeta.getId());
     pstsmt.setString(3, dmm.getDescription());
     pstsmt.setLong(4, dmm.getSubmissionTime());
     pstsmt.setLong(5, dmm.getModificationTime());
     pstsmt.setString(6, dmm.getSubmitter());
     pstsmt.setString(7, dmm.getModifier());
     pstsmt.setLong(8, dmm.getDocVersion());

     pstsmt.executeUpdate();

     if( dmm.getText() != null )
     {
      String txId = t.getTransactionId();
      
      if( txId == null )
       t.setTransactionId( txId = getTxId() );
      
      OutputStreamWriter wrtr = new OutputStreamWriter(
        txManager.writeResource(txId, 
          docDepotRelPath+docDepot.getRelativeFilePath(dmm.getId(), dmm.getDocVersion())), docCharset);
      
      wrtr.write(dmm.getText());
      
      wrtr.close();
     }
     
    }

    pstsmt.close();
   }

   if( sMeta.getAttachments() != null )
   {
    //(id,submid,desc,ctime,mtime,creator,modifier,filever)
    pstsmt = conn.prepareStatement(insertAttachmentSQL);
    
    for(FileAttachmentMeta fatm : sMeta.getAttachments())
    {
     pstsmt.setString(1, fatm.getId());
     pstsmt.setString(2, sMeta.getId());
     pstsmt.setString(3, fatm.getDescription());
     pstsmt.setLong(4, fatm.getSubmissionTime());
     pstsmt.setLong(5, fatm.getModificationTime());
     pstsmt.setString(6, fatm.getSubmitter());
     pstsmt.setString(7, fatm.getModifier());
     pstsmt.setLong(8, fatm.getFileVersion());
     pstsmt.setBoolean(9, fatm.isGlobal());

     pstsmt.executeUpdate();

    }

    pstsmt.close(); 
   }

  }
  catch(Exception e)
  {
   throw new SubmissionDBException("System error", e);
  }

 }

 @Override
 public boolean tranklucateSubmission( SDBTransaction t, String sbmID) throws SubmissionDBException
 {
  if(!t.isActive())
   throw new InvalidStateException();

  List<HistoryEntry> hist = getHistory(t, sbmID);
  
  if( hist == null || hist.size() == 0 )
   return false;
  
  for( HistoryEntry he : hist )
  {
   Collection<DataModuleDiff> mDiffs =  he.getDiff().getDataModuleDiffs();
   
   if( mDiffs != null )
   {
    String tx = t.getTransactionId();
    
    for( DataModuleDiff dmd : mDiffs )
    {
     try
     {
      if( tx == null )
       t.setTransactionId(tx=getTxId());

      String docPath = docDepotRelPath +  docDepot.getRelativeFilePath(createFileId(sbmID, dmd.getId()), dmd.getNewDocumentVersion());
      
      txManager.deleteResource(tx, docPath);
     }
     catch(ResourceManagerException e)
     {
      throw new SubmissionDBException("Can't delete file", e);
     }
 
    }
   }
   
   Collection<AttachmentDiff> aDiffs =  he.getDiff().getAttachmentDiffs();
   
   if( aDiffs != null )
   {
    String tx = t.getTransactionId();

    for( AttachmentDiff atd : aDiffs )
    {
     try
     {
      if( tx == null )
       t.setTransactionId(tx=getTxId());

      String attFile = attDepotRelPath +  attachmentDepot.getRelativeFilePath(createFileId(sbmID, atd.getId()), atd.getNewFileVersion());
      
      txManager.deleteResource(tx, attFile);
     }
     catch(ResourceManagerException e)
     {
      throw new SubmissionDBException("Can't delete file", e);
     }
    }
   }

  }
  
  Connection conn = t.getConnection();
  
  try
  {
   if( conn == null )
    t.setConnection( conn = createConnection() );
   
   PreparedStatement stmt = conn.prepareStatement(deleteSubmissionHistorySQL);
   
   stmt.setString(1, sbmID);
   
   stmt.executeUpdate();
   
   
   stmt = conn.prepareStatement(deleteSubmissionSQL);
   
   stmt.setString(1, sbmID);
   
   stmt.executeUpdate();
   
   stmt.close();

   return true;
  }
  catch(Exception e)
  {
   throw new SubmissionDBException("System error", e);
  }

 }

 
 @Override
 public boolean removeSubmission(SDBTransaction t, String sbmID) throws SubmissionDBException
 {
  return setSubmissionRemoved(t, sbmID, true );
 }
 
 @Override
 public boolean restoreSubmission(SDBTransaction t, String sbmID) throws SubmissionDBException
 {
  return setSubmissionRemoved(t, sbmID, false );
 }

 
 private boolean setSubmissionRemoved(SDBTransaction t, String sbmID, boolean rem) throws SubmissionDBException
 {
  if(!t.isActive())
   throw new InvalidStateException();
  
  Connection conn = t.getConnection();
  
  try
  {
   if( conn == null )
    t.setConnection(conn = createConnection());
   
   PreparedStatement stmt = conn.prepareStatement(switchSubmissionRemovedSQL);
   
   stmt.setBoolean(1, rem);
   stmt.setString(2, sbmID);
   
   int nUp = stmt.executeUpdate();
   
   return nUp==1;
  }
  catch(Exception e)
  {
   throw new SubmissionDBException("System error", e);
  }
  
 }
 
 @Override
 public List<HistoryEntry> getHistory( SDBReadLock rl, String sbmId ) throws SubmissionDBException
 {
  if(! rl.isActive())
   throw new InvalidStateException();

  Connection conn = rl.getConnection();
  
  try
  {
   if( conn == null )
    rl.setConnection( conn = createConnection() );

   List<HistoryEntry> res = new ArrayList<HistoryEntry>();
   
   
   PreparedStatement pstmt = conn.prepareStatement( selectHistoryBySubmissionSQL );
   
   pstmt.setString(1, sbmId);
   
   ResultSet rst = pstmt.executeQuery();
   
   //id VARCHAR, mtime BIGINT, modifier VARCHAR, descr VARCHAR, diff BINARY, data BINARY
   
   while( rst.next() )
   {
    HistoryEntry ent = Factory.createHistoryEntry();
    
    ent.setModificationTime( rst.getLong("mtime") );
    ent.setModifier( rst.getString("modifier") );
    ent.setDescription( rst.getString("descr") );
    
    InputStream dfis = rst.getBinaryStream("diff");
    
    if( ! rst.wasNull() )
    {
     ObjectInputStream ois = new ObjectInputStream(dfis);
     ent.setDiff( (SubmissionDiff)ois.readObject() );
     dfis.close();
    }
    
    res.add(ent);
   }

   rst.close();
   pstmt.close();
   
   return res;
  }
  catch(Exception e)
  {
   e.printStackTrace();
   
   throw new SubmissionDBException("System error", e);
  }

 }
 
 private SubmissionDiff calculateDiff(SubmissionMeta sMeta, SubmissionMeta oldSbm)
 {
  SubmissionDiff sDif = Factory.createSubmissionDiff();
  
  sDif.setId( sMeta.getId() );
  
  sDif.setCreator( sMeta.getSubmitter() );
  sDif.setCreationTime( sMeta.getSubmissionTime() );
  sDif.setModifier( sMeta.getModifier() );
  sDif.setModificationTime( sMeta.getModificationTime() );
  sDif.setDescription(sMeta.getDescription());
  
  boolean chngd = false;
  
  if( oldSbm != null )
  {
   if(sMeta.getDescription() != null)
   {
    if(oldSbm.getDescription() == null || !sMeta.getDescription().equals(oldSbm.getDescription()))
     chngd = true;
   }
   else if(oldSbm.getDescription() != null)
    chngd = true;
  }
  
  sDif.setDescriptionChanged( chngd );
  
  if( sMeta.getDataModules() != null )
  {
   for(DataModuleMeta dmm : sMeta.getDataModules())
   {
    DataModuleMeta oldDm = null;
    
    if( oldSbm != null && oldSbm.getDataModules() != null )
    {
     for(DataModuleMeta odmm : oldSbm.getDataModules())
     {
      if(odmm.getId().equals(dmm.getId()))
      {
       oldDm = odmm;
       break;
      }
     }
    }
    
    DataModuleDiff mdif = Factory.createDataModuleDiff();
    mdif.setId( dmm.getId() );
    mdif.setCreator(dmm.getSubmitter());
    mdif.setModifier(dmm.getModifier());
    mdif.setCreationTime( dmm.getSubmissionTime() );
    mdif.setModificationTime( dmm.getModificationTime() );
    mdif.setDescription( dmm.getDescription() );
    mdif.setNewDocumentVersion(dmm.getDocVersion());

    if( oldDm == null  )
    {
     mdif.setStatus( Status.NEW );
     
     sDif.addDataModuleDiff( mdif );
    }
    else
    {
     boolean dscChngd = false;
     
     if( dmm.getDescription() != null )
     {
      if( oldDm.getDescription() == null || ! dmm.getDescription().equals(oldDm.getDescription())  )
       dscChngd = true;
     }
     else if( oldDm.getDescription() != null )
      dscChngd = true;    
    

     if( dscChngd || dmm.getDocVersion() != oldDm.getDocVersion() )
     {
      mdif.setStatus( Status.UPDATE );
      
      mdif.setMetaChanged( dscChngd );
      mdif.setDataChanged( dmm.getDocVersion() != oldDm.getDocVersion() );
      
      mdif.setOldDocumentVersion(oldDm.getDocVersion());
     }
     else
      mdif.setStatus( Status.KEEP );
      
     sDif.addDataModuleDiff( mdif );
    }

   }
  }
  
  if( oldSbm != null && oldSbm.getDataModules() != null )
  {
   for(DataModuleMeta odmm : oldSbm.getDataModules())
   {
    DataModuleMeta updDm = null;
    
    if( sMeta.getDataModules() != null )
    {
     for(DataModuleMeta ndmm : sMeta.getDataModules() )
     {
      if(odmm.getId().equals(ndmm.getId()))
      {
       updDm = ndmm;
       break;
      }
     }
    }

    if( updDm == null )
    {
     DataModuleDiff mdif = Factory.createDataModuleDiff();
     
     mdif.setId( odmm.getId() );
     mdif.setStatus( Status.DELETE );
     mdif.setCreator(odmm.getSubmitter());
     mdif.setModifier(odmm.getModifier());
     mdif.setCreationTime( odmm.getSubmissionTime() );
     mdif.setModificationTime( odmm.getModificationTime() );
     mdif.setDescription( odmm.getDescription() );
     mdif.setNewDocumentVersion(odmm.getDocVersion());
     
     sDif.addDataModuleDiff( mdif );
    }
    
   }
  }
  
  
  
  if( sMeta.getAttachments() != null )
  {
   for(FileAttachmentMeta attm : sMeta.getAttachments())
   {
    FileAttachmentMeta oldAttm = null;
    
    if( oldSbm != null && oldSbm.getAttachments() != null )
    {
     for(FileAttachmentMeta oattm : oldSbm.getAttachments())
     {
      if(oattm.getId().equals(attm.getId()))
      {
       oldAttm = oattm;
       break;
      }
     }
    }
    
    AttachmentDiff adif = Factory.createAttachmentDiff();
    adif.setId( attm.getId() );
    adif.setCreator(attm.getSubmitter());
    adif.setModifier(attm.getModifier());
    adif.setCreationTime( attm.getSubmissionTime() );
    adif.setModificationTime( attm.getModificationTime() );
    adif.setDescription( attm.getDescription() );
    adif.setNewFileVersion(attm.getFileVersion());
    adif.setGlobal(attm.isGlobal());
    
    
    if( oldAttm == null  )
    {
     adif.setStatus( Status.NEW );
     
     sDif.addAttachmentDiff( adif );
    }
    else
    {
     boolean dscChngd = false;
     
     if( attm.getDescription() != null )
     {
      if( oldAttm.getDescription() == null || ! attm.getDescription().equals(oldAttm.getDescription())  )
       dscChngd = true;
     }
     else if( oldAttm.getDescription() != null )
      dscChngd = true;    
    

     if( dscChngd || attm.getFileVersion() != oldAttm.getFileVersion() || attm.isGlobal() != oldAttm.isGlobal() )
     {
      adif.setStatus( Status.UPDATE );
      
      adif.setMetaChanged( dscChngd );
      adif.setVisibilityChanged( attm.isGlobal() != oldAttm.isGlobal() );
      adif.setDataChanged( attm.getFileVersion() != oldAttm.getFileVersion() );
      
      adif.setOldFileVersion( oldAttm.getFileVersion() );
     }
     else
      adif.setStatus(Status.KEEP);
     
     sDif.addAttachmentDiff( adif );
    }

   }
  }
  
  if( oldSbm != null && oldSbm.getAttachments() != null )
  {
   for(FileAttachmentMeta oattm : oldSbm.getAttachments())
   {
    FileAttachmentMeta updAttm = null;
    
    if( sMeta.getAttachments() != null )
    {
     for(FileAttachmentMeta ndmm : sMeta.getAttachments() )
     {
      if(oattm.getId().equals(ndmm.getId()))
      {
       updAttm = ndmm;
       break;
      }
     }
    }

    if( updAttm == null )
    {
     AttachmentDiff adif = Factory.createAttachmentDiff();
     
     adif.setId( oattm.getId() );
     adif.setStatus( Status.DELETE );
     adif.setCreator(oattm.getSubmitter());
     adif.setModifier(oattm.getModifier());
     adif.setCreationTime( oattm.getSubmissionTime() );
     adif.setModificationTime( oattm.getModificationTime() );
     adif.setDescription( oattm.getDescription() );
     adif.setNewFileVersion(oattm.getFileVersion());
     adif.setGlobal(oattm.isGlobal());
     
     sDif.addAttachmentDiff( adif );
    }
    
   }
  }

  
  return sDif;
 }


 @Override
 public void shutdown()
 {
  if( permConn != null )
   try
   {
    permConn.close();
   }
   catch(SQLException e)
   {
    e.printStackTrace();
   }
  
  if( docDepot != null )
   docDepot.shutdown();  

  if( attachmentDepot != null )
   attachmentDepot.shutdown();  
 }

 @Override
 public void init()
 {
 }

 @Override
 public SubmissionReport getSubmissions(SDBReadLock rl, SubmissionQuery q) throws SubmissionDBException
 {
  if( ! rl.isActive() )
   throw new InvalidStateException();
  
  String query = q.getQuery();
  
  if( query != null )
  {
   query = query.trim();
   if( query.length() == 0 )
    query = null;
  }
  
  boolean needJoin = q.getModuleID() != null || q.getModifiedFrom() != -1 || q.getModifiedTo() != -1 || q.getModifier() != null;
  
  
  StringBuilder condExpr = new StringBuilder(800);
  
  int pos=0;
  if( q.getTotal() <= 0 )
  {
   condExpr.append("SELECT COUNT( DISTINCT S.ID) AS SC");
   
   if( needJoin )
    condExpr.append(", COUNT( DISTINCT M.ID )");
  
   pos = condExpr.length();
  }
  else
   condExpr.append("SELECT S.*");
  
  if( query != null )
  {
   condExpr.append(" FROM FTL_SEARCH_DATA('");
   StringUtils.appendEscaped(condExpr, q.getQuery(), '\'', '\'');
   condExpr.append("', 0, 0) FT JOIN "+submissionDB+'.'+submissionTable+" S ON  S.ID=FT.KEYS[0]");
  }
  else
   condExpr.append(" FROM "+submissionDB+'.'+submissionTable+" S");

  if( needJoin )
   condExpr.append(" LEFT JOIN "+submissionDB+'.'+moduleTable+" M ON S.id=M.submid");

  boolean hasCond = false;
  
  if( q.getStateSelector() != Selector.BOTH )
  {
   condExpr.append(" WHERE removed="+( q.getStateSelector() == Selector.REMOVED?"true":"false" ) );
   hasCond = true;
  }
  
  if( q.getCreatedFrom() != -1 )
  {
   if(hasCond)
    condExpr.append(" AND");
   else
    condExpr.append(" WHERE");

   condExpr.append(" S.ctime >= "+ q.getCreatedFrom());
   hasCond=true;
  }
  
  if( q.getCreatedTo() != -1 )
  {
   if(hasCond)
    condExpr.append(" AND");
   else
    condExpr.append(" WHERE");
   
   condExpr.append(" S.ctime <= ").append(q.getCreatedTo());
   hasCond=true;
  }

  if( q.getModifiedFrom() != -1 )
  {
   if(hasCond)
    condExpr.append(" AND");
   else
    condExpr.append(" WHERE");
   
   condExpr.append(" (S.mtime >= ").append(q.getModifiedFrom()).append(" OR M.mtime >= ").append(q.getModifiedFrom()).append(")");
   hasCond=true;
  }

  if( q.getModifiedTo() != -1 )
  {
   if(hasCond)
    condExpr.append(" AND");
   else
    condExpr.append(" WHERE");
   
   condExpr.append(" (S.mtime <= ").append(q.getModifiedTo()).append(" OR M.mtime <= ").append(q.getModifiedTo()).append(")");
   hasCond=true;
  }

  if( q.getSubmitter() != null )
  {
   if(hasCond)
    condExpr.append(" AND ");
   else
    condExpr.append(" WHERE ");
   
   addWildcardedCondition(condExpr, "S.creator", q.getSubmitter());
   
   hasCond=true;
  }

  if( q.getModifier() != null )
  {
   if(hasCond)
    condExpr.append(" AND (");
   else
    condExpr.append(" WHERE (");

   addWildcardedCondition(condExpr, "S.modifier", q.getModifier() );

   condExpr.append(" OR ");

   addWildcardedCondition(condExpr, "M.modifier", q.getModifier() );
   
   condExpr.append(")");
   
   hasCond=true;
  }

  if( q.getSubmissionID() != null )
  {
   if(hasCond)
    condExpr.append(" AND ");
   else
    condExpr.append(" WHERE ");
   
   addWildcardedCondition(condExpr, "S.id", q.getSubmissionID());
   
   hasCond=true;
  }
  
  if( q.getModuleID() != null )
  {
   if(hasCond)
    condExpr.append(" AND ");
   else
    condExpr.append(" WHERE ");
   
   addWildcardedCondition(condExpr, "M.id", q.getModuleID());

   hasCond=true;
  }
  
  
  Connection conn = rl.getConnection();
  
  try
  {
   if( conn == null )
    rl.setConnection( conn = createConnection() );
   
   SubmissionReport rep = new SubmissionReport();

   if( q.getTotal() <= 0 )
   {
    Statement stmt = conn.createStatement();
    
    ResultSet rst = stmt.executeQuery(condExpr.toString());
    
    rst.next();
    
    rep.setTotalSubmissions(rst.getInt(1));
    
    if( needJoin )
     rep.setTotalMatchedModules(rst.getInt(2));
    
    rst.close();
    stmt.close();
    
    String repStr = "SELECT S.*";
    
    for( int i=0; i < pos; i++ )
     condExpr.setCharAt(i, i>=repStr.length()?' ':repStr.charAt(i));
   }
   else
    rep.setTotalSubmissions( q.getTotal() );

   if( q.getLimit() <= 0 )
    q.setLimit(REQUEST_LIMIT);
   
   condExpr.append(" LIMIT ").append(q.getLimit());
   condExpr.append(" OFFSET ").append(q.getOffset());
   
   
   List<SubmissionMeta> slist = new ArrayList<SubmissionMeta>( REQUEST_LIMIT );
   
   extractSubmission(conn, condExpr.toString(), slist);
   
   rep.setSubmissions( slist );
   
   return rep;
  }
  catch(Exception e)
  {
   throw new SubmissionDBException("System error", e);
  }
  
  
  //select * from FTL_SEARCH_DATA('Hello seva world', 0, 0) FT join tbl S ON  S.ID=FT.KEYS[0] left join stbl M on S.id=m.pid  where  FT.TABLE='TBL' and m.txt like '%va%';
  //select distinct S.id from  tbl S left join stbl M on S.id=M.pid join FTL_SEARCH_DATA('Seva world', 0, 0) FT ON ( FT.TABLE='TBL' AND S.ID=FT.KEYS[0] ) OR (  FT.TABLE='STBL' AND M.ID=FT.KEYS[0]) limit 1,1
  //SELECT T.*,S.* FROM FTL_SEARCH_DATA('Hello AND external', 0, 0) FT, AAA.TBL T, AAA.STBL S WHERE ( FT.TABLE='TBL' AND T.ID=FT.KEYS[0] ) OR (  FT.TABLE='STBL' AND S.ID=FT.KEYS[0] AND S.PID=T.ID)

 }
 
 private void addWildcardedCondition(StringBuilder sb, String field, String value)
 {
  if( value.indexOf('*') == -1 )
  {
   sb.append(field).append(" = '");
   StringUtils.appendEscaped(sb, value, '\'', '\'');
   sb.append('\'');
  }
  else
  {
   sb.append(field).append(" LIKE '");
   StringUtils.appendReplaced(sb, value, likePairs);
   sb.append('\'');
  }
 }

 private SubmissionMeta extractSubmission( Connection conn, String sql, Collection<SubmissionMeta> sbmlist ) throws SQLException
 {
  
  Statement s = null;
  PreparedStatement mstmt = null;
  PreparedStatement fstmt = null;

  ResultSet rstS = null;;
  try
  {
   s = conn.createStatement();
   mstmt = conn.prepareStatement( selectModuleBySubmissionSQL );
   fstmt = conn.prepareStatement( selectAttachmentBySubmissionSQL );

   rstS = s.executeQuery(sql);

   while(rstS.next())
   {
    SubmissionMeta simp = Factory.createSubmissionMeta();

    simp.setId(rstS.getString("id"));
    simp.setDescription(rstS.getString("desc") );

    simp.setSubmissionTime(rstS.getLong("ctime"));
    simp.setModificationTime(rstS.getLong("mtime"));
    
    simp.setSubmitter( rstS.getString("creator") );
    simp.setModifier(rstS.getString("modifier") );
 
    simp.setRemoved( rstS.getBoolean("removed") );
    
    mstmt.setString(1, simp.getId());
    
    ResultSet rstMF = mstmt.executeQuery();

    while( rstMF.next() )
    {
     DataModuleMeta dmm = Factory.createDataModuleMeta();
     
     dmm.setId(rstMF.getString("id"));
     dmm.setDescription(rstMF.getString("desc") );

     dmm.setSubmissionTime(rstMF.getLong("ctime"));
     dmm.setModificationTime(rstMF.getLong("mtime"));
     
     dmm.setSubmitter( rstMF.getString("creator") );
     dmm.setModifier(rstMF.getString("modifier") );

     dmm.setDocVersion(rstMF.getLong("docver"));
     
     simp.addDataModule(dmm);
    }

    rstMF.close();
    
    fstmt.setString(1, simp.getId());
    rstMF = fstmt.executeQuery();

    
    while( rstMF.next() )
    {
     FileAttachmentMeta fam = Factory.createFileAttachmentMeta();

     fam.setId(rstMF.getString("id"));
     fam.setDescription(rstMF.getString("desc") );

     fam.setSubmissionTime(rstMF.getLong("ctime"));
     fam.setModificationTime(rstMF.getLong("mtime"));
     
     fam.setSubmitter( rstMF.getString("creator") );
     fam.setModifier(rstMF.getString("modifier") );

     fam.setGlobal(rstMF.getBoolean("isglobal"));
     
     fam.setFileVersion(rstMF.getLong("filever") );

     simp.addAttachment(fam);
    }
    
    rstMF.close();
   
    if( sbmlist == null )
     return simp;
    
    sbmlist.add(simp);
   }

  }
  finally
  {
   if( s != null )
    s.close();
   
   if( mstmt != null )
    mstmt.close();
   
   if( fstmt != null )
    fstmt.close();
  }
  
  return null;
 }
 
 @Override
 public SubmissionMeta getSubmission(SDBReadLock rl, String id) throws SubmissionDBException
 {
  if( ! rl.isActive() )
   throw new InvalidStateException();
  
  StringBuilder sb = new StringBuilder();

  sb.append(selectSubmissionSQL);
  StringUtils.appendEscaped(sb, id, '\'', '\'');
  sb.append('\'');

  Connection conn = rl.getConnection();
  
  try
  {
   if( conn == null )
    rl.setConnection( conn = createConnection() );
   
   return extractSubmission(conn, sb.toString(), null);
  }
  catch(SQLException e)
  {
   e.printStackTrace();
   throw new SubmissionDBException("System error", e);
  }

 }

 @Override
 public boolean hasSubmission( SDBReadLock rl, String id) throws SubmissionDBException
 {
  PreparedStatement pstsmt = null;
  ResultSet rst = null;
  
  Connection conn = rl.getConnection();
  
  try
  {
   if( conn == null )
    rl.setConnection( conn = createConnection() );
   
   pstsmt = conn.prepareStatement(selectSubmissionIDSQL);
   
   pstsmt.setString(1, id);
   
   rst =pstsmt.executeQuery();
   
   if( rst.next() )
    return true;

   return false;
  }
  catch(SQLException e)
  {
   e.printStackTrace();
   throw new SubmissionDBException("System error", e);
  }
  finally
  {
   try
   {
    if( rst != null )
     rst.close();
    
    if( pstsmt != null )
     pstsmt.close();
   }
   catch(SQLException e1)
   {
    e1.printStackTrace();
   }
  }


 }

 @Override
 public void storeAttachment( SDBTransaction t, String submId, String fileId, long modificationTime, File aux) throws SubmissionDBException
 {
  String id = createFileId(submId, fileId);

  File dest = attachmentDepot.getFilePath(id, modificationTime);
  
  dest.delete();
  
  FileSystem fs = FileSystems.getDefault();
  
  t.addLink( fs.getPath(aux.getAbsolutePath()), fs.getPath( dest.getAbsolutePath() ) );
 }
 
 private String createFileId( String submId, String fileId )
 {
  submId = M2Pcodec.encode(submId);
  
  return String.valueOf(submId.length())+'.'+submId+'.'+M2Pcodec.encode(fileId);
 }

 @Override
 public File getAttachment( SDBReadLock rl, String clustId, String fileId, long ver )
 {
  if(!rl.isActive())
   throw new InvalidStateException();
  
  return attachmentDepot.getFilePath( createFileId(clustId, fileId), ver);
 }

 
 @Override
 public File getDocument( SDBReadLock rl, String clustId, String docId, long ver)
 {
  if(!rl.isActive())
   throw new InvalidStateException();

  return docDepot.getFilePath( createFileId(clustId, docId), ver);
 }

 

 @Override
 public SDBReadLock getReadLock()
 {
  return arbiter.getReadLock();
 }

 @Override
 public SDBUpgReadLock getUpgradableReadLock()
 {
  return arbiter.getUpgradableReadLock();
 }

 @Override
 public void releaseLock(ReadLock l)
 {
  if(!l.isActive())
   throw new InvalidStateException();

  if( l instanceof Transaction )
   throw new InvalidStateException("Use commit or rollback for transactions");

  try
  {
   arbiter.releaseLock((SDBReadLock) l);
  }
  catch(Exception e)
  {
   throw new InvalidStateException();
  }

 }

 @Override
 public SDBTransaction startTransaction()
 {
  return arbiter.getWriteLock();
 }
 
 @Override
 public SDBTransaction startTransaction( UpgradableReadLock ulk ) throws TransactionException
 {
  try
  {
   return arbiter.upgradeReadLock( (SDBUpgReadLock)ulk );
  }
  catch(InvalidTokenException e)
  {
   throw new TransactionException("Invalid token type", e);
  }

 }


 @Override
 public void commitTransaction(Transaction t) throws TransactionException
 {
  if(!t.isActive())
   throw new InvalidStateException();

  SDBTransaction sdbt = (SDBTransaction) t;

  Connection conn = sdbt.getConnection();

  try
  {

   if(conn != null)
   {
    try
    {
     if(!sdbt.isPrepared())
      conn.commit();
     else
     {
      Statement s = conn.createStatement();

      s.executeUpdate("COMMIT TRANSACTION T1");
     }
    }
    catch(SQLException e)
    {
     throw new TransactionException("SQL commit failed", e);
    }
   }
   
   if( sdbt.getTransactionId() != null )
   {
    try
    {
     txManager.commitTransaction(sdbt.getTransactionId());
    }
    catch(ResourceManagerException e)
    {
     throw new InconsistentStateException("File commit failed", e);
    }
   }
   
   if( sdbt.getLinks() != null )
   {
    for( Pair<Path,Path> l : sdbt.getLinks() )
    {
     try
     {
      Files.createLink( l.getSecond(), l.getFirst() );
     }
     catch(IOException e)
     {
      throw new InconsistentStateException("Link commit failed", e);
     }
    }
   }
  }
  finally
  {
   try
   {
    arbiter.releaseLock(sdbt);
   }
   catch(InvalidTokenException e)
   {
    throw new TransactionException("Invalid token type", e);
   }

   if(conn != null)
   {
    try
    {
     releaseConnection(conn);
    }
    catch(SQLException e)
    {
     e.printStackTrace();
    }
   }

  }

 }

 @Override
 public void rollbackTransaction(Transaction t) throws TransactionException
 {
  SDBTransaction sdbt = (SDBTransaction) t;

  Connection conn = sdbt.getConnection();

  try
  {
   if( conn != null )
   {
    if(!sdbt.isPrepared())
     permConn.rollback();
    else
    {
     Statement s = conn.createStatement();
     
     s.executeUpdate("ROLLBACK TRANSACTION T1");
     s.close();
    }
    
    if( sdbt.getTransactionId() != null )
     txManager.rollbackTransaction( sdbt.getTransactionId() );
    
   }
   
  }
  catch(Exception e)
  {
   throw new TransactionException("Rollback failed", e);
  }
  finally
  {
   try
   {
    arbiter.releaseLock(sdbt);
   }
   catch(InvalidTokenException e)
   {
    throw new TransactionException("Invalid token type", e);
   }

   if( conn != null )
   {
    try
    {
     releaseConnection(conn);
    }
    catch(SQLException e)
    {
     e.printStackTrace();
    }
   }
  }
 }

 @Override
 public void prepareTransaction(Transaction t) throws TransactionException
 {
  
  if(!t.isActive())
   throw new InvalidStateException();

  SDBTransaction sdbt = (SDBTransaction) t;

  Connection conn = sdbt.getConnection();
  
  try
  {
   if( conn != null )
   {
    Statement s = conn.createStatement();
    
    s.executeUpdate("PREPARE COMMIT T1");
   }
   
   if( sdbt.getTransactionId() != null )
   {
    txManager.prepareTransaction(sdbt.getTransactionId());
   }
   
   if( sdbt.getLinks() != null )
   {
    for( Pair<Path, Path> lnk : sdbt.getLinks() )
    {
     try
     {
      Files.createLink(lnk.getSecond(), lnk.getFirst());
      Files.delete(lnk.getSecond());
     }
     catch( IOException e)
     {
      throw new TransactionException("Can't create file link", e);
     }
    }
   }

   sdbt.setPrepared( true );
  }
  catch(Exception e)
  {
   throw new TransactionException("Commit preparation failed", e);
  }
 }


}
