package uk.ac.ebi.age.storage.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import uk.ac.ebi.age.AgeResolver;
import uk.ac.ebi.age.ext.log.LogNode;
import uk.ac.ebi.age.model.AgeAttributeClass;
import uk.ac.ebi.age.model.AgeClass;
import uk.ac.ebi.age.model.AgeObject;
import uk.ac.ebi.age.model.AgeRelationClass;
import uk.ac.ebi.age.model.ModuleKey;
import uk.ac.ebi.age.model.SemanticModel;
import uk.ac.ebi.age.model.writable.AgeObjectWritable;
import uk.ac.ebi.age.model.writable.DataModuleWritable;
import uk.ac.ebi.age.query.AgeQuery;
import uk.ac.ebi.age.storage.AgeStorageAdm;
import uk.ac.ebi.age.storage.ConnectionInfo;
import uk.ac.ebi.age.storage.DataChangeListener;
import uk.ac.ebi.age.storage.DataModuleReaderWriter;
import uk.ac.ebi.age.storage.MaintenanceModeListener;
import uk.ac.ebi.age.storage.RelationResolveException;
import uk.ac.ebi.age.storage.exeption.AttachmentIOException;
import uk.ac.ebi.age.storage.exeption.IndexIOException;
import uk.ac.ebi.age.storage.exeption.ModuleStoreException;
import uk.ac.ebi.age.storage.impl.ser.SerializedStorageConfiguration;
import uk.ac.ebi.age.storage.index.AttachedSortedTextIndex;
import uk.ac.ebi.age.storage.index.AttachedTextIndex;
import uk.ac.ebi.age.storage.index.KeyExtractor;
import uk.ac.ebi.age.storage.index.TextFieldExtractor;
import uk.ac.ebi.age.storage.index.TextIndex;
import uk.ac.ebi.mg.executor.DefaultExecutorService;
import uk.ac.ebi.mg.filedepot.FileDepot;

import com.pri.util.M2codec;
import com.pri.util.collection.KeyHolderCache;

public class SageStorage implements AgeStorageAdm, AgeResolver
{
 private final KeyHolderCache<ModuleKey, DataModuleWritable> moduleCache = new KeyHolderCache<ModuleKey, DataModuleWritable>();
 
 private SemanticModel model;
 
 private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
 
 private final DataModuleReaderWriter submRW = new SerializedDataModuleReaderWriter();

 private final Collection<DataChangeListener> chgListeners = new ArrayList<DataChangeListener>(3);
 private final Collection<MaintenanceModeListener> mmodListeners = new ArrayList<MaintenanceModeListener>(3);
 
 private FileDepot dataDepot; 
 private FileDepot fileDepot; 
 
 private final boolean master = false;
 
 private volatile boolean maintenanceMode = false;
 
 private volatile long mModeTimeout=0;

 private long lastUpdate;
 
 private boolean dataDirty = false;
 
 private final SerializedStorageConfiguration config;
 
 private volatile Future<?> mmodeWDTimerFuture;


 @Override
 public void lockRead()
 {
  dbLock.readLock().lock();
 }

 @Override
 public void unlockRead()
 {
  dbLock.readLock().unlock();
 }

 
 @Override
 public void lockWrite()
 {
  dbLock.writeLock().lock();
 }

 @Override
 public void unlockWrite()
 {
  dbLock.writeLock().unlock();
 }

 @Override
 public Iterable<AgeObject> executeQuery(AgeQuery qury)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public SemanticModel getSemanticModel()
 {
  return model;
 }


 @Override
 public boolean updateSemanticModel(SemanticModel sm, LogNode log)
 {
  // TODO Auto-generated method stub
  return false;
 }
 
 @Override
 public AgeClass getDefinedAgeClass(String className)
 {
  return model.getDefinedAgeClass(className);
 }
 
 @Override
 public AgeAttributeClass getDefinedAgeAttributeClass(String className)
 {
  return model.getDefinedAgeAttributeClass(className);
 }
 
 @Override
 public AgeRelationClass getDefinedAgeRelationClass(String className)
 {
  return model.getDefinedAgeRelationClass(className);
 }
 
 @Override
 public AgeRelationClass getCustomAgeRelationClass(String className, ModuleKey modKey)
 {
  return getDataModule(modKey).getContextSemanticModel().getCustomAgeRelationClass(className);
 }

 @Override
 public AgeObject getObject(String clusterId, String moduleId, String objectId)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public boolean hasDataModule(String clstId, String id)
 {
  // TODO Auto-generated method stub
  return false;
 }

 @Override
 public boolean hasDataModule(ModuleKey mk)
 {
  // TODO Auto-generated method stub
  return false;
 }

 @Override
 public void addDataChangeListener(DataChangeListener dataChangeListener)
 {
  synchronized(chgListeners)
  {
   chgListeners.add(dataChangeListener);
  }
 }
 
 @Override
 public void addMaintenanceModeListener(MaintenanceModeListener mmListener)
 {
  synchronized(mmodListeners)
  {
   mmodListeners.add(mmListener);
  }
 }

 @Override
 public AttachedTextIndex createAttachedTextIndex(String name, AgeQuery qury, Collection<TextFieldExtractor> cb) throws IndexIOException
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public <KeyT> AttachedSortedTextIndex<KeyT> createAttachedSortedTextIndex(String name, AgeQuery qury, Collection<TextFieldExtractor> exts,
   KeyExtractor<KeyT> keyExtractor, Comparator<KeyT> comparator) throws IndexIOException
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public TextIndex createTextIndex(String name, AgeQuery qury, Collection<TextFieldExtractor> cb) throws IndexIOException
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public File getAttachment(String id)
 {
  return getAttachmentBySysRef(makeFileSysRef(id));
 }

 
 @Override
 public File getAttachment(String id, String clusterId)
 {
  return getAttachmentBySysRef(makeFileSysRef(id, clusterId));
 }
 
 private File getAttachmentBySysRef(String ref)
 {
  File f = fileDepot.getFilePath(ref);
  
  if( ! f.exists() )
   return null;
  
  return f;
 }

 private String makeFileSysRef(String id)
 {
  return "G"+M2codec.encode(id);
 }

 private String makeFileSysRef(String id, String clustID)
 {
  return String.valueOf(id.length())+'_'+M2codec.encode(id+clustID);
 }


 @Override
 public AgeObjectWritable getGlobalObject(String objID)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public AgeObjectWritable getClusterObject(String clustId, String objID)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public Collection< ? extends AgeObjectWritable> getAllObjects()
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public void update(Collection<DataModuleWritable> modListToIns, Collection<ModuleKey> modListToDel, ConnectionInfo conninf)
   throws RelationResolveException, ModuleStoreException
 {
  // TODO Auto-generated method stub

 }


 @Override
 public void shutdown()
 {
  // TODO Auto-generated method stub

 }


 @Override
 public DataModuleWritable getDataModule(String clstId, String name)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public DataModuleWritable getDataModule(ModuleKey modk )
 {
  // TODO Auto-generated method stub
  return null;
 }

 
 @Override
 public Collection< ? extends DataModuleWritable> getDataModules()
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public boolean deleteAttachment(String id, String clusterId, boolean global)
 {
  // TODO Auto-generated method stub
  return false;
 }

 @Override
 public File storeAttachment(String id, String clusterId, boolean global, File aux) throws AttachmentIOException
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public void changeAttachmentScope(String id, String clusterId, boolean global) throws AttachmentIOException
 {
  // TODO Auto-generated method stub

 }

 @Override
 public void invalidateIndices()
 {
  // TODO Auto-generated method stub

 }
 
 private void rebuildDirtyIndices()
 {
  // TODO Auto-generated method stub
  
 }

 private boolean enterMMode( long timeout )
 {
  try
  {
   lockWrite();
   
   if(maintenanceMode)
   {
    if( timeout > mModeTimeout )
     mModeTimeout = timeout;
    
    return false;
   }
   
   mModeTimeout = timeout;
   maintenanceMode = true;
   
   mmodeWDTimerFuture = DefaultExecutorService.getExecutorService().submit( new Runnable()
   {

    @Override
    public void run()
    {
     long wtCycle =0;

     Thread myThread = Thread.currentThread();
     String thName = myThread.getName();
     
     myThread.setName("MMode watchdog timer");

     while(true)
     {
      wtCycle = mModeTimeout + 500;
      
      try
      {
       Thread.sleep(wtCycle);
      }
      catch(InterruptedException e)
      {
      }

      if(!maintenanceMode)
       return;

      if((System.currentTimeMillis() - lastUpdate) > mModeTimeout && ! dbLock.isWriteLocked() )
      {
       if( dbLock.writeLock().tryLock() )
       {
        try
        {
         mmodeWDTimerFuture = null;
         leaveMMode();
         return;
        }
        finally
        {
         dbLock.writeLock().unlock();
         myThread.setName(thName);
        }
       }

      }

     }
    }
   });

  }
  finally
  {
   unlockWrite();
  }
  
  synchronized(mmodListeners)
  {
   for(MaintenanceModeListener mml : mmodListeners)
     mml.enterMaintenanceMode();
  }
  
  return true;
 }

 private boolean leaveMMode()
 {
  boolean dataModified = dataDirty;
  
  try
  {
   lockWrite();

   if(! maintenanceMode )
    return false;

   rebuildDirtyIndices();
   
   dataDirty = false;
   
   maintenanceMode = false;
   
   if( mmodeWDTimerFuture != null )
    mmodeWDTimerFuture.cancel( true );
  }
  finally
  {
   unlockWrite();
  }

  if( dataModified )
  {
   synchronized(chgListeners)
   {
    for(DataChangeListener chls : chgListeners )
     chls.dataChanged();
   }
  }
  
  synchronized(mmodListeners)
  {
   for(MaintenanceModeListener mml : mmodListeners)
     mml.exitMaintenanceMode();
  }

  return true;
 }


 @Override
 public boolean setMaintenanceMode( boolean mmode )
 {
  return setMaintenanceMode(mmode, config.getMaintenanceModeTimeout());
 }

 @Override
 public boolean setMaintenanceMode( boolean mmode, long timeout )
 {
  if( mmode )
   return enterMMode(timeout);
  else
   return leaveMMode();
 }


}
