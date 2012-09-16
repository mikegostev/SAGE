package uk.ac.ebi.age.storage.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jdbm.PrimaryTreeMap;
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
import uk.ac.ebi.age.storage.index.AgeIndexWritable;
import uk.ac.ebi.age.storage.index.AgeTextIndex;
import uk.ac.ebi.age.storage.index.KeyExtractor;
import uk.ac.ebi.age.storage.index.TextFieldExtractor;
import uk.ac.ebi.age.util.FileUtil;
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
 
 private PrimaryTreeMap<String,ModuleKey> globalObjectMap;
 private PrimaryTreeMap<ModuleKey,ModuleKey> clusterObjectMap;
 private PrimaryTreeMap<ModuleKey,Object> moduleSet;
 
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
 public Iterable<? extends AgeObject> executeQuery(AgeQuery qury)
 {
  return new QueryProcessor(qury);
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
 public AgeObject getObject(ModuleKey modk, String objectId)
 {
  DataModuleWritable dm = getDataModule(modk);
  
  if( dm == null )
   return null;
   
  return dm.getObject(objectId);
 }


 @Override
 public boolean hasDataModule(ModuleKey mk)
 {
  return moduleSet.containsKey(mk);
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
 public AgeTextIndex createAttachedTextIndex(String name, AgeQuery qury, Collection<TextFieldExtractor> cb) throws IndexIOException
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
  ModuleKey modK = globalObjectMap.get(objID);
  
  if( modK == null )
   return null;
  
  DataModuleWritable dm = getDataModule(modK);
  
  return dm.getObject(objID);
 }

 @Override
 public AgeObjectWritable getClusterObject(String clustId, String objID)
 {
  ModuleKey modK = new ModuleKey(clustId,objID);
  modK = clusterObjectMap.get(modK);
  
  if( modK == null )
   return null;
  
  DataModuleWritable dm = getDataModule(modK);
  
  return dm.getObject(objID);
 }

 @Override
 public Iterable<AgeObjectWritable> getAllObjects()
 {
  return new QueryProcessor(null);
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
 public DataModuleWritable getDataModule(ModuleKey modk )
 {
  DataModuleWritable dm = moduleCache.get(modk);
  
  if( dm == null )
  {
   try
   {
    dm = loadDataModule(modk);
   }
   catch(ModuleStoreException e)
   {
    e.printStackTrace();
    return null;
   }
   
   moduleCache.put(dm.getModuleKey(), dm);
  }
  
  return dm;
 }

 
 @Override
 public Iterable< ? extends DataModuleWritable> getDataModules()
 {
  return new ModuleIterable();
 }

 @Override
 public boolean deleteAttachment(String id, String clusterId, boolean global)
 {
  if( global )
   fileDepot.getFilePath(makeFileSysRef(id)).delete();
  
  File f = fileDepot.getFilePath(makeFileSysRef(id, clusterId));

  return f.delete();
 }

 @Override
 public File storeAttachment(String id, String clusterId, boolean global, File aux) throws AttachmentIOException
 {
  File fDest = fileDepot.getFilePath(makeFileSysRef(id, clusterId));
  fDest.delete();
  
  try
  {
   FileUtil.linkOrCopyFile(aux, fDest);
   
   if( global )
   {
    File glbfDest = fileDepot.getFilePath(makeFileSysRef(id));
    glbfDest.delete();
    
    FileUtil.linkOrCopyFile(aux, glbfDest);

   }
  }
  catch(IOException e)
  {
   throw new AttachmentIOException("Store attachment error: "+e.getMessage(), e);
  }
  
  return fDest;
 }


 @Override
 public void changeAttachmentScope( String id, String clusterId, boolean global ) throws AttachmentIOException
 {
  File globFile = fileDepot.getFilePath(makeFileSysRef(id));
  
  try
  {
   if(global)
   {
    File fSrc = fileDepot.getFilePath(makeFileSysRef(id, clusterId));
    FileUtil.linkOrCopyFile(fSrc, globFile);
   }
   else
    globFile.delete();
  }
  catch(IOException e)
  {
   throw new AttachmentIOException("Can't link file: "+e.getMessage(), e);
  }
 }

 @Override
 public void invalidateIndices()
 {
  lockWrite();
  
  try
  {
   for( AgeIndexWritable idx : indexMap.values() )
    idx.setDirty(true);
   
   enterMMode( config.getAutoMModeTimeout() );
  }
  finally
  {
   unlockWrite();
  }
 }
 
 private void rebuildDirtyIndices()
 {
  ArrayList<AgeObject> res = new ArrayList<AgeObject>();
  Collection<DataModuleWritable> mods = moduleMap.values();

  long ts=0;
  
  for( Map.Entry<String, AgeIndexWritable> idxEnt : indexMap.entrySet() )
  {
   AgeIndexWritable idx = idxEnt.getValue();
   
   if( ! idx.isDirty() )
    continue;
   
   assert log.debug("Start rebuilding index: "+idxEnt.getKey()) && ( (ts=System.currentTimeMillis()) > 0 ) ;
   
   Iterable<AgeObject> trv = traverse(idx.getQuery(), mods);

   res.clear();

   for(AgeObject nd : trv)
    res.add(nd);

   assert log.debug("Traversing finished. Index: "+idxEnt.getKey()+" "+(System.currentTimeMillis()-ts)+"ms") && ( (ts=System.currentTimeMillis()) > 0 ) ;
   
   idx.index(res, false);
  
   assert log.debug("Indexing finished. Index: "+idxEnt.getKey()+" "+(System.currentTimeMillis()-ts)+"ms");
 
   idx.setDirty( false );
  }

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

 private String getDataModuleFileName( ModuleKey sm )
 {
  return sm.getClusterId().length()+sm.getClusterId()+sm.getModuleId();
 }
 
 private void saveDataModule(DataModuleWritable sm) throws ModuleStoreException
 {
  File modFile = dataDepot.getFilePath( getDataModuleFileName(sm.getModuleKey()) );
  
  try
  {
   submRW.write(sm, modFile);
  }
  catch(Exception e)
  {
   modFile.delete();
   
   throw new ModuleStoreException("Can't store data module: "+e.getMessage(), e);
  }
 }
 
 private DataModuleWritable loadDataModule(ModuleKey mk) throws ModuleStoreException
 {
  File modFile = dataDepot.getFilePath( getDataModuleFileName(mk) );
  
  try
  {
   return submRW.read(modFile);
  }
  catch(Exception e)
  {
   throw new ModuleStoreException("Can't load data module: "+e.getMessage(), e);
  }
 }
 
 class QueryProcessorIterator implements Iterator<AgeObjectWritable>
 {
  private final AgeQuery query;
  
  private final Iterator<DataModuleWritable> modIter;
  private Iterator<AgeObjectWritable> objIter;
  
  private AgeObjectWritable nextObject;

  public QueryProcessorIterator(AgeQuery query)
  {
   modIter = new ModuleIterator();
   this.query = query;
  }

  @Override
  public boolean hasNext()
  {
   if( nextObject != null )
    return true;
   
   while( true )
   {
    if(objIter == null || !objIter.hasNext())
    {
     do
     {
      if(!modIter.hasNext())
      {
       nextObject = null;
       return false;
      }
      
      objIter = modIter.next().getObjects().iterator();

     } while(!objIter.hasNext());
    }
    
    nextObject = objIter.next();
    
    if( query == null || query.getExpression().test(nextObject) )
     return true;
   }
   
  }

  @Override
  public AgeObjectWritable next()
  {
   if( objIter == null && ! hasNext() )
    throw new NoSuchElementException();
   
   AgeObjectWritable obj = nextObject;
   nextObject = null;

   return obj;
  }

  @Override
  public void remove()
  {
   throw new UnsupportedOperationException();
  }

 }

 
 class QueryProcessor implements Iterable<AgeObjectWritable>
 {
  private final AgeQuery query;
  
  public QueryProcessor(AgeQuery qury)
  {
   query = qury;
  }
  @Override
  public Iterator<AgeObjectWritable> iterator()
  {
   return new QueryProcessorIterator(query);
  }
 }
 
 class ModuleIterable implements Iterable<DataModuleWritable>
 {
  
  public ModuleIterable()
  {
  }

  @Override
  public Iterator<DataModuleWritable> iterator()
  {
   return new ModuleIterator();
  }
 }
 
 class ModuleIterator implements Iterator<DataModuleWritable>
 {
  private final Set<ModuleKey> cachedSet = new HashSet<>();
  
  private Iterator<ModuleKey> modKeyIterator;
  private Iterator<ModuleKey> modKeyCacheIterator = moduleCache.keySet().iterator();
  private ModuleKey selKey;

  @Override
  public boolean hasNext()
  {
   if( selKey != null )
    return true;
   
   
   if( modKeyCacheIterator != null )
   {
    if( modKeyCacheIterator.hasNext() )
    {
     selKey = modKeyCacheIterator.next();

     cachedSet.add(selKey);

     return true;
    }
    else
    {
     modKeyCacheIterator = null;
     modKeyIterator = moduleSet.keySet().iterator();
    }
   }

   while( modKeyIterator.hasNext() )
   {
    selKey = modKeyIterator.next();
    
    if( ! cachedSet.contains(selKey) )
     return true;
   }
   
    return false;
  }

  @Override
  public DataModuleWritable next()
  {
   if( selKey == null && ! hasNext() )
    throw new NoSuchElementException();
   
   ModuleKey mk = selKey;
   selKey = null;
   
   return getDataModule(mk);
  }

  @Override
  public void remove()
  {
   throw new UnsupportedOperationException();
  }
  
 }
}
