package uk.ac.ebi.age.storage;

import java.io.File;
import java.util.Collection;

import uk.ac.ebi.age.ext.log.LogNode;
import uk.ac.ebi.age.model.ModuleKey;
import uk.ac.ebi.age.model.SemanticModel;
import uk.ac.ebi.age.model.writable.AgeObjectWritable;
import uk.ac.ebi.age.model.writable.DataModuleWritable;
import uk.ac.ebi.age.storage.exeption.AttachmentIOException;
import uk.ac.ebi.age.storage.exeption.ModuleStoreException;

public interface AgeStorageAdm extends AgeStorage
{
 @Override
 public AgeObjectWritable getGlobalObject(String objID);
 
 @Override
 public AgeObjectWritable getClusterObject(String clustId, String objID);
 
// @Override
// public Iterable<AgeObjectWritable> getAllObjects();

 
 void update(Collection<DataModuleWritable> modListToIns, Collection<ModuleKey> modListToDel, ConnectionInfo conninf) throws RelationResolveException, ModuleStoreException;

 boolean updateSemanticModel( SemanticModel sm, LogNode log ); // throws ModelStoreException;

// void init( String initStr) throws StorageInstantiationException;
 @Override
 void shutdown();

 void lockWrite();
 void unlockWrite();

// @Override
// Iterable<? extends DataModuleWritable> getDataModules();

 
 boolean deleteAttachment(String id, String clusterId, boolean global);
 File storeAttachment(String id, String clusterId, boolean global, File aux) throws AttachmentIOException;
 void changeAttachmentScope(String id, String clusterId, boolean global) throws AttachmentIOException;

 void invalidateIndices();

 boolean setMaintenanceMode( boolean mmode);
 boolean setMaintenanceMode( boolean mmode, long timeout );

 GlobalObjectConnection getGlobalObjectConnection(String id);
 GlobalObjectConnection getGlobalFileConnection(String id);

 GlobalObjectConnection getGlobalObjectConnectionRequest(String id);
 

}
