package uk.ac.ebi.age.storage.impl;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;

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
import uk.ac.ebi.age.storage.MaintenanceModeListener;
import uk.ac.ebi.age.storage.RelationResolveException;
import uk.ac.ebi.age.storage.exeption.AttachmentIOException;
import uk.ac.ebi.age.storage.exeption.IndexIOException;
import uk.ac.ebi.age.storage.exeption.ModuleStoreException;
import uk.ac.ebi.age.storage.index.AttachedSortedTextIndex;
import uk.ac.ebi.age.storage.index.AttachedTextIndex;
import uk.ac.ebi.age.storage.index.KeyExtractor;
import uk.ac.ebi.age.storage.index.TextFieldExtractor;
import uk.ac.ebi.age.storage.index.TextIndex;

public class SageStorage implements AgeStorageAdm, AgeResolver
{

 @Override
 public void lockRead()
 {
  // TODO Auto-generated method stub

 }

 @Override
 public void unlockRead()
 {
  // TODO Auto-generated method stub

 }

 @Override
 public Collection<AgeObject> executeQuery(AgeQuery qury)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public SemanticModel getSemanticModel()
 {
  // TODO Auto-generated method stub
  return null;
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
  // TODO Auto-generated method stub

 }

 @Override
 public void addMaintenanceModeListener(MaintenanceModeListener mmListener)
 {
  // TODO Auto-generated method stub

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
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public File getAttachment(String id, String clustId)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public AgeClass getDefinedAgeClass(String className)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public AgeAttributeClass getDefinedAgeAttributeClass(String className)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public AgeRelationClass getDefinedAgeRelationClass(String className)
 {
  // TODO Auto-generated method stub
  return null;
 }

 @Override
 public AgeRelationClass getCustomAgeRelationClass(String className, ModuleKey modKey)
 {
  // TODO Auto-generated method stub
  return null;
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
 public boolean updateSemanticModel(SemanticModel sm, LogNode log)
 {
  // TODO Auto-generated method stub
  return false;
 }

 @Override
 public void shutdown()
 {
  // TODO Auto-generated method stub

 }

 @Override
 public void lockWrite()
 {
  // TODO Auto-generated method stub

 }

 @Override
 public void unlockWrite()
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

 @Override
 public boolean setMaintenanceMode(boolean mmode)
 {
  // TODO Auto-generated method stub
  return false;
 }

 @Override
 public boolean setMaintenanceMode(boolean mmode, long timeout)
 {
  // TODO Auto-generated method stub
  return false;
 }

 /**
  * @param args
  */
 public static void main(String[] args)
 {
  // TODO Auto-generated method stub

 }

}
