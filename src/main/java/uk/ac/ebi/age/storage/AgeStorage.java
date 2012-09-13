package uk.ac.ebi.age.storage;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;

import uk.ac.ebi.age.model.AgeObject;
import uk.ac.ebi.age.model.DataModule;
import uk.ac.ebi.age.model.ModuleKey;
import uk.ac.ebi.age.model.SemanticModel;
import uk.ac.ebi.age.model.writable.DataModuleWritable;
import uk.ac.ebi.age.query.AgeQuery;
import uk.ac.ebi.age.storage.exeption.IndexIOException;
import uk.ac.ebi.age.storage.index.AttachedSortedTextIndex;
import uk.ac.ebi.age.storage.index.AttachedTextIndex;
import uk.ac.ebi.age.storage.index.KeyExtractor;
import uk.ac.ebi.age.storage.index.TextFieldExtractor;
import uk.ac.ebi.age.storage.index.TextIndex;

public interface AgeStorage
{
 void lockRead();
 void unlockRead();
 
 Iterable<AgeObject> executeQuery( AgeQuery qury );
 

 SemanticModel getSemanticModel();
 
 void shutdown();

 public Iterable<AgeObject> getAllObjects();
 public AgeObject getGlobalObject(String objID);
 public AgeObject getClusterObject(String clustId, String objID);
 AgeObject getObject(String clusterId, String moduleId, String objectId);

// boolean hasObject(String id);
 boolean hasDataModule(String clstId, String id);
 boolean hasDataModule(ModuleKey mk);

 void addDataChangeListener(DataChangeListener dataChangeListener);
 void addMaintenanceModeListener(MaintenanceModeListener mmListener);
 

 DataModule getDataModule(String clstId, String name);

 Collection<? extends DataModule> getDataModules();


 AttachedTextIndex createAttachedTextIndex(String name, AgeQuery qury, Collection<TextFieldExtractor> cb ) throws IndexIOException;
 public <KeyT> AttachedSortedTextIndex<KeyT> createAttachedSortedTextIndex(String name, AgeQuery qury, Collection<TextFieldExtractor> exts,
   KeyExtractor<KeyT> keyExtractor, Comparator<KeyT> comparator) throws IndexIOException;

 TextIndex createTextIndex(String name, AgeQuery qury, Collection<TextFieldExtractor> cb ) throws IndexIOException;

 File getAttachment(String id);
 File getAttachment(String id, String clustId);

 DataModuleWritable getDataModule(ModuleKey modk);

}
