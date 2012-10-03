package uk.ac.ebi.age.storage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import uk.ac.ebi.age.model.AgeObject;
import uk.ac.ebi.age.model.DataModule;
import uk.ac.ebi.age.model.ModuleKey;
import uk.ac.ebi.age.model.writable.AgeExternalObjectAttributeWritable;
import uk.ac.ebi.age.model.writable.AgeExternalRelationWritable;
import uk.ac.ebi.age.model.writable.AgeFileAttributeWritable;
import uk.ac.ebi.age.model.writable.AgeRelationWritable;

import com.pri.util.Pair;

public class ConnectionInfo
{
 private Collection<Pair<AgeExternalObjectAttributeWritable, AgeObject>> objectAttributesReconnection;
 private Collection<Pair<AgeFileAttributeWritable, Boolean>>             fileAttributesResolution;
 private Collection<AgeRelationWritable>                                 relationsRemoval;
 private Collection<AgeExternalRelationWritable>                                 relationsReconnection;
 private Collection<AgeRelationWritable>                                 relationsAttachment;

 public Collection<Pair<AgeExternalObjectAttributeWritable, AgeObject>> getObjectAttributesReconnection()
 {
  return objectAttributesReconnection;
 }

 public void setObjectAttributesReconnection(Collection<Pair<AgeExternalObjectAttributeWritable, AgeObject>> objectAttributesReconnection)
 {
  this.objectAttributesReconnection = objectAttributesReconnection;
 }

 public Collection<Pair<AgeFileAttributeWritable, Boolean>> getFileAttributesResolution()
 {
  return fileAttributesResolution;
 }

 public void setFileAttributesResolution(Collection<Pair<AgeFileAttributeWritable, Boolean>> fileAttributesResolution)
 {
  this.fileAttributesResolution = fileAttributesResolution;
 }

 public Collection<AgeRelationWritable> getRelationsRemoval()
 {
  return relationsRemoval;
 }

 public void setRelationsRemoval(Collection<AgeRelationWritable> relationsRemoval)
 {
  this.relationsRemoval = relationsRemoval;
 }

 public Collection<AgeExternalRelationWritable> getRelationsReconnection()
 {
  return relationsReconnection;
 }

 public void setRelationsReconnection(Collection<AgeExternalRelationWritable> relationsReconnection)
 {
  this.relationsReconnection = relationsReconnection;
 }

 public Collection<AgeRelationWritable> getRelationsAttachment()
 {
  return relationsAttachment;
 }

 public void setRelationsAttachment(Collection<AgeRelationWritable> relationsAttachment)
 {
  this.relationsAttachment = relationsAttachment;
 }

 public Map<String, Set<DataModule>> getGlobalAttachments()
 {
  // TODO Auto-generated method stub
  return null;
 }

 public Map<String, Set<DataModule>> getGlobalDetachments()
 {
  // TODO Auto-generated method stub
  return null;
 }

 public Map<String, Set<DataModule>> getGlobalAttachmentRequests()
 {
  // TODO Auto-generated method stub
  return null;
  
 }

 public Set<ModuleKey> getResetModules()
 {
  // TODO Auto-generated method stub
  return null;
 }
}
