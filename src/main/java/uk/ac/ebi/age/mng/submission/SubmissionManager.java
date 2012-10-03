package uk.ac.ebi.age.mng.submission;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.ebi.age.annotation.AnnotationManager;
import uk.ac.ebi.age.annotation.Topic;
import uk.ac.ebi.age.conf.Constants;
import uk.ac.ebi.age.ext.annotation.AnnotationDBException;
import uk.ac.ebi.age.ext.authz.TagRef;
import uk.ac.ebi.age.ext.entity.AttachmentEntity;
import uk.ac.ebi.age.ext.entity.ClusterEntity;
import uk.ac.ebi.age.ext.log.LogNode;
import uk.ac.ebi.age.ext.log.LogNode.Level;
import uk.ac.ebi.age.ext.submission.DataModuleMeta;
import uk.ac.ebi.age.ext.submission.Factory;
import uk.ac.ebi.age.ext.submission.FileAttachmentMeta;
import uk.ac.ebi.age.ext.submission.Status;
import uk.ac.ebi.age.ext.submission.SubmissionDBException;
import uk.ac.ebi.age.ext.submission.SubmissionMeta;
import uk.ac.ebi.age.model.AgeClass;
import uk.ac.ebi.age.model.AgeObject;
import uk.ac.ebi.age.model.AgeRelation;
import uk.ac.ebi.age.model.AgeRelationClass;
import uk.ac.ebi.age.model.AttributeClassRef;
import uk.ac.ebi.age.model.DataModule;
import uk.ac.ebi.age.model.IdScope;
import uk.ac.ebi.age.model.ModuleKey;
import uk.ac.ebi.age.model.RelationClassRef;
import uk.ac.ebi.age.model.Resolvable;
import uk.ac.ebi.age.model.ResolveScope;
import uk.ac.ebi.age.model.writable.AgeExternalObjectAttributeWritable;
import uk.ac.ebi.age.model.writable.AgeExternalRelationWritable;
import uk.ac.ebi.age.model.writable.AgeFileAttributeWritable;
import uk.ac.ebi.age.model.writable.AgeObjectWritable;
import uk.ac.ebi.age.model.writable.AgeRelationWritable;
import uk.ac.ebi.age.model.writable.DataModuleWritable;
import uk.ac.ebi.age.parser.AgeTab2AgeConverter;
import uk.ac.ebi.age.parser.AgeTabModule;
import uk.ac.ebi.age.parser.AgeTabSyntaxParser;
import uk.ac.ebi.age.parser.ParserException;
import uk.ac.ebi.age.service.id.IdGenerator;
import uk.ac.ebi.age.service.submission.SubmissionDB;
import uk.ac.ebi.age.storage.AgeStorageAdm;
import uk.ac.ebi.age.storage.ConnectionInfo;
import uk.ac.ebi.age.storage.GlobalObjectConnection;
import uk.ac.ebi.age.storage.exeption.AttachmentIOException;
import uk.ac.ebi.age.transaction.Transaction;
import uk.ac.ebi.age.transaction.TransactionException;
import uk.ac.ebi.age.validator.AgeSemanticValidator;
import uk.ac.ebi.age.validator.impl.AgeSemanticValidatorImpl;

import com.pri.util.Extractor;
import com.pri.util.Pair;
import com.pri.util.collection.CollectionMapCollection;
import com.pri.util.collection.CollectionsUnion;
import com.pri.util.collection.ExtractorCollection;
import com.pri.util.stream.StreamPump;

public class SubmissionManager
{
 // private static SubmissionManager instance = new SubmissionManager();

 private static class ModMeta
 {
  DataModuleWritable             origModule;
  DataModuleWritable             newModule;
  DataModuleMeta                 meta;
  ModuleAux                      aux;

  Map<String, AgeObjectWritable> idMap = new HashMap<String, AgeObjectWritable>();
 }

 private static Extractor<ModMeta, DataModuleWritable> modExtractor    = new Extractor<ModMeta, DataModuleWritable>()
   {
    @Override
    public DataModuleWritable extract(ModMeta obj)
    {
     return obj.newModule;
    }
   };

 private static Extractor<ModMeta, ModuleKey>          modkeyExtractor = new Extractor<ModMeta, ModuleKey>()
   {
    @Override
    public ModuleKey extract(ModMeta obj)
    {
     return new ModuleKey(obj.origModule.getClusterId(), obj.origModule.getId());
    }
   };

 private static class FileMeta
 {
  FileAttachmentMeta origFile;
  FileAttachmentMeta newFile;
  AttachmentAux      aux;
 }

 private static class ClustMeta
 {
  List<ModMeta> incomingMods = new ArrayList<SubmissionManager.ModMeta>(); //New modules and modules with data update (Ins+Upd) but in original order
  
  List<ModMeta> mod4Use = new ArrayList<SubmissionManager.ModMeta>(); //Ins+Upd+Hld+MetaUpd convenience map
 
  List<ModMeta> mod4Ins = new ArrayList<SubmissionManager.ModMeta>();
  
  Map<String,ModMeta> mod4MetaUpd = new HashMap<String, SubmissionManager.ModMeta>(); //Modules with meta (description) update only
  Map<String,ModMeta> mod4DataUpd = new HashMap<String, SubmissionManager.ModMeta>(); //Modules with data update
  Map<String,ModMeta> mod4Del = new HashMap<String, SubmissionManager.ModMeta>(); //Modules to be deleted
  Map<String,ModMeta> mod4Hld = new HashMap<String, SubmissionManager.ModMeta>(); //Modules to be retained (fully untouched, even meta)
  Map<String,ModMeta> mod4DataHld = new HashMap<String, SubmissionManager.ModMeta>(); //Modules with data to be retained (mod4Hld+mod4MetaUpd)

  Map<String,FileAttachmentMeta> att4Ins = new HashMap<String, FileAttachmentMeta>(); //New files
  Map<String,FileMeta> att4Upd = new HashMap<String, FileMeta>(); //Files with content update
  Map<String,FileMeta> att4MetaUpd = new HashMap<String, FileMeta>(); //Files with meta update only
  Map<String,FileAttachmentMeta> att4Del = new HashMap<String, FileAttachmentMeta>();
  Map<String,FileMeta> att4G2L = new HashMap<String, FileMeta>(); //Files that reduce visibility
  Map<String,FileMeta> att4L2G = new HashMap<String, FileMeta>(); //Files that increase visibility
  Map<String,FileAttachmentMeta> att4Hld = new HashMap<String, FileAttachmentMeta>(); //Files that keep both content and visibility untouched
  Map<String,FileAttachmentMeta> att4Use = new HashMap<String, FileAttachmentMeta>(); //New full file set

  public String                           id;

  Map<String, AgeObjectWritable>          clusterIdMap   = new HashMap<String, AgeObjectWritable>();
  Map<String, AgeObjectWritable>          newGlobalIdMap = new HashMap<String, AgeObjectWritable>();
  Map<String, AgeObjectWritable>          obsoleteGlobalIdMap = new HashMap<String, AgeObjectWritable>();

  Map<AgeRelationClass, RelationClassRef> relRefMap      = new HashMap<AgeRelationClass, RelationClassRef>();
  
  ConnectionInfo connInfo = new ConnectionInfo();  
 }


 private final AgeTabSyntaxParser   ageTabParser;
 private AgeTab2AgeConverter  converter = null;
 private final AgeSemanticValidator validator = new AgeSemanticValidatorImpl();
 private final AnnotationManager    annotationManager;

 private final SubmissionDB         submissionDB;
 private final AgeStorageAdm        ageStorage;

 public SubmissionManager(AgeStorageAdm ageS, SubmissionDB sDB, AgeTabSyntaxParser prs, AgeTab2AgeConverter conv, AnnotationManager aMngr)
 {
  ageStorage = ageS;
  submissionDB = sDB;
  ageTabParser = prs;
  converter = conv;

  annotationManager = aMngr;
 }

 public boolean storeSubmission(SubmissionMeta sMeta, String updateDescr, LogNode logRoot)
 {
  return storeSubmission(sMeta, updateDescr, logRoot, false);
 }

 @SuppressWarnings("unchecked")
 public boolean storeSubmission(SubmissionMeta sMeta, String updateDescr, LogNode logRoot, boolean verifyOnly)
 {

  SubmissionMeta origSbm = null;

  if(sMeta.getId() != null)
  {
   sMeta.setId(sMeta.getId().trim());

   if(sMeta.getId().length() == 0)
    sMeta.setId(null);
  }

  if(sMeta.getId() != null)
  {
   if(sMeta.getStatus() == Status.NEW)
   {
    try
    {
     if(submissionDB.hasSubmission(sMeta.getId()))
     {
      logRoot.log(Level.ERROR, "Submission with ID='" + sMeta.getId() + "' already exists");
      return false;
     }
    }
    catch(SubmissionDBException e)
    {
     logRoot.log(Level.ERROR, "Method hasSubmission error: " + e.getMessage());

     return false;
    }
   }

   try
   {
    origSbm = submissionDB.getSubmission(sMeta.getId());
   }
   catch(SubmissionDBException e)
   {
    logRoot.log(Level.ERROR, "Method getSubmition error: " + e.getMessage());

    return false;
   }
  }

  if(sMeta.getStatus() == Status.UPDATEORNEW)
  {
   if(sMeta.getId() == null)
    sMeta.setStatus(Status.NEW);
   else
   {
    if(origSbm != null)
     sMeta.setStatus(Status.UPDATE);
    else
     sMeta.setStatus(Status.NEW);
   }
  }

  if(sMeta.getStatus() == Status.UPDATE)
  {
   if(sMeta.getId() == null)
   {
    logRoot.log(Level.ERROR, "Submission is marked for update but no ID is provided");
    return false;
   }

   if(origSbm == null)
   {
    logRoot.log(Level.ERROR, "Submission with ID='" + sMeta.getId() + "' is not found to be updated");
    return false;
   }

   sMeta.setSubmitter(origSbm.getSubmitter());
   sMeta.setSubmissionTime(origSbm.getSubmissionTime());

   if(sMeta.getDescription() == null)
    sMeta.setDescription(origSbm.getDescription());
  }

  char[] tmpId = new char[] { '\0', '\0' };

  ClustMeta clusterMeta = new ClustMeta();

  if(sMeta.getId() == null)
   clusterMeta.id = new String(tmpId);
  else
   clusterMeta.id = sMeta.getId();

  List<FileAttachmentMeta> files = sMeta.getAttachments();

  boolean res = true;

  int n = 0;

  if(sMeta.getDataModules() != null)
  {
   for(DataModuleMeta dm : sMeta.getDataModules()) // Separating modules
   {

    n++;

    ModuleAux modAux = (ModuleAux) dm.getAux();

    DataModuleWritable exstMod = dm.getId() != null && sMeta.getStatus() == Status.UPDATE ? ageStorage.getDataModule(new ModuleKey(sMeta.getId(), dm.getId()) ) : null;

    if(modAux.getStatus() == Status.UPDATEORNEW)
    {
     if(exstMod == null)
      modAux.setStatus(Status.NEW);
     else
      modAux.setStatus(Status.UPDATE);
    }

    if(modAux.getStatus() == Status.UPDATE || modAux.getStatus() == Status.DELETE)
    {
     if(sMeta.getStatus() != Status.UPDATE)
     {
      logRoot.log(Level.ERROR, "Module " + modAux.getOrder() + " is marked for " + modAux.getStatus().name() + " but submission is not in UPDATE mode");
      res = false;
      continue;
     }

     if(exstMod == null)
     {
      logRoot.log(Level.ERROR, "The storage doesn't contain data module with ID='" + dm.getId() + "' (Cluster ID='" + sMeta.getId()
        + "') to be updated/deleted");
      res = false;
     }

     if(dm.getId() == null)
     {
      logRoot.log(Level.ERROR, "Module " + modAux.getOrder() + " is marked for " + modAux.getStatus().name() + " but no ID is provided");
      res = false;
      continue;
     }

     ModMeta mm = new ModMeta();
     mm.meta = dm;
     mm.aux = modAux;

     mm.origModule = exstMod;

     if(mm.aux.getStatus() == Status.DELETE)
     {
      clusterMeta.mod4Del.put(mm.meta.getId(), mm);
     }
     else if(dm.getText() != null)
     {
      clusterMeta.mod4DataUpd.put(mm.meta.getId(), mm);
      clusterMeta.mod4Use.add(mm);
      clusterMeta.incomingMods.add(mm);
     }
     else
     {
      clusterMeta.mod4MetaUpd.put(mm.meta.getId(), mm);
      clusterMeta.mod4DataHld.put(mm.meta.getId(), mm);
      clusterMeta.mod4Use.add(mm);
     }
    }
    else // modAux.getStatus() == Status.NEW
    {
     if(dm.getText() == null)
     {
      logRoot.log(Level.ERROR, "Module " + modAux.getOrder() + " is marked for insertion but no data were provided");
      res = false;
      continue;
     }

     if(dm.getId() != null)
     {

      if(exstMod != null)
      {
       logRoot.log(Level.ERROR, "Module " + modAux.getOrder() + " is marked for insertion and has it's own ID (" + dm.getId()
         + ") but this ID it already taken by module of cluster '" + exstMod.getClusterId() + "'");
       res = false;
       continue;
      }
     }
     else
     {
      tmpId[1] = (char) modAux.getOrder();
      dm.setId(new String(tmpId));
     }

     ModMeta mm = new ModMeta();
     mm.meta = dm;
     mm.aux = modAux;

     mm.meta.setDocVersion(mm.meta.getModificationTime());

     clusterMeta.mod4Ins.add(mm);
     clusterMeta.mod4Use.add(mm);

     clusterMeta.incomingMods.add(mm);
    }
   }
  }

  if(origSbm != null && origSbm.getDataModules() != null) // now we are sorting modules from the existing cluster (submission)
  {
   for(DataModuleMeta odm : origSbm.getDataModules())
   {
    String modID = odm.getId();

    ModMeta updMod = clusterMeta.mod4DataUpd.get(modID);

    if(updMod == null)
     updMod = clusterMeta.mod4MetaUpd.get(modID);

    if(updMod != null)
    {
     if(updMod.meta.getDescription() == null) // if the new module has no description we keep the old one
      updMod.meta.setDescription(odm.getDescription());

     updMod.meta.setSubmissionTime(odm.getSubmissionTime()); // Preserving original submitter and submission time
     updMod.meta.setSubmitter(odm.getSubmitter());

     if(updMod.meta.getText() != null)
      updMod.meta.setDocVersion(updMod.meta.getModificationTime());
     else
      updMod.meta.setDocVersion(odm.getDocVersion());

    }
    else if(!clusterMeta.mod4Del.containsKey(modID)) // i.e. module that will be kept untouched
    {
     ModMeta mm = new ModMeta();
     mm.meta = odm;
     mm.origModule = ageStorage.getDataModule( new ModuleKey(clusterMeta.id, modID) );

     if(mm.origModule == null)
     {
      logRoot.log(Level.ERROR, "Module '" + modID
        + "' is in the submission db but not in the graph. It means data inconsistency. Please contact system administrator");

      res = false;
      continue;
     }

     clusterMeta.mod4Use.add(mm);
     clusterMeta.mod4Hld.put(modID, mm);
     clusterMeta.mod4DataHld.put(modID, mm);
    }

   }
  }

  if(!res)
   return false;

  Map<String, Integer> globalFileConflicts = null; // = new HashMap<String,Integer>();

  if(files != null && files.size() > 0) // Sorting incoming files
  {
   LogNode fileNode = logRoot.branch("Preparing attachments");

   for(n = 0; n < files.size(); n++)
   {
    FileAttachmentMeta newFileMeta = files.get(n);
    AttachmentAux newAuxInfo = (AttachmentAux) newFileMeta.getAux();

    String cAtId = newAuxInfo.getNewId() != null ? newAuxInfo.getNewId() : newFileMeta.getId(); // atax.getNewId()!=null meant that we want to assign the new ID to some attachment

    if(cAtId == null)
    {
     fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " has empty ID");
     res = false;
     continue;
    }

    if(newAuxInfo.getStatus() != Status.DELETE)
    {
     for(int k = n + 1; k < files.size(); k++) // All IDs must be unique within the submission
     {
      FileAttachmentMeta ofa = files.get(k);

      AttachmentAux cmpax = (AttachmentAux) ofa.getAux();

      if(cmpax.getStatus() == Status.DELETE)
       continue;

      String cmpAtId = cmpax.getNewId() != null ? cmpax.getNewId() : ofa.getId();

      if(cmpAtId.equals(cAtId))
      {
       fileNode.log(Level.ERROR, "File ID (" + cmpAtId + ") conflict. Files: " + newAuxInfo.getOrder() + " and " + cmpax.getOrder());
       res = false;
       continue;
      }
     }
    }

    FileAttachmentMeta origFileMeta = null;

    if(origSbm != null && origSbm.getAttachments() != null)
    {
     for(FileAttachmentMeta ofa : origSbm.getAttachments())
     {
      if(newFileMeta.getId().equals(ofa.getId()))
      {
       origFileMeta = ofa;
       break;
      }
     }
    }

    if(newAuxInfo.getStatus() == Status.UPDATEORNEW)
    {
     if(origFileMeta != null) // OR ageStorage.getAttachment(fm.getId(),
                              // cstMeta.id) != null
      newAuxInfo.setStatus(Status.UPDATE);
     else
      newAuxInfo.setStatus(Status.NEW);
    }

    if(newAuxInfo.getStatus() == Status.DELETE || newAuxInfo.getStatus() == Status.UPDATE)
    {
     if(origSbm == null) // No original submission. This means a new submission
     {
      fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " is marked for update/deletion but submission is not in UPDATE mode");
      res = false;
      continue;
     }

     if(origFileMeta == null)
     {
      fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " is marked for update/deletion but it doesn't exist within the submission");
      res = false;
      continue;
     }

     // fm.setSystemId(origFm.getSystemId());

     if(newFileMeta.getDescription() == null)
      newFileMeta.setDescription(origFileMeta.getDescription());

     newFileMeta.setSubmitter(origFileMeta.getSubmitter());
     newFileMeta.setSubmissionTime(origFileMeta.getSubmissionTime());

     if(newAuxInfo.getStatus() == Status.DELETE)
     {
      if(origFileMeta.isGlobal())
      {
       if(globalFileConflicts == null)
        globalFileConflicts = new HashMap<String, Integer>();

       globalFileConflicts.put(newFileMeta.getId(), -1);
      }

      newFileMeta.setGlobal(origFileMeta.isGlobal());
      clusterMeta.att4Del.put(newFileMeta.getId(), newFileMeta);
     }
     else
     // UPDATE
     {
      if(newAuxInfo.getNewId() != null && !newAuxInfo.getNewId().equals(newFileMeta.getId())) // Submitter wants to rename this attachment
      {

       // We have checked that all IDs are unique within this submission let's check conflicts with the global IDs

       if(newFileMeta.isGlobal())
       {
        if(ageStorage.getAttachment(newAuxInfo.getNewId()) != null) // ok, it could be a problem if it not some attachment that we a going to delete
        {
         if(globalFileConflicts == null)
          globalFileConflicts = new HashMap<String, Integer>();

         if(!globalFileConflicts.containsKey(newAuxInfo.getNewId()))
          globalFileConflicts.put(newAuxInfo.getNewId(), newAuxInfo.getOrder());

         // fileNode.log(Level.ERROR, "File " + newAuxInfo.getNewId() +
         // " has global ID but this ID is already taken");
         // res = false;
         // continue;
        }
       }

       // To do renaming we will simulate insertion/deletion

       FileAttachmentMeta nfm = Factory.createFileAttachmentMeta();
       AttachmentAux nax = new AttachmentAux();

       nfm.setAux(nax);
       nax.setStatus(Status.NEW);
       nfm.setId(newAuxInfo.getNewId());
       nax.setOrder(newAuxInfo.getOrder());
       nax.setFile(newAuxInfo.getFile() != null ? newAuxInfo.getFile() : submissionDB.getAttachment(origSbm.getId(), origFileMeta.getId(),
         origFileMeta.getModificationTime()));

       nfm.setSubmitter(origFileMeta.getSubmitter());
       nfm.setModifier(newFileMeta.getModifier());
       nfm.setSubmissionTime(origFileMeta.getSubmissionTime());
       nfm.setModificationTime(newFileMeta.getModificationTime());
       nfm.setDescription(newFileMeta.getDescription());
       nfm.setGlobal(newFileMeta.isGlobal());

       clusterMeta.att4Ins.put(nfm.getId(), nfm);
       clusterMeta.att4Use.put(nfm.getId(), nfm);

       newAuxInfo.setNewId(null);
       newAuxInfo.setStatus(Status.DELETE);

       newFileMeta.setGlobal(origFileMeta.isGlobal());

       clusterMeta.att4Del.put(newFileMeta.getId(), newFileMeta);
      }
      else
      // UPDATE not renaming
      {
       FileMeta fmeta = new FileMeta(); // Our local structure to keep attachment info together

       fmeta.newFile = newFileMeta;
       fmeta.origFile = origFileMeta;
       fmeta.aux = newAuxInfo;

       if(newAuxInfo.getFile() != null)
        clusterMeta.att4Upd.put(newFileMeta.getId(), fmeta);
       else
       {
        clusterMeta.att4MetaUpd.put(newFileMeta.getId(), fmeta);
        newFileMeta.setFileVersion(origFileMeta.getFileVersion());
       }

       clusterMeta.att4Use.put(newFileMeta.getId(), newFileMeta);

       if(newFileMeta.isGlobal() != origFileMeta.isGlobal())
       {
        if(newFileMeta.isGlobal())
         clusterMeta.att4L2G.put(newFileMeta.getId(), fmeta);
        else
         clusterMeta.att4G2L.put(newFileMeta.getId(), fmeta);
       }

      }

     }
    }
    else if(newAuxInfo.getStatus() == Status.NEW)
    {
     if(newAuxInfo.getFile() == null)
     {
      fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " is marked as NEW but contains no data");
      res = false;
      continue;
     }

     if(origFileMeta != null)
     {
      fileNode.log(Level.ERROR, "File " + newAuxInfo.getOrder() + " is marked as NEW but file with the same ID already exists");
      res = false;
      continue;
     }

     if(newFileMeta.isGlobal()) // this is a new file with a new global ID. We have to check its uniqueness
     {

      if(ageStorage.getAttachment(newFileMeta.getId()) != null) // ok, it could be a problem if it not attachment that we are going to delete
      {
       if(globalFileConflicts == null)
        globalFileConflicts = new HashMap<String, Integer>();

       if(!globalFileConflicts.containsKey(newFileMeta.getId()))
        globalFileConflicts.put(newFileMeta.getId(), newAuxInfo.getOrder());
      }

      // fm.setSystemId(gid);
     }

     clusterMeta.att4Ins.put(newFileMeta.getId(), newFileMeta);
     clusterMeta.att4Use.put(newFileMeta.getId(), newFileMeta);

    }

   }

   if(globalFileConflicts != null)
   {
    for(Map.Entry<String, Integer> me : globalFileConflicts.entrySet())
    {
     if(me.getValue() != -1)
     {
      fileNode.log(Level.ERROR, "File " + me.getValue() + " has global ID='" + me.getKey() + "' but this ID is already taken");
      res = false;
     }
    }

    globalFileConflicts = null;
   }

   if(res)
    fileNode.success();

  }

  if(origSbm != null && origSbm.getAttachments() != null)
  {
   for(FileAttachmentMeta fm : origSbm.getAttachments())
   {
    if(!clusterMeta.att4Del.containsKey(fm.getId()) && !clusterMeta.att4Upd.containsKey(fm.getId()) && !clusterMeta.att4MetaUpd.containsKey(fm.getId()))
    {
     clusterMeta.att4Hld.put(fm.getDescription(), fm);
     clusterMeta.att4Use.put(fm.getId(), fm);
    }
   }
  }

  for(n = 0; n < clusterMeta.incomingMods.size(); n++)
  {
   ModMeta mm = clusterMeta.incomingMods.get(n);

   boolean modRes = true;
   LogNode modNode = logRoot.branch("Processing module: " + mm.aux.getOrder());

   if(mm.meta.getText() == null) // Modules to be deleted or meta update
    continue;

   LogNode atLog = modNode.branch("Parsing AgeTab");

   AgeTabModule atMod = null;
   try
   {
    atMod = ageTabParser.parse(mm.meta.getText());

    atLog.success();
   }
   catch(ParserException e)
   {
    atLog.log(Level.ERROR, "Parsing failed: " + e.getMessage() + ". Row: " + e.getLineNumber() + ". Col: " + e.getColumnNumber());
    res = false;
    continue;
   }

   LogNode convLog = modNode.branch("Converting AgeTab to Age data module");
   mm.newModule = converter.convert(atMod, ageStorage.getSemanticModel().createContextSemanticModel(), convLog);

   if(mm.newModule != null)
   {
    ModuleKey modk = new ModuleKey(clusterMeta.id, mm.meta.getId());
    mm.newModule.setModuleKey(modk);

    convLog.success();
   }
   else
   {
    convLog.log(Level.ERROR, "Conversion failed");
    modRes = false;
   }

   if(modRes)
    modNode.success();
   else
    mm.newModule = null;

   res = res && modRes;
  }

  if(!res)
   return false;

  try
  {
   ageStorage.lockWrite();
   // XXX storeSubmission: connection to the main graph

   if(!checkUniqObjects(clusterMeta, logRoot))
   {
    res = false;
    return false;
   }

   LogNode connLog = logRoot.branch("Connecting data module" + (clusterMeta.incomingMods.size() > 1 ? "s" : "") + " to the main graph");

   ConnectionInfo connectionInfo = new ConnectionInfo();
   
   
   Collection<Pair<AgeExternalObjectAttributeWritable, AgeObject>> extAttrConnector = new ArrayList<Pair<AgeExternalObjectAttributeWritable, AgeObject>>();
   Collection< AgeExternalRelationWritable > relConnections = null;
   Map<AgeObjectWritable, Set<AgeRelationWritable>> relationDetachMap = null;

   connectionInfo.setObjectAttributesReconnection(extAttrConnector);

   if(clusterMeta.mod4DataUpd.size() != 0 || clusterMeta.mod4Ins.size() != 0 || clusterMeta.mod4Del.size() != 0)
   {
    relConnections = new ArrayList< AgeExternalRelationWritable >();
    relationDetachMap = new HashMap<AgeObjectWritable, Set<AgeRelationWritable>>();

    connectionInfo.setRelationsRemoval( new com.pri.util.collection.CollectionMapCollection<AgeRelationWritable>(relationDetachMap) );
    connectionInfo.setRelationsReconnection( relConnections );
    
    if(!reconnectExternalObjectAttributes(clusterMeta, extAttrConnector, logRoot))
    {
     res = false;
     return false;
    }

    if(!reconnectExternalRelations(clusterMeta, relConnections, relationDetachMap, logRoot))
    {
     res = false;
     return false;
    }
   }

   Map<AgeObjectWritable, Set<AgeRelationWritable>> invRelMap = new HashMap<AgeObjectWritable, Set<AgeRelationWritable>>();
   // invRelMap contains a map of external objects to sets of prepared inverse
   // relations for new external relations

   connectionInfo.setRelationsAttachment( new com.pri.util.collection.CollectionMapCollection<AgeRelationWritable>(invRelMap) );
   
   if(!connectNewExternalRelations(clusterMeta, invRelMap, logRoot))
   {
    res = false;
    return false;
   }

   if(!connectNewObjectAttributes(clusterMeta, logRoot))
   {
    res = false;
    return false;
   }

   if( !resolveIncomingFileAttributes(clusterMeta, logRoot) )
   {
    res = false;
    return false;
   }

   
   Collection<Pair<AgeFileAttributeWritable, Boolean>> fileConn = new ArrayList<Pair<AgeFileAttributeWritable, Boolean>>();

   
   if(clusterMeta.att4Del.size() != 0 || clusterMeta.att4G2L.size() != 0)
   {
    if(!checkExternalFileConnections(clusterMeta, logRoot) || ! reconnectLocalModulesToFiles(clusterMeta, fileConn, logRoot) )
    {
     res = false;
     return false;
    }

   }


   LogNode semLog = logRoot.branch("Validating semantic");

   boolean vldRes = true;
   n = 0;
   for(ModMeta mm : clusterMeta.incomingMods)
   {
    n++;

    if(mm.newModule == null)
     continue;

    LogNode vldLog = semLog.branch("Processing module: " + n);

    boolean modRes = validator.validate(mm.newModule, vldLog);

    if(modRes)
     vldLog.success();

    vldRes = vldRes && modRes;
   }

   if(vldRes)
    semLog.success();

   res = res && vldRes;

   Set<AgeObject> affObjSet = new HashSet<AgeObject>();

   if(invRelMap != null)
    affObjSet.addAll(invRelMap.keySet());

   if(relationDetachMap != null)
    affObjSet.addAll(relationDetachMap.keySet());

   if(affObjSet.size() > 0)
   {
    boolean invRelRes = true;
    LogNode invRelLog = logRoot.branch("Validating externaly related object semantic");

    Set<AgeRelation> relSet = new HashSet<AgeRelation>();

    for(AgeObject obj : affObjSet)
    {
     LogNode objLogNode = invRelLog.branch("Validating object Id: " + obj.getId() + " Class: " + obj.getAgeElClass());

     Collection< ? extends AgeRelation> relColl = null;

     Collection<AgeRelationWritable> inv = invRelMap != null ? invRelMap.get(obj) : null;
     Collection<AgeRelationWritable> det = relationDetachMap != null ? relationDetachMap.get(obj) : null;

     if(inv == null && det == null)
      relColl = obj.getRelations();
     else
     {
      relSet.clear();

      relSet.addAll(obj.getRelations());

      if(inv != null)
       relSet.addAll(inv);

      if(det != null)
       relSet.removeAll(det);

      relColl = relSet;
     }

     if(validator.validateRelations(obj.getAgeElClass(), relColl, objLogNode))
      objLogNode.success();
     else
      invRelRes = false;
    }

    if(invRelRes)
     invRelLog.success();
    else
     invRelRes = false;

    res = res && invRelRes;
   }

   if(!res)
    return false;
   
   if(verifyOnly)
    return true;


   if(clusterMeta.id.charAt(0) == '\0')
   {
    String id = null;

    try
    {
     do
     {
      id = Constants.submissionIDPrefix + IdGenerator.getInstance().getStringId(Constants.clusterIDDomain);
     } while(submissionDB.hasSubmission(id));
    }
    catch(SubmissionDBException e)
    {
     logRoot.log(Level.ERROR, "Method hasSubmission error: " + e.getMessage());

     res = false;

     return false;
    }

    clusterMeta.id = id;
    sMeta.setId(id);
   }

   for(ModMeta mm : clusterMeta.incomingMods)
   {
    ModuleKey mk = new ModuleKey();

    mk.setClusterId(clusterMeta.id);

    mm.newModule.setModuleKey(mk);

    if(mm.origModule != null)
     mk.setModuleId(mm.origModule.getId());
    else if(mm.meta.getId() != null && mm.meta.getId().charAt(0) != '\0')
     mk.setModuleId(mm.meta.getId());
    else
    {
     String id = null;

     do
     {
      id = Constants.dataModuleIDPrefix + IdGenerator.getInstance().getStringId(Constants.dataModuleIDDomain);
      mk.setModuleId(id);
     } while(ageStorage.hasDataModule(mk));

     mm.meta.setId(id);

    }

    for(AgeObjectWritable obj : mm.newModule.getObjects())
    {
     if(obj.getId() == null)
     {
      String id = null;

      do
      {
       id = Constants.localObjectIDPrefix + obj.getAgeElClass().getIdPrefix() + IdGenerator.getInstance().getStringId(Constants.objectIDDomain) + "@"
         + mm.newModule.getId();
      } while(mm.idMap.containsKey(id));

      obj.setId(id);
      mm.idMap.put(id, obj);
     }
    }
   }

   if(clusterMeta.att4Ins.size() > 0)
   {
    LogNode finsLog = logRoot.branch("Storing files");

    for(FileAttachmentMeta fam : clusterMeta.att4Ins.values())
    {
     finsLog.log(Level.INFO, "Storing file: '" + fam.getId() + "' (scope " + (fam.isGlobal() ? "global" : "cluster") + ")");

     try
     {
      fam.setFileVersion(fam.getModificationTime());

      File tagt = ageStorage.storeAttachment(fam.getId(), clusterMeta.id, fam.isGlobal(), ((AttachmentAux) fam.getAux()).getFile());

      submissionDB.storeAttachment(clusterMeta.id, fam.getId(), fam.getFileVersion(), tagt);
     }
     catch(Exception e)
     {
      finsLog.log(Level.ERROR, e.getMessage());

      res = false;
      return false;
     }

     finsLog.success();
    }
   }

   if(clusterMeta.att4Del.size() > 0)
   {
    LogNode fdelLog = logRoot.branch("Deleting files");

    boolean delRes = true;

    for(FileAttachmentMeta fam : clusterMeta.att4Del.values())
    {
     fdelLog.log(Level.INFO, "Deleting file: '" + fam.getId() + "' (scope " + (fam.isGlobal() ? "global" : "cluster") + ")");

     if(!ageStorage.deleteAttachment(fam.getId(), clusterMeta.id, fam.isGlobal()))
     {
      fdelLog.log(Level.WARN, "File wasn't deleted or doesn't exist");
      delRes = false;
     }

    }

    if(delRes)
     fdelLog.success();

   }

   if(clusterMeta.att4Upd.size() > 0)
   {
    LogNode fupdLog = logRoot.branch("Updating files");

    for(FileMeta fam : clusterMeta.att4Upd.values())
    {
     fupdLog.log(Level.INFO, "Updating file: '" + fam.origFile.getId() + "' (scope " + (fam.newFile.isGlobal() ? "global" : "cluster") + ")");

     try
     {
      fam.newFile.setFileVersion(fam.newFile.getModificationTime());

      File tagt = ageStorage.storeAttachment(fam.origFile.getId(), clusterMeta.id, fam.origFile.isGlobal(),
        ((AttachmentAux) fam.newFile.getAux()).getFile());

      submissionDB.storeAttachment(clusterMeta.id, fam.newFile.getId(), fam.newFile.getFileVersion(), tagt);
     }
     catch(Exception e)
     {
      fupdLog.log(Level.ERROR, e.getMessage());

      res = false;
      return false;
     }

     fupdLog.success();
    }
   }

   if(clusterMeta.att4G2L.size() > 0)
   {
    LogNode fupdLog = logRoot.branch("Changing file visibility scope (global to local)");

    for(FileMeta fam : clusterMeta.att4G2L.values())
    {
     fupdLog.log(Level.INFO, "Processing file '" + fam.origFile.getId() + "'");

     try
     {
      ageStorage.changeAttachmentScope(fam.origFile.getId(), clusterMeta.id, fam.newFile.isGlobal());
     }
     catch(AttachmentIOException e)
     {
      fupdLog.log(Level.ERROR, e.getMessage());

      res = false;
      return false;
     }

     fupdLog.success();
    }
   }

   if(clusterMeta.att4L2G.size() > 0)
   {
    LogNode fupdLog = logRoot.branch("Changing file visibility scope (local to global)");

    for(FileMeta fam : clusterMeta.att4L2G.values())
    {
     fupdLog.log(Level.INFO, "Processing file '" + fam.origFile.getId() + "'");

     try
     {
      ageStorage.changeAttachmentScope(fam.origFile.getId(), clusterMeta.id, fam.newFile.isGlobal());
     }
     catch(AttachmentIOException e)
     {
      fupdLog.log(Level.ERROR, e.getMessage());

      res = false;
      return false;
     }

     fupdLog.success();
    }
   }

   if(clusterMeta.mod4DataUpd.size() > 0 || clusterMeta.mod4Del.size() > 0 || clusterMeta.mod4Ins.size() > 0)
   {
    LogNode updtLog = logRoot.branch("Updating storage");

    try
    {

     Collection<DataModuleWritable> chgMods = new CollectionsUnion<DataModuleWritable>(new ExtractorCollection<ModMeta, DataModuleWritable>(
       clusterMeta.mod4DataUpd.values(), modExtractor), new ExtractorCollection<ModMeta, DataModuleWritable>(clusterMeta.mod4Ins, modExtractor));

     for(DataModuleWritable mod : chgMods)
      mod.pack();

     ageStorage.update(chgMods,
     new CollectionsUnion<ModuleKey>(new ExtractorCollection<ModMeta, ModuleKey>(clusterMeta.mod4DataUpd.values(), modkeyExtractor),
       new ExtractorCollection<ModMeta, ModuleKey>(clusterMeta.mod4Del.values(), modkeyExtractor)),
     connectionInfo);

     updtLog.success();
    }
    catch(Exception e)
    {
     e.printStackTrace();
     updtLog.log(Level.ERROR, e.getMessage() != null ? e.getMessage() : "Exception: " + e.getClass().getName());

     res = false;

     return false;
    }
   }

   SubmissionMeta newSMeta = Factory.createSubmissionMeta();

   newSMeta.setId(sMeta.getId());
   newSMeta.setDescription(sMeta.getDescription());
   newSMeta.setRemoved(false);
   newSMeta.setSubmitter(sMeta.getSubmitter());
   newSMeta.setModifier(sMeta.getModifier());
   newSMeta.setSubmissionTime(sMeta.getSubmissionTime());
   newSMeta.setModificationTime(sMeta.getModificationTime());

   for(ModMeta dmm : clusterMeta.mod4Use)
    newSMeta.addDataModule(dmm.meta);

   for(FileAttachmentMeta fam : clusterMeta.att4Use.values())
    newSMeta.addAttachment(fam);

   try
   {
    submissionDB.storeSubmission(newSMeta, origSbm, updateDescr);
   }
   catch(SubmissionDBException e)
   {
    logRoot.log(Level.ERROR, "Method storeSubmission error: " + e.getMessage());

    res = false;

    return false;
   }

 /*
   if(extAttrConnector != null)
   {
    for(Pair<AgeExternalObjectAttributeWritable, AgeObject> cn : extAttrConnector)
     cn.getFirst().setTargetObject(cn.getSecond());
   }

   if(relConnections != null)
   {
    for(Pair<AgeExternalRelationWritable, AgeObjectWritable> cn : relConnections)
     cn.getFirst().setTargetObject(cn.getSecond());
   }

   for(Pair<AgeFileAttributeWritable, String> fc : fileConn)
    fc.getFirst().setFileSysRef(fc.getSecond());

   if(relationDetachMap != null)
   {
    for(Map.Entry<AgeObjectWritable, Set<AgeRelationWritable>> me : relationDetachMap.entrySet())
     for(AgeRelationWritable rel : me.getValue())
      me.getKey().removeRelation(rel);
   }
   // stor.removeRelations(me.getKey().getId(),me.getValue());

   for(Map.Entry<AgeObjectWritable, Set<AgeRelationWritable>> me : invRelMap.entrySet())
    for(AgeRelationWritable rel : me.getValue())
     me.getKey().addRelation(rel);

   for(ModMeta dm : clusterMeta.incomingMods)
   {
    if(dm.newModule != null && dm.newModule.getExternalRelations() != null)
    {
     for(AgeExternalRelationWritable rel : dm.newModule.getExternalRelations())
      rel.getInverseRelation().setInverseRelation(rel);
    }
   }
   // stor.addRelations(me.getKey().getId(),me.getValue());
 */

  }
  finally
  {
   ageStorage.unlockWrite();
  }

  Transaction trn = annotationManager.startTransaction();

  ClusterEntity cEnt = new ClusterEntity(sMeta.getId());

  try
  {
   if(sMeta.getStatus() == Status.NEW)
    annotationManager.addAnnotation(trn, Topic.OWNER, cEnt, sMeta.getModifier());
   else
   {
    for(ModMeta mm : clusterMeta.mod4Del.values())
     annotationManager.removeAnnotation(trn, Topic.OWNER, mm.newModule, true);

    AttachmentEntity ate = new AttachmentEntity(cEnt, null);

    for(FileAttachmentMeta fatm : clusterMeta.att4Del.values())
    {
     ate.setEntityId(fatm.getId());
     annotationManager.removeAnnotation(trn, Topic.OWNER, ate, true);
    }

    String cstOwner = (String) annotationManager.getAnnotation(trn, Topic.OWNER, cEnt, false);

    if(!sMeta.getModifier().equals(cstOwner))
    {
     for(ModMeta mm : clusterMeta.mod4Ins)
      annotationManager.addAnnotation(trn, Topic.OWNER, mm.newModule, sMeta.getModifier());

     for(FileAttachmentMeta fatm : clusterMeta.att4Ins.values())
     {
      ate.setEntityId(fatm.getId());
      annotationManager.addAnnotation(trn, Topic.OWNER, ate, sMeta.getModifier());
     }
    }

   }

   if(sMeta.getTags() != null)
   {
    List<TagRef> tgs = sMeta.getTags();

    if(!(tgs instanceof ArrayList))
    {
     tgs = new ArrayList<TagRef>(tgs.size());
     tgs.addAll(sMeta.getTags());
    }

    Collections.sort(tgs);

    annotationManager.addAnnotation(trn, Topic.TAG, cEnt, (Serializable) tgs);
   }

   for(ModMeta mm : clusterMeta.incomingMods)
   {
    if(mm.meta.getTags() != null)
    {
     List<TagRef> tgs = mm.meta.getTags();

     if(!(tgs instanceof ArrayList))
     {
      tgs = new ArrayList<TagRef>(tgs.size());
      tgs.addAll(mm.meta.getTags());
     }

     Collections.sort(tgs);

     annotationManager.addAnnotation(trn, Topic.TAG, mm.newModule, (Serializable) tgs);
    }
   }

   if(sMeta.getAttachments() != null)
   {
    AttachmentEntity ate = new AttachmentEntity(cEnt, null);

    for(FileAttachmentMeta att : sMeta.getAttachments())
    {
     List<TagRef> tgs = att.getTags();

     if(tgs != null)
     {
      if(!(tgs instanceof ArrayList))
      {
       tgs = new ArrayList<TagRef>(tgs.size());
       tgs.addAll(att.getTags());
      }

      Collections.sort(tgs);

      ate.setEntityId(att.getId());

      annotationManager.addAnnotation(trn, Topic.TAG, ate, (Serializable) tgs);
     }
    }
   }

  }
  catch(AnnotationDBException e)
  {
   try
   {
    annotationManager.rollbackTransaction(trn);
   }
   catch(TransactionException e1)
   {
    e1.printStackTrace();
   }

   trn = null;
  }
  finally
  {
   if(trn != null)
   {
    try
    {
     annotationManager.commitTransaction(trn);
    }
    catch(TransactionException e)
    {
     e.printStackTrace();
    }
   }
  }

  // Impute reverse relation and revalidate.

  return res;
 }

 public boolean restoreSubmission(String sbmID, LogNode logRoot)
 {

  SubmissionMeta sMeta = null;

  try
  {
   sMeta = submissionDB.getSubmission(sbmID);
  }
  catch(SubmissionDBException e)
  {
   logRoot.log(Level.ERROR, "Method restoreSubmission error: " + e.getMessage());

   return false;
  }

  if(sMeta == null)
  {
   logRoot.log(Level.ERROR, "Submission with ID='" + sbmID + "' is not found to be restored");
   return false;
  }

  ClustMeta cstMeta = new ClustMeta();
  cstMeta.id = sbmID;

  int n = 0;
  if(sMeta.getDataModules() != null)
  {
   for(DataModuleMeta dmm : sMeta.getDataModules())
   {
    n++;

    ModMeta mm = new ModMeta();
    ModuleAux maux = new ModuleAux();
    maux.setOrder(n);

    mm.meta = dmm;
    mm.aux = maux;
    dmm.setAux(maux);

    cstMeta.mod4Ins.add(mm);
    cstMeta.mod4Use.add(mm);

    cstMeta.incomingMods.add(mm);
   }
  }

  if(sMeta.getAttachments() != null)
  {
   for(FileAttachmentMeta fatt : sMeta.getAttachments())
   {
    cstMeta.att4Ins.put(fatt.getId(), fatt);
    cstMeta.att4Use.put(fatt.getId(), fatt);
   }
  }

  boolean res = true;

  for(n = 0; n < cstMeta.incomingMods.size(); n++)
  {
   ModMeta mm = cstMeta.incomingMods.get(n);

   boolean modRes = true;
   LogNode modNode = logRoot.branch("Processing module: " + mm.meta.getId());

   File modFile = submissionDB.getDocument(cstMeta.id, mm.meta.getId(), mm.meta.getDocVersion());

   if(modFile == null)
   {
    modNode.log(Level.ERROR, "File for module " + mm.meta.getId() + " is not found");
    modRes = false;
   }

   ByteArrayOutputStream bais = new ByteArrayOutputStream();

   try
   {
    FileInputStream fis = new FileInputStream(modFile);
    StreamPump.doPump(fis, bais, false);
    fis.close();

    bais.close();

   }
   catch(IOException e)
   {
    modNode.log(Level.ERROR, "File read error. " + e.getMessage());
    res = false;
   }

   byte[] barr = bais.toByteArray();
   String enc = "UTF-8";

   if(barr.length >= 2 && (barr[0] == -1 && barr[1] == -2) || (barr[0] == -2 && barr[1] == -1))
    enc = "UTF-16";

   try
   {
    mm.meta.setText(new String(bais.toByteArray(), enc));
   }
   catch(UnsupportedEncodingException e1)
   {
   }

   AgeTabModule atMod = null;

   LogNode atLog = modNode.branch("Parsing AgeTab");
   try
   {
    atMod = ageTabParser.parse(mm.meta.getText());
    atLog.success();
   }
   catch(ParserException e)
   {
    atLog.log(Level.ERROR, "Parsing failed: " + e.getMessage() + ". Row: " + e.getLineNumber() + ". Col: " + e.getColumnNumber());
    res = false;
    continue;
   }

   LogNode convLog = modNode.branch("Converting AgeTab to Age data module");
   mm.newModule = converter.convert(atMod, ageStorage.getSemanticModel().createContextSemanticModel(), convLog);

   if(mm.newModule != null)
    convLog.success();
   else
    modRes = false;

   if(modRes)
    modNode.success();
   else
    mm.newModule = null;

   res = res && modRes;
  }

  if(!res)
   return false;

  try
  {
   ageStorage.lockWrite();

   // XXX connection to main graph

   if(!checkUniqObjects(cstMeta, logRoot))
   {
    res = false;
    return false;
   }


   Map<AgeObjectWritable, Set<AgeRelationWritable>> invRelMap = new HashMap<AgeObjectWritable, Set<AgeRelationWritable>>();
   // invRelMap contains a map of external objects to sets of prepared inverse
   // relations for new external relations

   if(!connectNewExternalRelations(cstMeta, invRelMap, logRoot))
   {
    res = false;
    return false;
   }

   if(!connectNewObjectAttributes(cstMeta, logRoot))
   {
    res = false;
    return false;
   }

   if( !resolveIncomingFileAttributes(cstMeta, logRoot) )
   {
    res = false;
    return false;
   }

   
   LogNode semLog = logRoot.branch("Validating semantic");

   boolean vldRes = true;
   n = 0;
   for(ModMeta mm : cstMeta.incomingMods)
   {
    n++;

    if(mm.newModule == null)
     continue;

    LogNode vldLog = semLog.branch("Processing module: " + mm.meta.getId());

    boolean modRes = validator.validate(mm.newModule, vldLog);

    if(modRes)
     vldLog.success();

    vldRes = vldRes && modRes;
   }

   if(vldRes)
    semLog.success();

   res = res && vldRes;

   // Set<AgeObject> affObjSet = new HashSet<AgeObject>();
   //
   // if( invRelMap != null )
   // affObjSet.addAll( invRelMap.keySet() );

   if(invRelMap.size() > 0)
   {
    boolean invRelRes = true;
    LogNode invRelLog = logRoot.branch("Validating externaly related object semantic");

    Set<AgeRelation> relSet = new HashSet<AgeRelation>();

    for(AgeObject obj : invRelMap.keySet())
    {
     LogNode objLogNode = invRelLog.branch("Validating object Id: " + obj.getId() + " Class: " + obj.getAgeElClass());

     Collection< ? extends AgeRelation> relColl = null;

     Collection<AgeRelationWritable> inv = invRelMap != null ? invRelMap.get(obj) : null;

     if(inv == null)
      relColl = obj.getRelations();
     else
     {
      relSet.clear();

      relSet.addAll(obj.getRelations());

      relSet.addAll(inv);

      relColl = relSet;
     }

     if(validator.validateRelations(obj.getAgeElClass(), relColl, objLogNode))
      objLogNode.success();
     else
      invRelRes = false;
    }

    if(invRelRes)
     invRelLog.success();
    else
     invRelRes = false;

    res = res && invRelRes;
   }

   if(!res)
    return false;

   boolean needReload = false;

   for(ModMeta mm : cstMeta.incomingMods)
   {
    String modId = mm.meta.getId();

    mm.newModule.setClusterId(cstMeta.id);

    // while(ageStorage.hasDataModule(modId))
    // {
    // modId = Constants.dataModuleIDPrefix +
    // IdGenerator.getInstance().getStringId(Constants.dataModuleIDDomain);
    // }

    // if( ! modId.equals(mm.meta.getId()) )
    // {
    // logRoot.log(Level.WARN,
    // "Module ID '"+mm.meta.getId()+"' is already taken by some another module. New ID="+modId+" is assigned");
    // needReload = true;
    // }

    mm.newModule.setId(modId);
    mm.meta.setId(modId);

    for(AgeObjectWritable obj : mm.newModule.getObjects())
    {
     if(obj.getId() == null)
     {
      String id = null;

      do
      {
       id = Constants.localObjectIDPrefix + obj.getAgeElClass().getIdPrefix() + IdGenerator.getInstance().getStringId(Constants.objectIDDomain) + "@"
         + mm.newModule.getId();
      } while(mm.idMap.containsKey(id));

      obj.setId(id);
      mm.idMap.put(id, obj);
     }
    }
   }

   LogNode updtLog = logRoot.branch("Updating storage");

   ConnectionInfo connInfo = new ConnectionInfo();
   
   connInfo.setRelationsAttachment( new CollectionMapCollection<AgeRelationWritable>(invRelMap) );
   
   try
   {
    if(cstMeta.mod4DataUpd.size() > 0 || cstMeta.mod4Del.size() > 0 || cstMeta.mod4Ins.size() > 0)
    {

     ageStorage.update(new ExtractorCollection<ModMeta, DataModuleWritable>(cstMeta.mod4Ins, modExtractor), null, connInfo);

     updtLog.success();
    }
   }
   catch(Exception e)
   {
    updtLog.log(Level.ERROR, e.getMessage());

    res = false;

    return false;
   }

   if(!needReload)
   {
    try
    {
     submissionDB.restoreSubmission(sbmID);
    }
    catch(Exception e)
    {
     logRoot.log(Level.ERROR, "Method restoreSubmission error: " + e.getMessage());

     res = false;

     return false;
    }
   }
   else
   {
    SubmissionMeta newSMeta = Factory.createSubmissionMeta();

    newSMeta.setId(sMeta.getId());
    newSMeta.setDescription(sMeta.getDescription());
    newSMeta.setRemoved(false);
    newSMeta.setSubmitter(sMeta.getSubmitter());
    newSMeta.setModifier(sMeta.getModifier());
    newSMeta.setSubmissionTime(sMeta.getSubmissionTime());
    newSMeta.setModificationTime(sMeta.getModificationTime());

    for(ModMeta dmm : cstMeta.mod4Use)
     newSMeta.addDataModule(dmm.meta);

    for(FileAttachmentMeta fam : cstMeta.att4Use.values())
     newSMeta.addAttachment(fam);

    try
    {
     submissionDB.storeSubmission(newSMeta, sMeta, "Restoring submission with changed modules' IDs");
    }
    catch(SubmissionDBException e)
    {
     logRoot.log(Level.ERROR, "Method storeSubmission error: " + e.getMessage());

     res = false;

     return false;
    }

   }

  }
  finally
  {
   ageStorage.unlockWrite();
  }

  // Impute reverse relation and revalidate.

  return res;
 }

 public boolean removeSubmission(String sbmID, LogNode logRoot)
 {
  return removeSubmission(sbmID, false, logRoot);
 }

 public boolean tranklucateSubmission(String sbmID, LogNode logRoot)
 {
  return removeSubmission(sbmID, true, logRoot);
 }

 private boolean removeSubmission(String sbmID, boolean wipeOut, LogNode logRoot)
 {
  SubmissionMeta sMeta = null;

  try
  {
   sMeta = submissionDB.getSubmission(sbmID);
  }
  catch(SubmissionDBException e)
  {
   logRoot.log(Level.ERROR, "Method removeSubmission error: " + e.getMessage());

   return false;
  }

  if(sMeta == null)
  {
   logRoot.log(Level.ERROR, "Submission with ID='" + sbmID + "' is not found to be removed");
   return false;
  }

  ClustMeta cstMeta = new ClustMeta();
  cstMeta.id = sbmID;

  if(sMeta.getDataModules() != null)
  {
   for(DataModuleMeta dmm : sMeta.getDataModules())
   {
    ModMeta mm = new ModMeta();

    mm.meta = dmm;
    mm.origModule = ageStorage.getDataModule(new ModuleKey(sbmID, dmm.getId()));

    if(mm.origModule != null)
     cstMeta.mod4Del.put(dmm.getId(), mm);
   }
  }

  if(sMeta.getAttachments() != null)
  {
   for(FileAttachmentMeta fatt : sMeta.getAttachments())
    cstMeta.att4Del.put(fatt.getId(), fatt);
  }

  boolean res = true;

  try
  {
   ageStorage.lockWrite();

   // XXX connection to main graph

   Collection<Pair<AgeExternalObjectAttributeWritable, AgeObject>> extAttrConnector = new ArrayList<Pair<AgeExternalObjectAttributeWritable, AgeObject>>();
   Collection< AgeExternalRelationWritable >  relConnections = null;
   Map<AgeObjectWritable, Set<AgeRelationWritable>> relationDetachMap = null;

   if(cstMeta.mod4Del.size() != 0)
   {
    relConnections = new ArrayList<AgeExternalRelationWritable>();
    relationDetachMap = new HashMap<AgeObjectWritable, Set<AgeRelationWritable>>();

    if(!reconnectExternalObjectAttributes(cstMeta, extAttrConnector, logRoot))
    {
     return false;
    }

    if(!reconnectExternalRelations(cstMeta, relConnections, relationDetachMap, logRoot))
    {
     return false;
    }
   }

   if(cstMeta.att4Del.size() != 0)
   {
    if(!checkExternalFileConnections(cstMeta, logRoot))
    {
     return false;
    }
   }

   if(relationDetachMap != null)
   {
    boolean invRelRes = true;
    LogNode invRelLog = logRoot.branch("Validating externaly related object semantic");

    Set<AgeRelation> relSet = new HashSet<AgeRelation>();

    for(AgeObject obj : relationDetachMap.keySet())
    {
     LogNode objLogNode = invRelLog.branch("Validating object Id: " + obj.getId() + " Class: " + obj.getAgeElClass());

     Collection< ? extends AgeRelation> relColl = null;

     Collection<AgeRelationWritable> det = relationDetachMap != null ? relationDetachMap.get(obj) : null;

     if(det == null)
      relColl = obj.getRelations();
     else
     {
      relSet.clear();

      relSet.addAll(obj.getRelations());

      relSet.removeAll(det);

      relColl = relSet;
     }

     if(validator.validateRelations(obj.getAgeElClass(), relColl, objLogNode))
      objLogNode.success();
     else
      invRelRes = false;
    }

    if(invRelRes)
     invRelLog.success();
    else
     invRelRes = false;

    res = res && invRelRes;
   }

   if(!res)
    return false;

   if(cstMeta.att4Del.size() > 0)
   {
    LogNode fdelLog = logRoot.branch("Deleting files");

    boolean delRes = true;

    for(FileAttachmentMeta fam : cstMeta.att4Del.values())
    {
     fdelLog.log(Level.INFO, "Deleting file: '" + fam.getId() + "' (scope " + (fam.isGlobal() ? "global" : "cluster") + ")");

     if(!ageStorage.deleteAttachment(fam.getId(), cstMeta.id, fam.isGlobal()))
     {
      fdelLog.log(Level.WARN, "File deletion failed");
      delRes = false;
     }
    }

    if(delRes)
     fdelLog.success();
   }

   ConnectionInfo connInfo = new ConnectionInfo();
   
   connInfo.setObjectAttributesReconnection( extAttrConnector );
   connInfo.setRelationsReconnection( relConnections );
   connInfo.setRelationsRemoval( new CollectionMapCollection<AgeRelationWritable>(relationDetachMap) );
   
   if(cstMeta.mod4DataUpd.size() > 0 || cstMeta.mod4Del.size() > 0 || cstMeta.mod4Ins.size() > 0)
   {
    LogNode updtLog = logRoot.branch("Updating storage");

    try
    {
     ageStorage.update(null, new ExtractorCollection<ModMeta, ModuleKey>(cstMeta.mod4Del.values(), modkeyExtractor),connInfo);

     updtLog.success();
    }
    catch(Exception e)
    {
     updtLog.log(Level.ERROR, "Exception: " + e.getClass().getName() + " Message: " + e.getMessage());

     e.printStackTrace();

     res = false;

     return false;
    }
   }

   LogNode updtLog = logRoot.branch("Updating submission DB");
   try
   {

    if(wipeOut)
     submissionDB.tranklucateSubmission(sbmID);
    else
     submissionDB.removeSubmission(sbmID);

    updtLog.success();
   }
   catch(SubmissionDBException e)
   {
    logRoot.log(Level.ERROR, "Method removeSubmission error: " + e.getMessage());

    res = false;

    return false;
   }


  }
  finally
  {
   ageStorage.unlockWrite();
  }

  if(!wipeOut)
   return res;

  Transaction trn = annotationManager.startTransaction();

  ClusterEntity cEnt = new ClusterEntity(sMeta.getId());

  try
  {
   annotationManager.removeAnnotation(trn, null, cEnt, true);
  }
  catch(AnnotationDBException e)
  {
   try
   {
    annotationManager.rollbackTransaction(trn);
   }
   catch(TransactionException e1)
   {
    e1.printStackTrace();
   }

   trn = null;
  }
  finally
  {
   if(trn != null)
   {
    try
    {
     annotationManager.commitTransaction(trn);
    }
    catch(TransactionException e)
    {
     e.printStackTrace();
    }
   }
  }

  return res;
 }

 private boolean connectNewExternalRelations(ClustMeta cstMeta, Map<AgeObjectWritable, Set<AgeRelationWritable>> invRelMap, LogNode rootNode)
 {

  LogNode extRelLog = rootNode.branch("Connecting external object relations");
  boolean extRelRes = true;

  for(ModMeta mm : cstMeta.incomingMods)
  {
   if(mm.newModule == null || mm.newModule.getExternalRelations() == null)
    continue;

   LogNode extRelModLog = extRelLog.branch("Processing module: " + mm.aux.getOrder() + (mm.meta.getId() != null ? " ID='" + mm.meta.getId() + "'" : ""));

   boolean extModRelRes = true;

   for(AgeExternalRelationWritable exr : mm.newModule.getExternalRelations())
   {
    if(exr.getTargetObject() != null) // It can happen when we connected inverse
                                      // relation
     continue;

    String ref = exr.getTargetObjectId();

    AgeObjectWritable tgObj = resolveTarget(exr, cstMeta);

    if(tgObj == null)
    {
     extModRelRes = false;
     
     AgeObjectWritable srcObj = exr.getSourceObject();
     
     extRelModLog.log(Level.ERROR, "Object " + objId2Str(srcObj) + " has external relation (Class: '" + exr.getAgeElClass()
       + "' Position: " + exr.getOrder() + " ) that can't be resolved");

     continue;
    }

    AgeExternalRelationWritable invRel = null;

    invRel = findInverseRelation(exr, tgObj);

    if(invRel == null)
    {
     AgeRelationClass invRCls = exr.getAgeElClass().getInverseRelationClass();

     RelationClassRef invCRef = cstMeta.relRefMap.get(invRCls);

     if(invCRef == null)
     {
      invCRef = tgObj.getDataModule().getContextSemanticModel().getModelFactory()
        .createRelationClassRef(tgObj.getDataModule().getContextSemanticModel().getAgeRelationClassPlug(invRCls), 0, "");

      cstMeta.relRefMap.put(invRCls, invCRef);
     }

     invRel = tgObj.getDataModule().getContextSemanticModel()
       .createExternalRelation(invCRef, tgObj, exr.getSourceObject().getId(), ResolveScope.CLUSTER_CASCADE);
     invRel.setInferred(true);

     invRel.setTargetObject(exr.getSourceObject());
     invRel.setInverseRelation(exr);

     Set<AgeRelationWritable> rels = invRelMap.get(tgObj);

     if(rels == null)
     {
      rels = new HashSet<AgeRelationWritable>();
      invRelMap.put(tgObj, rels);
     }

     rels.add(invRel);
    }

    invRel.setTargetObject(exr.getSourceObject()); // Only unconnected objects
                                                   // (new) can have free
                                                   // explicit external
                                                   // relations
    invRel.setInverseRelation(exr);

    exr.setInverseRelation(invRel);

    exr.setTargetObject(tgObj);
   }

   if(extModRelRes)
    extRelModLog.success();

   extRelRes = extRelRes && extModRelRes;

  }

  if(extRelRes)
   extRelLog.success();

  return extRelRes;
 }

 private boolean checkUniqObjects(ClustMeta clusterMeta, LogNode logRoot)
 {
  boolean res = true;

  LogNode logUniq = logRoot.branch("Checking object identifiers uniquness");

  Map<DataModule, ModMeta> modMap = new HashMap<DataModule, SubmissionManager.ModMeta>();

  for(ModMeta mm : clusterMeta.mod4Use)
  {
   modMap.put(mm.newModule, mm);

   if(mm.newModule == null) // Hld+MetaUpd
   {
    for(AgeObjectWritable obj : mm.origModule.getObjects())
    {
     if(obj.getIdScope() == IdScope.CLUSTER || obj.getIdScope() == IdScope.GLOBAL)
     {
      AgeObject clashObj = clusterMeta.clusterIdMap.get(obj.getId());

      if(clashObj != null) // It meant that some new object pretends to this ID
      {
       res = false;

       ModMeta clashMM = modMap.get(clashObj.getDataModule());

       logUniq
         .log(
           Level.ERROR,
           "Object identifiers clash (ID='"
             + obj.getId()
             + "') whithin the cluster. The first object: module "
             + ((mm.aux != null ? mm.aux.getOrder() + " " : "(existing) ") + (mm.meta.getId() != null ? ("ID='" + mm.meta.getId() + "' ") : "") + "Row: " + obj
               .getOrder())
             + ". The second object: module "
             + ((clashMM.aux != null ? clashMM.aux.getOrder() + " " : "(existing) ")
               + (clashMM.meta.getId() != null ? ("ID='" + clashMM.meta.getId() + "' ") : "") + "Row: " + clashObj.getOrder()));

      }

      clusterMeta.clusterIdMap.put(obj.getId(), obj);
     }
    }

    continue;
   }

   for(AgeObjectWritable obj : mm.newModule.getObjects())
   {
    if(obj.getId() == null) // new object with anonymous ID
     continue;

    AgeObject clashObj = mm.idMap.get(obj.getId());

    if(clashObj != null)
    {
     res = false;

     logUniq
       .log(
         Level.ERROR,
         "Object identifiers clash (ID='"
           + obj.getId()
           + "') whithin the same module: "
           + ((mm.aux != null ? mm.aux.getOrder() + " " : "(existing) ") + (mm.meta.getId() != null ? ("ID='" + mm.meta.getId() + "' ") : "") + "Row: " + obj
             .getOrder()) + " and Row: " + clashObj.getOrder());

     continue;
    }

    mm.idMap.put(obj.getId(), obj);

    if(obj.getIdScope() == IdScope.CLUSTER || obj.getIdScope() == IdScope.GLOBAL)
    {
     clashObj = clusterMeta.clusterIdMap.get(obj.getId());

     if(clashObj != null)
     {
      res = false;

      ModMeta clashMM = modMap.get(clashObj.getDataModule());

      logUniq
        .log(
          Level.ERROR,
          "Object identifiers clash (ID='"
            + obj.getId()
            + "') whithin the cluster. The first object: module "
            + ((mm.aux != null ? mm.aux.getOrder() + " " : "(existing) ") + (mm.meta.getId() != null ? ("ID='" + mm.meta.getId() + "' ") : "") + "Row: " + obj
              .getOrder())
            + ". The second object: module "
            + ((clashMM.aux != null ? clashMM.aux.getOrder() + " " : "(existing) ")
              + (clashMM.meta.getId() != null ? ("ID='" + clashMM.meta.getId() + "' ") : "") + "Row: " + clashObj.getOrder()));

      continue;
     }

     clusterMeta.clusterIdMap.put(obj.getId(), obj);
    }

    if(obj.getIdScope() == IdScope.GLOBAL)
    {
     ModuleKey gmk = ageStorage.getGlobalObjectConnection(obj.getId()).getHostModuleKey();
     
     // We try to find clashing object outside of our cluster as all clashes
     // within the cluster we detected earlier
     if(gmk != null && !gmk.getClusterId().equals(clusterMeta.id))
     {
      res = false;

      logUniq.log(Level.ERROR, "Object identifiers clash (ID='" + obj.getId() + "') whithin the global scope. The first object: " + objId2Str(obj)
        + ". The second object: " + objId2Str(obj.getId(), gmk, -1, -1));

      continue;
     }

     clusterMeta.newGlobalIdMap.put(obj.getId(), obj);
    }
   }
  }

  if(res)
   logUniq.success();

  return res;
 }

 @SuppressWarnings("unchecked")
 private boolean reconnectExternalRelations(ClustMeta cstMeta, Collection<AgeExternalRelationWritable> relConn,
   Map<AgeObjectWritable, Set<AgeRelationWritable>> detachedRelMap, LogNode logRoot)
 {
  LogNode logRecon = logRoot.branch("Reconnecting external relations");

  boolean res = true;

  for(ModMeta mm : new CollectionsUnion<ModMeta>(cstMeta.mod4Del.values(), cstMeta.mod4DataUpd.values()))
  {
   if(mm.origModule == null) // Skipping new modules, processing only
    continue;                // update/delete modules (where original data are going away)
 
   

   Collection< ? extends AgeExternalRelationWritable> origExtRels = mm.origModule.getExternalRelations();

   if(origExtRels == null)
    continue;

   for(AgeExternalRelationWritable extRel : origExtRels)
   {
    AgeExternalRelationWritable invrsRel = extRel.getInverseRelation();
    AgeObjectWritable target = extRel.getTargetObject(); // external object

    if(invrsRel.isInferred())
    {
     Set<AgeRelationWritable> objectsRels = detachedRelMap.get(target);

     if(objectsRels == null)
      detachedRelMap.put(target, objectsRels = new HashSet<AgeRelationWritable>());

     objectsRels.add(invrsRel);
    }
    else
    {
     AgeObjectWritable replObj = null;
     String replObjId = invrsRel.getTargetObjectId();

     if(invrsRel.getTargetResolveScope() == ResolveScope.GLOBAL || !target.getModuleKey().getClusterId().equals(cstMeta.id))
      replObj = cstMeta.newGlobalIdMap.get(replObjId);
     else
      replObj = cstMeta.clusterIdMap.get(replObjId);

     if(replObj == null)
     {
      logRecon.log(Level.ERROR, "Module " + mm.aux.getOrder() + " (ID='" + mm.meta.getId() + "') is marked for "
        + (mm.newModule == null ? "deletion" : "update") + " but some object (ID='" + extRel.getTargetObjectId() + "' Module ID: '"
        + extRel.getTargetObject().getDataModule().getId() + "' Cluster ID: '" + extRel.getTargetObject().getDataModule().getClusterId()
        + "') holds the relation of class  '" + invrsRel.getAgeElClass() + "' with object '" + invrsRel.getTargetObjectId() + "'");
      res = false;
     }
     else
     {
      AgeExternalRelationWritable dirRel = findInverseRelation(invrsRel, replObj);

      if(dirRel == null)
      {

       RelationClassRef invCRef = cstMeta.relRefMap.get(extRel.getAgeElClass());

       if(invCRef == null)
       {
        invCRef = replObj
          .getDataModule()
          .getContextSemanticModel()
          .getModelFactory()
          .createRelationClassRef(replObj.getDataModule().getContextSemanticModel().getAgeRelationClassPlug(extRel.getAgeElClass()), 0,
            extRel.getTargetObjectId());

        cstMeta.relRefMap.put(extRel.getAgeElClass(), invCRef);
       }

       dirRel = replObj.getDataModule().getContextSemanticModel().createExternalRelation(invCRef, replObj, target.getId(), ResolveScope.CLUSTER_CASCADE);

       dirRel.setInferred(true);

       replObj.addRelation(dirRel);
      }

      dirRel.setInverseRelation(invrsRel);
      dirRel.setTargetObject(target);

      relConn.add(dirRel);

      if(!detachedRelMap.containsKey(target)) // This is to enforce semantic check on the target object
       detachedRelMap.put(target, null);
     }
    }

   }

  }

  // As new IDs could appear within cluster scope we need to reconnect
  // CASCADE_CLUSTER relations
  // that point to the GLOBAL scope but can be resolved on the CLUSTER scope now
  for(ModMeta mm : cstMeta.mod4DataHld.values() )
  {
   Collection< ? extends AgeExternalRelationWritable> origExtRels = mm.origModule.getExternalRelations();

   if(origExtRels == null)
    continue;

   for(AgeExternalRelationWritable extRel : origExtRels)
   {
    if( extRel.isInferred() || ( extRel.getTargetResolveScope() != ResolveScope.CLUSTER_CASCADE && extRel.getTargetResolveScope() != ResolveScope.MODULE_CASCADE ) )
     continue;

    AgeObjectWritable newTarget = cstMeta.clusterIdMap.get(extRel.getTargetObjectId()); // newTarget can only be of the new objects

    if(newTarget != null)
    {
     AgeExternalRelationWritable invrsRel = extRel.getInverseRelation();
     AgeObjectWritable oldTarget = extRel.getTargetObject(); // external object
     AgeObjectWritable relSource = extRel.getSourceObject();
     
     if( newTarget == oldTarget || oldTarget.getModuleKey().getClusterId().equals(cstMeta.id) )
      continue;

     if(invrsRel.isInferred())
     {
      Set<AgeRelationWritable> detSet = detachedRelMap.get(oldTarget);

      if(detSet == null)
       detachedRelMap.put(oldTarget, detSet = new HashSet<AgeRelationWritable>());

      detSet.add(invrsRel);
     }
     else
     {
      res = false;

      logRecon.log(Level.ERROR, "Object " + objId2Str(relSource) + " has relation (Class: '" + extRel.getAgeElClass() + "') with "
        + ResolveScope.CLUSTER_CASCADE.name() + " resolution scope pointing to object " + objId2Str(oldTarget)
        + " in global scope and this relation can be reconnected to object " + objId2Str(newTarget)
        + " but current relation has explicit inverse relation that can't be reconnected");

      continue;
     }

     AgeExternalRelationWritable newInvRel = findInverseRelation(extRel, newTarget);

     if(newInvRel == null)
     {
      RelationClassRef invCRef = cstMeta.relRefMap.get(extRel.getAgeElClass());

      if(invCRef == null)
      {
       invCRef = newTarget
         .getDataModule()
         .getContextSemanticModel()
         .getModelFactory()
         .createRelationClassRef(newTarget.getDataModule().getContextSemanticModel().getAgeRelationClassPlug(extRel.getAgeElClass()), 0,
           extRel.getTargetObjectId());

       cstMeta.relRefMap.put(extRel.getAgeElClass(), invCRef);
      }

      newInvRel = newTarget.getDataModule().getContextSemanticModel()
        .createExternalRelation(invCRef, newTarget, relSource.getId(), ResolveScope.CLUSTER);

      newInvRel.setInferred(true);
      newInvRel.setTargetObject(relSource);

      newTarget.addRelation(newInvRel);
     }

     relConn.add(newInvRel);

     newInvRel.setInverseRelation(extRel);

     if(!detachedRelMap.containsKey(relSource)) // This is to enforce semantic
      detachedRelMap.put(relSource, null);      // check on the changed object
     

    }
   }
  }

  if(res)
   logRecon.success();

  return res;

 }

 private AgeExternalRelationWritable findInverseRelation(AgeExternalRelationWritable extRel, AgeObjectWritable replObj)
 {
  AgeRelationClass invClass = extRel.getAgeElClass().getInverseRelationClass();

  if(replObj.getDataModule().getExternalRelations() != null)
  {
   for(AgeExternalRelationWritable cndtRel : replObj.getDataModule().getExternalRelations()) // Looking for suitable explicit relation
   {
    if(cndtRel.getSourceObject() == replObj
      && extRel.getTargetObjectId().equals(replObj.getId())
      && cndtRel.getAgeElClass().equals(invClass)
      && cndtRel.getTargetObjectId().equals(extRel.getSourceObject().getId())
      && (extRel.getSourceObject().getIdScope() == IdScope.GLOBAL || extRel.getSourceObject().getModuleKey().getClusterId() //Objects can be connected in global scope or
        .equals(replObj.getModuleKey().getClusterId()))                                                                     //in cluster scope if they are in the same cluster
      && (cndtRel.getTargetResolveScope() != ResolveScope.CLUSTER || extRel.getSourceObject().getModuleKey().getClusterId() //If they are not in the same cluster resolution
        .equals(replObj.getModuleKey().getClusterId())))                                                                    //scope can be GLOBAL, CASCADE_CLUSTER or CASCADE_MODULE
     return cndtRel;
   }
  }

  return null;
 }

 private AgeObjectWritable resolveTarget(Resolvable rslv, ClustMeta cstMeta)
 {
  String ref = rslv.getTargetObjectId();

  AgeObjectWritable tgObj = null;

  if(rslv.getTargetResolveScope() == ResolveScope.GLOBAL)
  {
   tgObj = cstMeta.newGlobalIdMap.get(ref);

   if(tgObj == null)
    tgObj = ageStorage.getGlobalObject(ref);

   if(tgObj != null && (cstMeta.mod4Del.containsKey(tgObj.getDataModule().getId()) || cstMeta.mod4DataUpd.containsKey(tgObj.getDataModule().getId())))
    tgObj = null;
  }
  else
  {
   tgObj = cstMeta.clusterIdMap.get(ref);

   if(tgObj == null && ( rslv.getTargetResolveScope() == ResolveScope.CLUSTER_CASCADE || rslv.getTargetResolveScope() == ResolveScope.MODULE_CASCADE ))
   {
    tgObj = ageStorage.getGlobalObject(ref);

    if(tgObj != null && (cstMeta.mod4Del.containsKey(tgObj.getDataModule().getId()) || cstMeta.mod4DataUpd.containsKey(tgObj.getDataModule().getId())))
     tgObj = null;
   }
  }

  return tgObj;
 }

 private String objId2Str( AgeObject obj )
 {
  return objId2Str(obj.getId(), obj.getModuleKey(), obj.getRow(), obj.getCol()); 
 }

 
 private String objId2Str(String objId, ModuleKey mk, int r, int c)
 {
  String modId = null;

  if(mk.getModuleId().charAt(0) == '\0')
   modId = "Module order: " + (int) mk.getModuleId().charAt(1);
  else
   modId = "MID: '" + mk.getModuleId() + '\'';

  return "CID: '" + mk.getClusterId() + "' " + modId + " OID: '" + objId + "'" +(r>=0?(" Pos: " + r+":"+c):"");
 }

 // New G->C
 // Rem G
 //  a) G unresErr
 //  b) CC G->C
 // Rem C
 //  a) C unserErr
 //  b) CC C->G
 //For every outgoing global object we check
 //1)If there are incoming connection (refs by object attrs)
 // a) if no replacement
 //   aa) connection from within another cluster - error
 //   ab) connection from within the same cluster and resolution GLOBAL - error
 // b) replacement has incompatible class - error
 // c) adding GlobalObjectConnection to attrReConn to reset obsolete cached connection
 // d) connection from within the same cluster and resolution CASCADE_CLUSTER - try to re-resolve in cluster pool

 private boolean reconnectExternalObjectAttributes(ClustMeta cstMeta,  LogNode logRoot)
 {
  LogNode logRecon = logRoot.branch("Reconnecting external object attributes");

  boolean res = true;
  
  for( ModMeta mm : new CollectionsUnion<ModMeta>(cstMeta.mod4Del.values(),cstMeta.mod4DataUpd.values()) )
  {
   
   for( AgeObjectWritable obj : mm.origModule.getObjects() )
   {
    if( obj.getIdScope() == IdScope.GLOBAL )
    {
     GlobalObjectConnection objConn = ageStorage.getGlobalObjectConnection(obj.getId());
     AgeObjectWritable newObj = cstMeta.newGlobalIdMap.get(obj.getId());

     if(newObj == null)
      cstMeta.obsoleteGlobalIdMap.put(obj.getId(), obj);

     boolean hasExternal = false;
     if(objConn.getIncomingConnections() != null && objConn.getIncomingConnections().size() > 0)
      hasExternal = true;

     // We will treat connections to global object coming from the same cluster differently.
     // They can go away with modules or can be down graded to cluster scope
     // for(ModuleKey cmk : objConn.getIncomingConnections().keySet())
     // if(cmk.getClusterId().endsWith(cstMeta.id))
     // {
     // hasExternal = true;
     // break;
     // }

     if(hasExternal)
     {

      if(newObj != null)
      {
       if(!newObj.getAgeElClass().isClassOrSubclassOf(obj.getAgeElClass()))
       {
        res = false;

        logRecon.log(Level.ERROR, "Global object: " + objId2Str(obj) + " will be replaced with object of incompatible class: " + objId2Str(newObj));

        continue;
       }

       cstMeta.connInfo.getResetModules().addAll(objConn.getIncomingConnections().keySet());
      }
      else
      {
       res = false;
       logRecon.log(Level.ERROR, "Global object: " + objId2Str(obj) + " will be removed but it is a value for "
         + objConn.getIncomingConnections().size() + " object attributes. Modules: " + objConn.getIncomingConnections());

       continue;
      }
     }
    }
   }
   
   //We have to disconnect global connections
   if( mm.origModule.getExternalObjectAttributes() != null )
   {
    for( AgeExternalObjectAttributeWritable exta : mm.origModule.getExternalObjectAttributes() )
    {
     String tgtId = exta.getTargetObjectId();
     
     if( (exta.getTargetResolveScope() == ResolveScope.GLOBAL || ageStorage.getClusterObject(cstMeta.id, tgtId ) == null) 
         && ! cstMeta.obsoleteGlobalIdMap.containsKey(tgtId) )
     {
      Set<DataModule> globset = cstMeta.connInfo.getGlobalDetachments().get(tgtId);
      
      if( globset == null )
       cstMeta.connInfo.getGlobalDetachments().put(tgtId,globset=new HashSet<DataModule>());
      
      globset.add(mm.origModule);
     }
    }
    
   }
  }
  
  for( ModMeta mm : cstMeta.mod4DataHld.values() )
  {
   Collection<? extends AgeExternalObjectAttributeWritable> extAttrs = mm.origModule.getExternalObjectAttributes();
   
   if( extAttrs == null || extAttrs.size() == 0 )
    continue;
   
   for( AgeExternalObjectAttributeWritable exta : extAttrs )
   {
    AgeClass tgtObjClass = null;

    AgeObjectWritable tgtObj = null;
    
    // trying re-resolving objattrs, some necessary object can go off
    
    if( exta.getTargetResolveScope() != ResolveScope.GLOBAL ) // CLUSTER, CASCADE_MODULE or CASCADE_CLUSTER
    {
     tgtObj = cstMeta.clusterIdMap.get(exta.getTargetObjectId());
     
     if( tgtObj != null )
      tgtObjClass = tgtObj.getAgeElClass();
    }
    
    if(  tgtObjClass == null )
    {
     if( exta.getTargetResolveScope() == ResolveScope.CLUSTER )
     {
      res = false;
      logRecon.log(Level.ERROR, "Can't resolve object attribute in cluster scope: " + objId2Str(exta.getMasterObject()) + " Attribute: "+exta.getClassReference().getHeading());
      
      continue;
     }
     
     // GLOBAL, CASCADE_MODULE or CASCADE_CLUSTER in global scope
     if( ! cstMeta.obsoleteGlobalIdMap.containsKey(exta.getTargetObjectId()) )
     {
      GlobalObjectConnection gcon = ageStorage.getGlobalObjectConnection(exta.getTargetObjectId());
      
      if( gcon != null )
       tgtObjClass = ageStorage.getSemanticModel().getDefinedAgeClass(gcon.getClassName());
     }
    }
    
    if( tgtObjClass == null )
    {
     res = false;
     logRecon.log(Level.ERROR, "Can't resolve object attribute in global scope: " + objId2Str(exta.getMasterObject()) + " Attribute: "+exta.getClassReference().getHeading());
     
     continue;
    }

    if( ! tgtObjClass.isClassOrSubclassOf( exta.getAgeElClass().getTargetClass() ) )
    {
     res = false;
     logRecon.log(Level.ERROR, "Object attribute is resolved to the object of incompatible class " + objId2Str(exta.getMasterObject()) + " Attribute: "+exta.getClassReference().getHeading());
     
     continue;
    }
    
    cstMeta.connInfo.getResetModules().add(mm.origModule.getModuleKey());
    
   }
   
  }
  
  return res;
 }
 

 @SuppressWarnings("unchecked")
 private boolean resolveIncomingFileAttributes(ClustMeta cMeta, LogNode logRoot)
 {
  boolean res = true;

  LogNode logCon = logRoot.branch("Checking file attributes to files connections");

  for(ModMeta mm : new CollectionsUnion<ModMeta>(cMeta.mod4Ins, cMeta.mod4DataUpd.values()))
  {
   for(AgeFileAttributeWritable fattr : mm.newModule.getFileAttributes())
   {
    if(fattr.getTargetResolveScope() == ResolveScope.GLOBAL)
    {
     if(ageStorage.getAttachment(fattr.getFileId()) == null)
     {
      AttributeClassRef clRef = fattr.getClassReference();

      logCon.log(Level.ERROR, "Reference to file can't be resolved (in global scope). Module: " + mm.aux.getOrder()
        + (mm.meta.getId() != null ? (" (ID='" + mm.meta.getId() + "')") : "") + " Attribute: row: " + fattr.getOrder() + " col: " + clRef.getOrder());

      res = false;
     }
     else
      fattr.setResolvedGlobal(true);
    }
    else
    {
     FileAttachmentMeta fmt = cMeta.att4Use.get(fattr.getFileId());

     if( fmt != null )
      fattr.setResolvedGlobal(false);
     else if( fattr.getTargetResolveScope() == ResolveScope.CLUSTER_CASCADE )
     {
      if(ageStorage.getAttachment(fattr.getFileId()) == null)
      {
       AttributeClassRef clRef = fattr.getClassReference();

       logCon.log(Level.ERROR, "Reference to file can't be resolved (in global scope). Module: " + mm.aux.getOrder()
         + (mm.meta.getId() != null ? (" (ID='" + mm.meta.getId() + "')") : "") + " Attribute: row: " + fattr.getOrder() + " col: " + clRef.getOrder());

       res = false;
      }
      else
       fattr.setResolvedGlobal(true);
     }

    }
   }
  }


  if(res)
   logCon.success();

  return res;
 }

 private boolean reconnectExternalFileAttributes( ClustMeta cMeta, Set<String> remGlobFiles, LogNode logRoot )
 {
  LogNode logRecon = logRoot.branch("Reconnecting file attributes");

  boolean res = true;

  for(FileMeta glMeta : cMeta.att4G2L.values() )
  {
   GlobalObjectConnection conn = ageStorage.getGlobalFileConnection(glMeta.origFile.getId());
   
   if( conn == null )
   {
    res = false;
    logRecon.log(Level.ERROR,
     "File with ID '" + glMeta.origFile.getId() + "' is not global");

    continue;
   }    
   
   if( conn.getIncomingConnections() != null || conn.getIncomingConnections().size() > 0 )
   {
    res = false;
    logRecon.log(Level.ERROR,
     "File with ID '" + glMeta.origFile.getId() + "' is referred by the modules: " + conn.getIncomingConnections() + " and can't be deleted");

    continue;
   }
   
   remGlobFiles.add(glMeta.origFile.getId());
  }
  
  for( FileAttachmentMeta fm : cMeta.att4Del.values() )
  {
   if( ! fm.isGlobal() )
    continue;
  
   GlobalObjectConnection conn = ageStorage.getGlobalFileConnection(fm.getId());
 
   if( conn.getIncomingConnections() == null || conn.getIncomingConnections().size() == 0 )
   {
    res = false;
    logRecon.log(Level.ERROR,
     "File with ID '" + fm.getId() + "' is referred by the modules: " + conn.getIncomingConnections() + " and can't be deleted");

    continue;
   }
 
   remGlobFiles.add(fm.getId());
  }
  
  return res;
 }
 
 private boolean checkExternalFileConnectionsX(ClustMeta cMeta, LogNode logRoot)
 {

  LogNode logRecon = logRoot.branch("Reconnecting file attributes");

  boolean res = true;

  for(DataModule extDM : ageStorage.getDataModules())
  {
   if(extDM.getClusterId().equals(cMeta.id))
    continue;

   for(AgeFileAttributeWritable fileAttr : extDM.getFileAttributes())
   {
    if( fileAttr.isResolvedGlobal() )
    {

     FileAttachmentMeta meta = cMeta.att4Del.get(fileAttr.getFileId());

     if(meta != null && meta.isGlobal())
     {
      FileAttachmentMeta fm = cMeta.att4Ins.get(fileAttr.getFileId());

      if( fm == null )
      {
       FileMeta fmet = cMeta.att4L2G.get(fileAttr.getFileId());
       
       if( fmet != null )
        fm = fmet.newFile;
      }
      
      if( fm == null || !fm.isGlobal() )
      {
       res = false;
       logRecon.log(Level.ERROR,
         "File with ID '" + fileAttr.getFileId() + "' is referred by the module '" + extDM.getId() + "' cluster '" + extDM.getClusterId()
           + "' and can't be deleted");
      }
     }
     else
     {
      FileMeta fm = cMeta.att4G2L.get(fileAttr.getFileId());
      
      if(fm != null)
      {
       res = false;
       logRecon.log(Level.ERROR,
         "File with ID '" + fileAttr.getFileId() + "' is referred by the module '" + extDM.getId() + "' cluster '" + extDM.getClusterId()
         + "' and can't limit visibility");
       continue;
      }
     }

    }

   }
  }

  if(res)
   logRecon.success();

  return res;
 }

 private boolean connectNewFileAttributes( ClustMeta cMeta, LogNode reconnLog )
 {
  boolean res = true;

  for(ModMeta mm : cMeta.incomingMods)
  {
   if(mm.newModule.getFileAttributes() == null)
    continue;

   for(AgeFileAttributeWritable fatt : mm.newModule.getFileAttributes())
   {
    if(fatt.getTargetResolveScope() != ResolveScope.GLOBAL)
    {
     if(cMeta.att4Use.containsKey(fatt.getFileId()))
      continue;
     else if(fatt.getTargetResolveScope() == ResolveScope.CLUSTER)
     {
      reconnLog.log(Level.ERROR,
        "Can't connect file attribute: '" + fatt.getFileId() + "'. Module: ID='" + mm.meta.getId() + "' Row: " + fatt.getOrder() + " Col: "
          + fatt.getClassReference().getOrder());
      res = false;

      continue;
     }
    }

    if(ageStorage.getGlobalFileConnection(fatt.getFileId()) == null || cMeta.att4G2L.containsKey(fatt.getFileId()) || cMeta.att4Del.containsKey(fatt.getFileId()))
    {
     reconnLog.log(Level.ERROR,
       "Can't connect file attribute: '" + fatt.getFileId() + "'. Module: ID='" + mm.meta.getId() + "' Row: " + fatt.getOrder() + " Col: "
         + fatt.getClassReference().getOrder());
     res = false;

     continue;
    }
   }
  }

  return res;
 }
 
 @SuppressWarnings("unchecked")
 private boolean reconnectLocalModulesToFiles(ClustMeta cMeta, LogNode reconnLog)
 {
  boolean res = true;

  
  for(ModMeta mm : new CollectionsUnion<ModMeta>(cMeta.mod4Hld.values(), cMeta.mod4MetaUpd.values()))
  {
   for(AgeFileAttributeWritable fattr : mm.origModule.getFileAttributes())
   {
    if( fattr.getTargetResolveScope() == ResolveScope.GLOBAL && cMeta.att4G2L.containsKey(fattr.getFileId()) )
    {
     reconnLog.log(Level.ERROR,
       "Can't connect file attribute: '" + fattr.getFileId() + "'. Module: ID='" + mm.meta.getId() + "' Row: " + fattr.getOrder() + " Col: "
         + fattr.getClassReference().getOrder());
     res = false;

     continue;
    }
    

    if( cMeta.att4Del.containsKey(fattr.getFileId() ) )
    {
     if( fattr.getTargetResolveScope() != ResolveScope.CLUSTER_CASCADE || ageStorage.getGlobalFileConnection(fattr.getFileId()) == null )
     {
      reconnLog.log(Level.ERROR,
        "Can't connect file attribute: '" + fattr.getFileId() + "'. Module: ID='" + mm.meta.getId() + "' Row: " + fattr.getOrder() + " Col: "
          + fattr.getClassReference().getOrder());
      res = false;
     }
    }
    
   }
  }


  return res;
 }

 private boolean connectNewObjectAttributes(ClustMeta cstMeta, LogNode logRoot)
 {
  LogNode extAttrLog = logRoot.branch("Connecting external object attributes");
  boolean extAttrRes = true;

  for(ModMeta mm : cstMeta.incomingMods)
  {
   if(mm.newModule == null)
    continue;

   boolean mdres = true;
   
   LogNode extAttrModLog = extAttrLog.branch("Processing module: " + mm.aux.getOrder());

   for(AgeExternalObjectAttributeWritable exta : mm.newModule.getExternalObjectAttributes() )
   {
    if( exta.getTargetResolveScope() == ResolveScope.GLOBAL_FALLBACK )
    {
     Set<DataModule> attMods = cstMeta.connInfo.getGlobalAttachmentRequests().get(exta.getTargetObjectId());
     
     if( attMods == null )
      cstMeta.connInfo.getGlobalAttachmentRequests().put(exta.getTargetObjectId(), attMods = new HashSet<DataModule>() );
     
     attMods.add(mm.newModule);
    }
    
    if(exta.getTargetResolveScope() != ResolveScope.GLOBAL && exta.getTargetResolveScope() != ResolveScope.GLOBAL_FALLBACK )
    {
     if(cstMeta.clusterIdMap.containsKey(exta.getTargetObjectId()))
      continue;

     if(exta.getTargetResolveScope() == ResolveScope.CLUSTER)
     {
      mdres = false;
      extAttrModLog.log(Level.ERROR, "Can't resolve object attribute in global scope: " + objId2Str(exta.getMasterObject()) + " Attribute: "
        + exta.getClassReference().getHeading());

      continue;
     }
    }

    boolean globOk=false;
    
    AgeObject tgObj = cstMeta.newGlobalIdMap.get(exta.getTargetObjectId());
    
    if(  tgObj != null ) // no more actions needed
    {
     if( ! tgObj.getAgeElClass().isClassOrSubclassOf(exta.getAgeElClass().getTargetClass()) )
     {
      mdres = false;
      extAttrModLog.log(Level.ERROR, "Object attribute is resolved to the object of incompatible class " + objId2Str(exta.getMasterObject()) + " Attribute: "+exta.getClassReference().getHeading());
      continue;
     }
     
     globOk = true;
    }
    else if( ! cstMeta.obsoleteGlobalIdMap.containsKey(exta.getTargetObjectId()) ) // if obsoleteGlobalIdMap contains targetId then resolution will fail
    {
     GlobalObjectConnection gcon = ageStorage.getGlobalObjectConnection(exta.getTargetObjectId());
     
     if( gcon != null )
     {
      AgeClass tgCls = null;
      
      if( gcon.getClassName() != null)
       tgCls = ageStorage.getSemanticModel().getDefinedAgeClass(gcon.getClassName());
      else
       tgCls = ageStorage.getGlobalObject(exta.getTargetObjectId()).getAgeElClass();
      
      if( ! tgCls.isClassOrSubclassOf(exta.getAgeElClass().getTargetClass())  )
      {
       if( exta.getTargetResolveScope() != ResolveScope.GLOBAL_FALLBACK )
       {
        mdres = false;
        extAttrModLog.log(Level.ERROR, "Object attribute is resolved to the object of incompatible class " + objId2Str(exta.getMasterObject()) + " Attribute: "+exta.getClassReference().getHeading());
       }
       else
        extAttrModLog.log(Level.WARN, "A global object has incompatible class for GLOBAL_FALLBACK object attribute " + objId2Str(exta.getMasterObject()) + " Attribute: "+exta.getClassReference().getHeading());

       continue;
      }
      
      globOk = true;
      
      //Setting global object attachments
      if( ! gcon.getHostModuleKey().getClusterId().equals(cstMeta.id))
      {
       Set<DataModule> attMods = cstMeta.connInfo.getGlobalAttachments().get(exta.getTargetObjectId());
       
       if( attMods == null )
        cstMeta.connInfo.getGlobalAttachments().put(exta.getTargetObjectId(), attMods = new HashSet<DataModule>() );
       
       attMods.add(mm.newModule);
      }
     }
    }
    
    if( exta.getTargetResolveScope() != ResolveScope.GLOBAL_FALLBACK &&  ! globOk )
    {
     mdres = false;
     if( exta.getTargetResolveScope() == ResolveScope.GLOBAL )
      extAttrModLog.log(Level.ERROR, "Can't resolve object attribute in global scope: " + objId2Str(exta.getMasterObject()) + " Attribute: "
       + exta.getClassReference().getHeading());
     else
      extAttrModLog.log(Level.ERROR, "Can't resolve object attribute in cascade scope: " + objId2Str(exta.getMasterObject()) + " Attribute: "
        + exta.getClassReference().getHeading());
    }
    
    if( mdres )
     extAttrModLog.success();
    
    extAttrRes = extAttrRes && mdres;
   }

  }
  
  if( extAttrRes )
   extAttrLog.success();
  
  return extAttrRes;
 }
 

 public AgeTabSyntaxParser getAgeTabParser()
 {
  return ageTabParser;
 }

 public AgeTab2AgeConverter getAgeTab2AgeConverter()
 {
  return converter;
 }

 public AgeSemanticValidator getAgeSemanticValidator()
 {
  return validator;
 }

 public SubmissionDB getSubmissionDB()
 {
  return submissionDB;
 }

}
